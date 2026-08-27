#!/usr/bin/env python3
"""PoC compute-node metrics agent for ML2/OVN.

Reads bulk per-vNIC counters from libvirt, obtains the Neutron port UUID from
the domain XML or the OVN tap-name convention, and resolves active ports in
OVN Southbound:

    Port_Binding.logical_port == port UUID
    Port_Binding.chassis.name == local chassis
    Port_Binding.up == true
    external_ids["neutron:network_name"] == neutron-<network UUID>

The same bulk libvirt query also collects instance CPU/RAM gauges and per-disk
I/O counters. Each counter family marks its first observation as a baseline so
ClickHouse can calculate restart- and reset-aware hourly usage.
"""

from __future__ import annotations

import argparse
import configparser
import json
import logging
import queue
import socket
import sys
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Any


LOG = logging.getLogger("ovn-traffic-agent")
SUPPORTED_METRICS = frozenset({"network", "disk", "ram", "cpu"})
DEFAULT_METRICS = "network,disk,ram,cpu"


@dataclass(frozen=True)
class PortMetadata:
    port_uuid: str
    network_uuid: str
    instance_uuid: str


@dataclass(frozen=True)
class PortBinding:
    port_uuid: str
    external_ids: dict[str, str]
    chassis_names: frozenset[str]
    up: bool
    port_type: str


@dataclass(frozen=True)
class VNICSample:
    instance_uuid: str
    interface_name: str
    port_uuid: str
    rx: int
    tx: int


@dataclass(frozen=True)
class InstanceSample:
    instance_uuid: str
    cpu_time_ns: int
    vcpus: int
    memory_actual_bytes: int | None
    memory_rss_bytes: int | None
    memory_usable_bytes: int | None


@dataclass(frozen=True)
class PreviousCPU:
    cpu_time_ns: int
    ts: float


@dataclass(frozen=True)
class DiskSample:
    instance_uuid: str
    device: str
    read_bytes: int | None
    write_bytes: int | None
    read_requests: int | None
    write_requests: int | None
    capacity_bytes: int | None
    allocation_bytes: int | None
    physical_bytes: int | None


@dataclass(frozen=True)
class CollectionBatch:
    vnics: list[VNICSample]
    instances: list[InstanceSample]
    disks: list[DiskSample]


@dataclass(frozen=True)
class HostConfig:
    host: str
    libvirt_uri: str
    ovn_chassis: str | None
    region_name: str
    libvirt_username: str | None
    libvirt_password: str | None
    libvirt_auth_file: str | None
    libvirt_auth_section: str


@dataclass
class NodeRuntime:
    args: argparse.Namespace
    collector: Any
    resolver: Any
    previous_counters: dict[tuple[str, str, str], tuple[int, int]] = field(
        default_factory=dict
    )
    previous_cpu: dict[str, PreviousCPU] = field(default_factory=dict)
    previous_disk_counters: dict[tuple[str, str], tuple] = field(
        default_factory=dict
    )


@dataclass(frozen=True)
class CycleResult:
    emitted: int
    skipped: int
    baselines: int
    instances: int
    instance_baselines: int
    disks: int
    disk_baselines: int
    discovery_seconds: float
    resolution_seconds: float

    @property
    def query_seconds(self) -> float:
        return self.discovery_seconds + self.resolution_seconds


@dataclass(frozen=True)
class PublishRecord:
    topic: str
    key: str
    value: bytes


@dataclass
class PublishBarrier:
    deadline: float
    event: threading.Event = field(default_factory=threading.Event)
    outstanding: int = 0
    error: BaseException | None = None


class CommandError(RuntimeError):
    pass


def exception_text(error: BaseException) -> str:
    """Format third-party exceptions whose __str__ implementation may fail."""
    try:
        return str(error)
    except Exception:
        return type(error).__name__


def parse_metrics(value: str) -> frozenset[str]:
    metrics = frozenset(part.strip().lower() for part in value.split(",") if part.strip())
    if not metrics:
        raise argparse.ArgumentTypeError("at least one metric must be selected")
    unknown = metrics - SUPPORTED_METRICS
    if unknown:
        raise argparse.ArgumentTypeError(
            "unknown metrics: " + ", ".join(sorted(unknown))
        )
    if "ram" in metrics and "cpu" not in metrics:
        raise argparse.ArgumentTypeError(
            "ram requires cpu because both share the instance-stats record"
        )
    return metrics


def load_yaml_config(path: str) -> dict[str, Any]:
    try:
        import yaml
    except ImportError as err:
        raise CommandError("PyYAML is required when --config is used") from err
    try:
        with open(path, encoding="utf-8") as config_file:
            loaded = yaml.safe_load(config_file)
    except OSError as err:
        raise CommandError(f"cannot read config {path}: {err}") from err
    except yaml.YAMLError as err:
        raise CommandError(f"invalid YAML in {path}: {err}") from err
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise CommandError("agent config must be a YAML mapping")
    return loaded


def _config_section(config: dict[str, Any], name: str) -> dict[str, Any]:
    value = config.get(name, {})
    if not isinstance(value, dict):
        raise CommandError(f"config section {name!r} must be a mapping")
    return value


def _metrics_default(value: Any) -> str:
    if isinstance(value, list) and all(isinstance(metric, str) for metric in value):
        return ",".join(value)
    if isinstance(value, str):
        return value
    raise CommandError("metrics must be a comma-separated string or a list of strings")


def parse_host_configs(
    config: dict[str, Any], args: argparse.Namespace
) -> tuple[HostConfig, ...]:
    configured_nodes = config.get("nodes")
    if not isinstance(configured_nodes, list) or not configured_nodes:
        raise CommandError("config must contain a non-empty nodes list")

    hosts: list[HostConfig] = []
    names: set[str] = set()
    for index, node in enumerate(configured_nodes):
        if not isinstance(node, dict):
            raise CommandError(f"nodes[{index}] must be a mapping")
        host = node.get("host")
        if not isinstance(host, str) or not host.strip():
            raise CommandError(f"nodes[{index}].host must be a non-empty string")
        host = host.strip()
        if host in names:
            raise CommandError(f"duplicate node host {host!r}")
        names.add(host)

        libvirt_uri = node.get("libvirt_uri", args.libvirt_uri)
        if not isinstance(libvirt_uri, str) or not libvirt_uri:
            raise CommandError(f"nodes[{index}].libvirt_uri must be a non-empty string")
        ovn_chassis = node.get("ovn_chassis", args.ovn_chassis)
        if ovn_chassis is not None and not isinstance(ovn_chassis, str):
            raise CommandError(f"nodes[{index}].ovn_chassis must be a string")
        if "network" in args.metrics and not ovn_chassis:
            raise CommandError(
                f"nodes[{index}].ovn_chassis is required for network metrics"
            )

        region_name = node.get("region_name", args.region_name)
        if not isinstance(region_name, str) or not region_name:
            raise CommandError(f"nodes[{index}].region_name must be a non-empty string")
        username = node.get("libvirt_username", args.libvirt_username)
        password = node.get("libvirt_password", args.libvirt_password)
        if bool(username) != bool(password):
            raise CommandError(
                f"nodes[{index}] must set both libvirt_username and libvirt_password"
            )
        auth_file = node.get("libvirt_auth_file", args.libvirt_auth_file)
        auth_section = node.get("libvirt_auth_section", args.libvirt_auth_section)
        hosts.append(
            HostConfig(
                host=host,
                libvirt_uri=libvirt_uri,
                ovn_chassis=ovn_chassis,
                region_name=region_name,
                libvirt_username=username,
                libvirt_password=password,
                libvirt_auth_file=auth_file,
                libvirt_auth_section=auth_section,
            )
        )
    return tuple(hosts)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    config_parser = argparse.ArgumentParser(add_help=False)
    config_parser.add_argument("--config")
    preliminary, _ = config_parser.parse_known_args(argv)
    requested_help = any(arg in ("-h", "--help") for arg in (argv or sys.argv[1:]))
    if preliminary.config is None and not requested_help:
        config_parser.error("--config is required; agent settings are YAML-only")
    try:
        config = load_yaml_config(preliminary.config) if preliminary.config else {}
        kafka = _config_section(config, "kafka")
        libvirt_config = _config_section(config, "libvirt")
        ovn = _config_section(config, "ovn")
        metrics_default = _metrics_default(config.get("metrics", DEFAULT_METRICS))
    except CommandError as err:
        config_parser.error(str(err))

    parser = argparse.ArgumentParser(
        description="Publish libvirt metrics enriched by OVN SBDB port bindings."
    )
    parser.add_argument(
        "--config",
        required=True,
        help="YAML agent/fleet configuration file",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=kafka.get("bootstrap_servers", "localhost:9092"),
        help="Kafka bootstrap servers (default: %(default)s)",
    )
    parser.add_argument(
        "--sasl-username",
        default=kafka.get("sasl_username"),
        help="SASL username for Kafka/Redpanda authentication",
    )
    parser.add_argument(
        "--sasl-password",
        default=kafka.get("sasl_password"),
        help="SASL password for Kafka/Redpanda authentication",
    )
    parser.add_argument(
        "--topic",
        default=kafka.get("topic", "port-stats"),
        help="Kafka port-statistics topic (default: %(default)s)",
    )
    parser.add_argument(
        "--instance-topic",
        default=kafka.get("instance_topic", "instance-stats"),
        help="Kafka CPU/RAM topic (default: %(default)s)",
    )
    parser.add_argument(
        "--disk-topic",
        default=kafka.get("disk_topic", "disk-stats"),
        help="Kafka per-disk statistics topic (default: %(default)s)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(config.get("interval", 10)),
        help="Seconds between polls (default: %(default)s)",
    )
    parser.add_argument(
        "--metrics",
        type=parse_metrics,
        default=metrics_default,
        help=(
            "Comma-separated metrics: network,disk,ram,cpu "
            f"(default: {DEFAULT_METRICS})"
        ),
    )
    parser.add_argument(
        "--region-name",
        default=config.get("region_name", "RegionOne"),
        help="OpenStack region sent with each sample (default: %(default)s)",
    )
    parser.add_argument(
        "--libvirt-uri",
        default=libvirt_config.get("uri", "qemu:///system"),
        help="Read-only libvirt connection URI (default: %(default)s)",
    )
    parser.add_argument(
        "--libvirt-auth-file",
        default=libvirt_config.get("auth_file"),
        help="INI file containing SASL credentials for a remote libvirt URI",
    )
    parser.add_argument(
        "--libvirt-username",
        default=libvirt_config.get("username"),
        help="SASL username for a remote libvirt URI",
    )
    parser.add_argument(
        "--libvirt-password",
        default=libvirt_config.get("password"),
        help="SASL password for a remote libvirt URI",
    )
    parser.add_argument(
        "--libvirt-auth-section",
        default=libvirt_config.get("auth_section", "credentials-default"),
        help="Section in --libvirt-auth-file (default: %(default)s)",
    )
    parser.add_argument(
        "--ovn-sb-db",
        default=ovn.get("sb_db"),
        help=(
            "OVN SBDB connection string; required for network metrics and "
            "comma-separated remotes are supported"
        ),
    )
    parser.add_argument(
        "--ovn-chassis",
        default=ovn.get("chassis"),
        help=(
            "Local OVN chassis name (Open_vSwitch external_ids:system-id); "
            "required for network metrics"
        ),
    )
    parser.add_argument(
        "--ovsdb-private-key",
        default=ovn.get("private_key"),
        help="PEM private key for an ssl: OVSDB remote",
    )
    parser.add_argument(
        "--ovsdb-certificate",
        default=ovn.get("certificate"),
        help="PEM certificate for an ssl: OVSDB remote",
    )
    parser.add_argument(
        "--ovsdb-ca-cert",
        default=ovn.get("ca_cert"),
        help="PEM CA certificate for an ssl: OVSDB remote",
    )
    parser.add_argument(
        "--command-timeout", type=int, default=int(config.get("command_timeout", 5))
    )
    parser.add_argument("--once", action="store_true", help="Run one poll then exit")
    parser.add_argument(
        "--publish-kafka",
        action=argparse.BooleanOptionalAction,
        default=bool(config.get("publish_kafka", False)),
        help="Publish port, instance, and disk samples to Kafka for ClickHouse",
    )
    parser.add_argument(
        "--verbose", action="store_true", default=bool(config.get("verbose", False))
    )
    args = parser.parse_args(argv)
    if args.interval <= 0:
        parser.error("--interval must be positive")
    if not args.region_name:
        parser.error("--region-name must not be empty")
    if "network" in args.metrics and not args.ovn_sb_db:
        parser.error("--ovn-sb-db is required when network metrics are enabled")
    if bool(args.sasl_username) != bool(args.sasl_password):
        parser.error("Kafka sasl_username and sasl_password must be used together")
    try:
        args.hosts = parse_host_configs(config, args)
    except CommandError as err:
        parser.error(str(err))
    return args


class OVSDBClients:
    """Persistent native OVSDB IDL client for OVN Southbound."""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        try:
            from ovs import stream
            from ovsdbapp.backend.ovs_idl import connection
            from ovsdbapp.schema.ovn_southbound import impl_idl as ovn_impl
        except ImportError as err:
            raise CommandError(
                "install ovsdbapp and python3-openvswitch for direct OVSDB access"
            ) from err
        try:
            if any(
                remote.strip().startswith("ssl:")
                for remote in args.ovn_sb_db.split(",")
            ):
                if not all(
                    (
                        args.ovsdb_private_key,
                        args.ovsdb_certificate,
                        args.ovsdb_ca_cert,
                    )
                ):
                    raise CommandError(
                        "an ssl: OVN SBDB remote requires --ovsdb-private-key, "
                        "--ovsdb-certificate, and --ovsdb-ca-cert"
                    )
                stream.Stream.ssl_set_private_key_file(args.ovsdb_private_key)
                stream.Stream.ssl_set_certificate_file(args.ovsdb_certificate)
                stream.Stream.ssl_set_ca_cert_file(args.ovsdb_ca_cert)
            sb_conn = self._resolve_to_ipv4(args.ovn_sb_db)
            sb_idl = connection.OvsdbIdl.from_server(
                sb_conn,
                "OVN_Southbound",
                helper_tables=["Port_Binding", "Chassis"],
                leader_only=False,
            )
            self.sb = ovn_impl.OvnSbApiIdlImpl(
                connection.Connection(sb_idl, args.command_timeout)
            )
        except Exception as err:
            raise CommandError(
                f"cannot connect to OVN SBDB: {exception_text(err)}"
            ) from err

    @staticmethod
    def _resolve_to_ipv4(conn_str: str) -> str:
        import re
        def _replace(m):
            proto, host, port = m.group(1), m.group(2), m.group(3)
            try:
                ip = socket.getaddrinfo(host, None, socket.AF_INET)[0][4][0]
                return f"{proto}:{ip}:{port}"
            except socket.gaierror:
                return m.group(0)
        return re.sub(r'(tcp|ssl):([^:,]+):(\d+)', _replace, conn_str)

    @staticmethod
    def _chassis_names(row: Any) -> frozenset[str]:
        names: set[str] = set()
        for chassis in getattr(row, "chassis", ()):
            name = getattr(chassis, "name", None)
            if isinstance(name, str) and name:
                names.add(name)
        return frozenset(names)

    @staticmethod
    def _optional_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (list, tuple, set, frozenset)):
            return bool(value) and bool(next(iter(value)))
        return False

    def list_port_bindings(self) -> list[PortBinding]:
        """Return an immutable snapshot of the current SBDB port bindings."""
        try:
            rows = list(self.sb.tables["Port_Binding"].rows.values())
        except Exception as err:
            raise CommandError(f"cannot read SBDB Port_Binding rows: {err}") from err

        bindings: list[PortBinding] = []
        for row in rows:
            port_uuid = as_uuid(getattr(row, "logical_port", None))
            external_ids = getattr(row, "external_ids", None)
            if port_uuid is None or not isinstance(external_ids, dict):
                continue
            bindings.append(
                PortBinding(
                    port_uuid=port_uuid,
                    external_ids=dict(external_ids),
                    chassis_names=self._chassis_names(row),
                    up=self._optional_bool(getattr(row, "up", ())),
                    port_type=getattr(row, "type", "") or "",
                )
            )
        return bindings

    def port_ids_for_taps(
        self, tap_keys: set[tuple[str, str]]
    ) -> dict[tuple[str, str], str]:
        return port_ids_for_taps(self.list_port_bindings(), tap_keys)


def as_uuid(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        return str(uuid.UUID(value))
    except ValueError:
        return None


def port_ids_for_taps(
    bindings: list[PortBinding] | tuple[PortBinding, ...],
    tap_keys: set[tuple[str, str]],
) -> dict[tuple[str, str], str]:
    """Map unresolved `(instance UUID, tap name)` pairs from an SBDB snapshot."""
    requested: dict[str, list[tuple[tuple[str, str], str]]] = {}
    for instance_uuid, interface_name in tap_keys:
        if not interface_name.startswith("tap"):
            continue
        prefix = interface_name[3:].replace("-", "").lower()
        if prefix:
            requested.setdefault(instance_uuid, []).append(
                ((instance_uuid, interface_name), prefix)
            )
    if not requested:
        return {}
    port_ids: dict[tuple[str, str], str] = {}
    ambiguous: set[tuple[str, str]] = set()
    for binding in bindings:
        port_uuid = binding.port_uuid
        external_ids = binding.external_ids
        instance_uuid = as_uuid(external_ids.get("neutron:device_id"))
        if instance_uuid not in requested:
            continue
        if external_ids.get("neutron:device_owner") != "compute:nova":
            continue
        normalized_port = port_uuid.replace("-", "")
        for key, prefix in requested[instance_uuid]:
            if normalized_port.startswith(prefix):
                if key in ambiguous:
                    continue
                existing = port_ids.get(key)
                if existing is not None and existing != port_uuid:
                    LOG.warning("ambiguous SBDB tap-prefix match for %s", key[1])
                    port_ids.pop(key, None)
                    ambiguous.add(key)
                elif key not in port_ids:
                    port_ids[key] = port_uuid
    return port_ids


class OVNBindingSnapshot:
    """Immutable SBDB data safe to share with concurrent libvirt threads."""

    def __init__(self, bindings: list[PortBinding]):
        self.bindings = tuple(bindings)
        metadata_by_chassis: dict[str, dict[str, PortMetadata]] = {}
        for binding in self.bindings:
            if binding.port_type != "" or not binding.up:
                continue
            external_ids = binding.external_ids
            if external_ids.get("neutron:device_owner") != "compute:nova":
                continue
            network_name = external_ids.get("neutron:network_name")
            if not isinstance(network_name, str) or not network_name.startswith(
                "neutron-"
            ):
                continue
            network_uuid = as_uuid(network_name.removeprefix("neutron-"))
            instance_uuid = as_uuid(external_ids.get("neutron:device_id"))
            if network_uuid is None or instance_uuid is None:
                continue
            metadata = PortMetadata(binding.port_uuid, network_uuid, instance_uuid)
            for chassis_name in binding.chassis_names:
                metadata_by_chassis.setdefault(chassis_name, {})[
                    binding.port_uuid
                ] = metadata
        self._metadata_by_chassis = metadata_by_chassis

    def list_port_bindings(self) -> tuple[PortBinding, ...]:
        return self.bindings

    def port_ids_for_taps(
        self, tap_keys: set[tuple[str, str]]
    ) -> dict[tuple[str, str], str]:
        return port_ids_for_taps(self.bindings, tap_keys)

    def metadata_for_chassis(self, chassis_name: str) -> dict[str, PortMetadata]:
        return dict(self._metadata_by_chassis.get(chassis_name, {}))


class SnapshotStore:
    """Thread-safe pointer to the latest immutable OVN snapshot."""

    def __init__(self, snapshot: OVNBindingSnapshot | None):
        self._snapshot = snapshot
        self._lock = threading.Lock()

    def get(self) -> OVNBindingSnapshot | None:
        with self._lock:
            return self._snapshot

    def update(self, snapshot: OVNBindingSnapshot) -> None:
        with self._lock:
            self._snapshot = snapshot


def as_counter(value: Any) -> int | None:
    try:
        counter = int(value)
    except (TypeError, ValueError):
        return None
    return counter if counter >= 0 else None


def kib_to_bytes(value: Any) -> int | None:
    counter = as_counter(value)
    return None if counter is None else counter * 1024


class OVNResolver:
    def __init__(self, ovsdb: Any, chassis_name: str):
        self.ovsdb = ovsdb
        self.chassis_name = chassis_name
        self.cache: dict[str, PortMetadata] = {}

    def refresh(self) -> None:
        """Build the active local Nova-port map once for the current poll."""
        if isinstance(self.ovsdb, OVNBindingSnapshot):
            self.cache = self.ovsdb.metadata_for_chassis(self.chassis_name)
            return
        cache: dict[str, PortMetadata] = {}
        for binding in self.ovsdb.list_port_bindings():
            if binding.port_type != "" or not binding.up:
                continue
            # The primary chassis owns accounting during live migration. The
            # target's additional_chassis binding must not emit duplicate data.
            if self.chassis_name not in binding.chassis_names:
                continue
            external_ids = binding.external_ids
            if external_ids.get("neutron:device_owner") != "compute:nova":
                continue
            network_name = external_ids.get("neutron:network_name")
            if not isinstance(network_name, str) or not network_name.startswith("neutron-"):
                continue
            network_uuid = as_uuid(network_name.removeprefix("neutron-"))
            instance_uuid = as_uuid(external_ids.get("neutron:device_id"))
            if network_uuid is None or instance_uuid is None:
                continue
            cache[binding.port_uuid] = PortMetadata(
                port_uuid=binding.port_uuid,
                network_uuid=network_uuid,
                instance_uuid=instance_uuid,
            )
        self.cache = cache

    def resolve(self, port_uuid: str) -> PortMetadata | None:
        metadata = self.cache.get(port_uuid)
        if metadata is None:
            LOG.warning(
                "OVN SBDB has no active Port_Binding for port %s on chassis %s",
                port_uuid,
                self.chassis_name,
            )
        return metadata


class LibvirtCollector:
    """Bulk domain metrics plus a cache of libvirt XML interface IDs."""

    def __init__(self, args: argparse.Namespace, ovsdb: OVSDBClients | None):
        self.args = args
        self.ovsdb = ovsdb
        self.port_ids: dict[tuple[str, str], str] = {}
        self.connection: Any = None
        self._reconnecting = False

    def _open_connection(self, libvirt: Any) -> Any:
        if self.args.libvirt_username:
            authname = self.args.libvirt_username
            password = self.args.libvirt_password
        elif self.args.libvirt_auth_file:
            authname, password = self._credentials_from_file()
        else:
            return libvirt.openReadOnly(self.args.libvirt_uri)

        def credentials_callback(credentials: list[list[Any]], _: Any) -> int:
            for credential in credentials:
                credential_type = credential[0]
                if credential_type == libvirt.VIR_CRED_AUTHNAME:
                    credential[4] = authname
                elif credential_type == libvirt.VIR_CRED_PASSPHRASE:
                    credential[4] = password
                else:
                    return -1
            return 0

        auth = [
            [libvirt.VIR_CRED_AUTHNAME, libvirt.VIR_CRED_PASSPHRASE],
            credentials_callback,
            None,
        ]
        return libvirt.openAuth(
            self.args.libvirt_uri, auth, libvirt.VIR_CONNECT_RO
        )

    def _credentials_from_file(self) -> tuple[str, str]:
        parser = configparser.ConfigParser(interpolation=None)
        loaded = parser.read(self.args.libvirt_auth_file)
        if not loaded:
            raise CommandError(
                f"cannot read libvirt auth file {self.args.libvirt_auth_file}"
            )
        try:
            authname = parser.get(self.args.libvirt_auth_section, "authname")
            password = parser.get(self.args.libvirt_auth_section, "password")
        except (configparser.Error, KeyError) as err:
            raise CommandError(
                "libvirt auth file must contain authname and password in "
                f"[{self.args.libvirt_auth_section}]"
            ) from err
        return authname, password

    def read(self) -> CollectionBatch:
        try:
            import libvirt
        except ImportError as err:
            raise CommandError("install python3-libvirt to collect libvirt metrics") from err

        if self.connection is None:
            try:
                self.connection = self._open_connection(libvirt)
            except libvirt.libvirtError as err:
                raise CommandError(
                    f"cannot open libvirt URI {self.args.libvirt_uri}: "
                    f"{exception_text(err)}"
                ) from err
            if self.connection is None:
                raise CommandError(f"cannot open libvirt URI {self.args.libvirt_uri}")
        connection = self.connection

        try:
            stat_groups = 0
            if "cpu" in self.args.metrics:
                stat_groups |= (
                    libvirt.VIR_DOMAIN_STATS_CPU_TOTAL
                    | libvirt.VIR_DOMAIN_STATS_VCPU
                )
            if "ram" in self.args.metrics:
                stat_groups |= libvirt.VIR_DOMAIN_STATS_BALLOON
            if "disk" in self.args.metrics:
                stat_groups |= libvirt.VIR_DOMAIN_STATS_BLOCK
            if "network" in self.args.metrics:
                stat_groups |= libvirt.VIR_DOMAIN_STATS_INTERFACE
            records = connection.getAllDomainStats(
                stat_groups,
                libvirt.VIR_CONNECT_GET_ALL_DOMAINS_STATS_RUNNING,
            )
            pending: list[tuple[Any, str, str, int, int]] = []
            instances: list[InstanceSample] = []
            disks: list[DiskSample] = []
            active_keys: set[tuple[str, str]] = set()
            for domain, stats in records:
                instance_uuid = as_uuid(domain.UUIDString())
                if instance_uuid is None:
                    LOG.warning("skipping libvirt domain with invalid UUID")
                    continue

                if "cpu" in self.args.metrics:
                    cpu_time_ns = as_counter(stats.get("cpu.time"))
                    if cpu_time_ns is None:
                        LOG.warning(
                            "skipping instance metrics without cpu.time for %s",
                            instance_uuid,
                        )
                    else:
                        instances.append(
                            InstanceSample(
                                instance_uuid=instance_uuid,
                                cpu_time_ns=cpu_time_ns,
                                vcpus=as_counter(stats.get("vcpu.current")) or 0,
                                memory_actual_bytes=(
                                    kib_to_bytes(stats.get("balloon.current"))
                                    if "ram" in self.args.metrics
                                    else None
                                ),
                                memory_rss_bytes=(
                                    kib_to_bytes(stats.get("balloon.rss"))
                                    if "ram" in self.args.metrics
                                    else None
                                ),
                                memory_usable_bytes=(
                                    kib_to_bytes(stats.get("balloon.usable"))
                                    if "ram" in self.args.metrics
                                    else None
                                ),
                            )
                        )

                block_count = (
                    as_counter(stats.get("block.count")) or 0
                    if "disk" in self.args.metrics
                    else 0
                )
                for index in range(block_count):
                    device = stats.get(f"block.{index}.name")
                    if not isinstance(device, str) or not device:
                        LOG.debug("disk %d on %s has no stable device name", index, instance_uuid)
                        continue
                    disks.append(
                        DiskSample(
                            instance_uuid=instance_uuid,
                            device=device,
                            read_bytes=as_counter(stats.get(f"block.{index}.rd.bytes")),
                            write_bytes=as_counter(stats.get(f"block.{index}.wr.bytes")),
                            read_requests=as_counter(stats.get(f"block.{index}.rd.reqs")),
                            write_requests=as_counter(stats.get(f"block.{index}.wr.reqs")),
                            capacity_bytes=as_counter(stats.get(f"block.{index}.capacity")),
                            allocation_bytes=as_counter(stats.get(f"block.{index}.allocation")),
                            physical_bytes=as_counter(stats.get(f"block.{index}.physical")),
                        )
                    )

                count = (
                    as_counter(stats.get("net.count")) or 0
                    if "network" in self.args.metrics
                    else 0
                )
                for index in range(count):
                    interface_name = stats.get(f"net.{index}.name")
                    rx = as_counter(stats.get(f"net.{index}.rx.bytes"))
                    tx = as_counter(stats.get(f"net.{index}.tx.bytes"))
                    if not isinstance(interface_name, str) or rx is None or tx is None:
                        continue
                    key = (instance_uuid, interface_name)
                    active_keys.add(key)
                    pending.append((domain, instance_uuid, interface_name, rx, tx))

            # Refresh XML only for newly observed vNICs. The XML carries the
            # full OVN interface UUID, while bulk stats contain only the tap name.
            for domain, instance_uuid, interface_name, _, _ in pending:
                key = (instance_uuid, interface_name)
                if key in self.port_ids:
                    continue
                self.port_ids.update(self._port_ids_from_xml(domain, instance_uuid, libvirt))

            unresolved = {
                (instance_uuid, interface_name)
                for _, instance_uuid, interface_name, _, _ in pending
                if (instance_uuid, interface_name) not in self.port_ids
            }
            if unresolved:
                if self.ovsdb is None:
                    raise CommandError(
                        "OVN SBDB client is unavailable for network metrics"
                    )
                self.port_ids.update(self.ovsdb.port_ids_for_taps(unresolved))

            self.port_ids = {
                key: port_uuid for key, port_uuid in self.port_ids.items() if key in active_keys
            }
            samples: list[VNICSample] = []
            for _, instance_uuid, interface_name, rx, tx in pending:
                port_uuid = self.port_ids.get((instance_uuid, interface_name))
                if port_uuid is None:
                    LOG.debug("no Neutron port UUID for libvirt interface %s", interface_name)
                    continue
                samples.append(VNICSample(instance_uuid, interface_name, port_uuid, rx, tx))
            return CollectionBatch(vnics=samples, instances=instances, disks=disks)
        except libvirt.libvirtError as err:
            self.close()
            if self._reconnecting:
                raise CommandError(
                    f"libvirt scrape failed after reconnect for "
                    f"{self.args.libvirt_uri}: {exception_text(err)}"
                ) from err
            LOG.warning(
                "libvirt connection to %s failed; reconnecting in host worker: %s",
                self.args.libvirt_uri,
                exception_text(err),
            )
            self._reconnecting = True
            try:
                return self.read()
            finally:
                self._reconnecting = False

    def close(self) -> None:
        connection, self.connection = self.connection, None
        if connection is None:
            return
        try:
            connection.close()
        except Exception as err:
            LOG.warning(
                "cannot close libvirt URI %s: %s",
                self.args.libvirt_uri,
                exception_text(err),
            )

    @staticmethod
    def _port_ids_from_xml(domain: Any, instance_uuid: str, libvirt: Any) -> dict[tuple[str, str], str]:
        try:
            tree = ET.fromstring(domain.XMLDesc(0))
        except (ET.ParseError, libvirt.libvirtError) as err:
            LOG.warning("cannot read XML for domain %s: %s", instance_uuid, err)
            return {}
        port_ids: dict[tuple[str, str], str] = {}
        for iface in tree.findall("devices/interface"):
            target = iface.find("target")
            interface_name = target.get("dev") if target is not None else None
            params = iface.find("virtualport/parameters")
            interface_id = params.get("interfaceid") if params is not None else None
            port_uuid = as_uuid(interface_id)
            if interface_name and port_uuid:
                port_ids[(instance_uuid, interface_name)] = port_uuid
        return port_ids


def run_cycle(
    args: argparse.Namespace,
    collector: LibvirtCollector,
    resolver: OVNResolver | None,
    producer: Any,
    previous_counters: dict[tuple[str, str, str], tuple[int, int]],
    previous_cpu: dict[str, PreviousCPU],
    previous_disk_counters: dict[tuple[str, str], tuple],
    flush_producer: bool = True,
) -> CycleResult:
    now = int(time.time())
    emitted = 0
    skipped = 0
    baselines = 0
    instance_baselines = 0
    disk_baselines = 0
    active_keys: set[tuple[str, str, str]] = set()
    discovery_started = time.monotonic()
    batch = collector.read()
    discovery_seconds = time.monotonic() - discovery_started
    vnics = batch.vnics if "network" in args.metrics else []
    instances = batch.instances if "cpu" in args.metrics else []
    disks = batch.disks if "disk" in args.metrics else []
    resolution_seconds = 0.0
    if vnics:
        if resolver is None:
            raise CommandError("OVN SBDB resolver is unavailable for network metrics")
        resolution_started = time.monotonic()
        resolver.refresh()
        resolution_seconds = time.monotonic() - resolution_started
    for vnic in vnics:
        assert resolver is not None
        metadata = resolver.resolve(vnic.port_uuid)
        if metadata is None:
            skipped += 1
            continue
        if metadata.instance_uuid != vnic.instance_uuid:
            LOG.warning(
                "OVN port %s belongs to %s, not libvirt domain %s",
                vnic.port_uuid,
                metadata.instance_uuid,
                vnic.instance_uuid,
            )
            skipped += 1
            continue
        key = (vnic.instance_uuid, vnic.port_uuid, vnic.interface_name)
        active_keys.add(key)
        previous = previous_counters.get(key)
        previous_counters[key] = (vnic.rx, vnic.tx)
        if previous is None:
            baselines += 1
            continue
        previous_rx, previous_tx = previous
        rx_delta = vnic.rx if vnic.rx < previous_rx else vnic.rx - previous_rx
        tx_delta = vnic.tx if vnic.tx < previous_tx else vnic.tx - previous_tx
        sample = {
            "ts": now,
            "host": args.host,
            "region_name": args.region_name,
            "instance_uuid": vnic.instance_uuid,
            "port_uuid": metadata.port_uuid,
            "network_uuid": metadata.network_uuid,
            "rx_bytes": rx_delta,
            "tx_bytes": tx_delta,
        }
        if args.publish_kafka:
            producer.produce(args.topic, key=vnic.port_uuid, value=json.dumps(sample).encode())
            producer.poll(0)
        emitted += 1
        LOG.debug("sampled %s on %s", vnic.port_uuid, vnic.interface_name)

    active_instances = {sample.instance_uuid for sample in instances}
    now_mono = time.monotonic()
    for instance in instances:
        prev = previous_cpu.get(instance.instance_uuid)
        if prev is None or instance.vcpus == 0:
            cpu_pct = None
            instance_baselines += 1
        else:
            dt = now_mono - prev.ts
            if dt <= 0:
                cpu_pct = None
            else:
                delta_ns = instance.cpu_time_ns - prev.cpu_time_ns
                if delta_ns < 0:
                    delta_ns = instance.cpu_time_ns
                capacity_ns = dt * 1e9 * instance.vcpus
                cpu_pct = round(100.0 * delta_ns / capacity_ns, 2)
                cpu_pct = max(0.0, min(100.0, cpu_pct))
        previous_cpu[instance.instance_uuid] = PreviousCPU(
            cpu_time_ns=instance.cpu_time_ns, ts=now_mono,
        )
        if cpu_pct is None:
            continue
        sample = {
            "ts": now,
            "host": args.host,
            "region_name": args.region_name,
            "instance_uuid": instance.instance_uuid,
            "cpu_pct": cpu_pct,
            "memory_actual_bytes": instance.memory_actual_bytes,
            "memory_rss_bytes": instance.memory_rss_bytes,
            "memory_usable_bytes": instance.memory_usable_bytes,
        }
        if args.publish_kafka:
            producer.produce(
                args.instance_topic,
                key=instance.instance_uuid,
                value=json.dumps(sample).encode(),
            )
            producer.poll(0)

    active_disks = {(sample.instance_uuid, sample.device) for sample in disks}
    for disk in disks:
        key = (disk.instance_uuid, disk.device)
        prev_disk = previous_disk_counters.get(key)
        previous_disk_counters[key] = (
            disk.read_bytes, disk.write_bytes,
            disk.read_requests, disk.write_requests,
        )
        if prev_disk is None:
            disk_baselines += 1
            continue
        prev_rb, prev_wb, prev_rr, prev_wr = prev_disk

        def _delta(cur, prev):
            if cur is None or prev is None:
                return None
            return cur if cur < prev else cur - prev

        sample = {
            "ts": now,
            "host": args.host,
            "region_name": args.region_name,
            "instance_uuid": disk.instance_uuid,
            "device": disk.device,
            "read_bytes": _delta(disk.read_bytes, prev_rb),
            "write_bytes": _delta(disk.write_bytes, prev_wb),
            "read_requests": _delta(disk.read_requests, prev_rr),
            "write_requests": _delta(disk.write_requests, prev_wr),
            "capacity_bytes": disk.capacity_bytes,
            "allocation_bytes": disk.allocation_bytes,
            "physical_bytes": disk.physical_bytes,
        }
        if args.publish_kafka:
            producer.produce(
                args.disk_topic,
                key=f"{disk.instance_uuid}/{disk.device}",
                value=json.dumps(sample).encode(),
            )
            producer.poll(0)

    for key in tuple(previous_counters):
        if key not in active_keys:
            del previous_counters[key]
    for iid in list(previous_cpu):
        if iid not in active_instances:
            del previous_cpu[iid]
    for dk in list(previous_disk_counters):
        if dk not in active_disks:
            del previous_disk_counters[dk]
    if args.publish_kafka and flush_producer:
        outstanding = producer.flush(30)
        if outstanding:
            raise RuntimeError(f"Kafka timed out with {outstanding} undelivered messages")
    return CycleResult(
        emitted=emitted,
        skipped=skipped,
        baselines=baselines,
        instances=len(instances),
        instance_baselines=instance_baselines,
        disks=len(disks),
        disk_baselines=disk_baselines,
        discovery_seconds=discovery_seconds,
        resolution_seconds=resolution_seconds,
    )


def build_node_runtimes(
    args: argparse.Namespace, ovsdb: OVSDBClients | None
) -> list[NodeRuntime]:
    runtimes: list[NodeRuntime] = []
    for host in args.hosts:
        node_args = argparse.Namespace(**vars(args))
        node_args.host = host.host
        node_args.libvirt_uri = host.libvirt_uri
        node_args.ovn_chassis = host.ovn_chassis
        node_args.region_name = host.region_name
        node_args.libvirt_username = host.libvirt_username
        node_args.libvirt_password = host.libvirt_password
        node_args.libvirt_auth_file = host.libvirt_auth_file
        node_args.libvirt_auth_section = host.libvirt_auth_section
        collector = LibvirtCollector(node_args, ovsdb)
        resolver = (
            OVNResolver(ovsdb, host.ovn_chassis)
            if ovsdb is not None and host.ovn_chassis is not None
            else None
        )
        runtimes.append(NodeRuntime(node_args, collector, resolver))
    return runtimes


def run_node_cycle(
    runtime: NodeRuntime, producer: Any
) -> CycleResult:
    return run_cycle(
        runtime.args,
        runtime.collector,
        runtime.resolver,
        producer,
        runtime.previous_counters,
        runtime.previous_cpu,
        runtime.previous_disk_counters,
        flush_producer=False,
    )


class QueuedPublisher:
    """Serialize all access to a Kafka producer in one publisher thread."""

    def __init__(self, producer: Any):
        self.producer = producer
        self.records: queue.Queue[Any] = queue.Queue()
        self._error: BaseException | None = None
        self._error_lock = threading.Lock()
        self.thread = threading.Thread(
            target=self._run,
            name="kafka-publisher",
        )
        self.thread.start()

    def produce(self, topic: str, key: str, value: bytes) -> None:
        self._raise_if_failed()
        self.records.put(PublishRecord(topic=topic, key=key, value=value))

    def poll(self, timeout: float) -> int:
        # Delivery polling belongs to the publisher thread along with produce().
        return 0

    def flush(self, timeout: float) -> int:
        self._raise_if_failed()
        barrier = PublishBarrier(deadline=time.monotonic() + timeout)
        self.records.put(barrier)
        if not barrier.event.wait(timeout):
            return max(1, self.records.qsize())
        if barrier.error is not None:
            raise RuntimeError(
                f"Kafka publisher failed: {exception_text(barrier.error)}"
            ) from barrier.error
        return barrier.outstanding

    def close(self, timeout: float = 30) -> None:
        try:
            outstanding = self.flush(timeout)
            if outstanding:
                LOG.error(
                    "Kafka shutdown timed out with %d undelivered messages",
                    outstanding,
                )
        finally:
            self.records.put(None)
            self.thread.join(timeout)
            if self.thread.is_alive():
                LOG.error("Kafka publisher thread did not stop within %.1fs", timeout)

    def _set_error(self, error: BaseException) -> None:
        with self._error_lock:
            if self._error is None:
                self._error = error

    def _raise_if_failed(self) -> None:
        with self._error_lock:
            error = self._error
        if error is not None:
            raise RuntimeError(
                f"Kafka publisher failed: {exception_text(error)}"
            ) from error

    def _run(self) -> None:
        while True:
            item = self.records.get()
            if item is None:
                return
            if isinstance(item, PublishBarrier):
                try:
                    self._raise_if_failed()
                    item.outstanding = self.producer.flush(
                        max(0.0, item.deadline - time.monotonic())
                    )
                except BaseException as err:
                    self._set_error(err)
                    item.error = err
                finally:
                    item.event.set()
                continue
            try:
                self.producer.produce(item.topic, key=item.key, value=item.value)
                self.producer.poll(0)
            except BaseException as err:
                self._set_error(err)


class HostWorker:
    """Dedicated host thread that exclusively owns its libvirt connection."""

    def __init__(
        self,
        runtime: NodeRuntime,
        producer: Any,
        snapshots: SnapshotStore,
        interval: float,
        once: bool = False,
    ):
        self.runtime = runtime
        self.producer = producer
        self.snapshots = snapshots
        self.interval = interval
        self.once = once
        self.stop_event = threading.Event()
        self.thread = threading.Thread(
            target=self._run,
            name=f"libvirt-{runtime.args.host}",
        )

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()

    def join(self, timeout: float | None = None) -> None:
        self.thread.join(timeout)

    def _run(self) -> None:
        try:
            while not self.stop_event.is_set():
                started = time.monotonic()
                snapshot = self.snapshots.get()
                if snapshot is not None:
                    self.runtime.collector.ovsdb = snapshot
                    if self.runtime.resolver is not None:
                        self.runtime.resolver.ovsdb = snapshot
                try:
                    result = run_node_cycle(self.runtime, self.producer)
                except (CommandError, RuntimeError) as err:
                    LOG.error(
                        "host %s cycle failed: %s",
                        self.runtime.args.host,
                        exception_text(err),
                    )
                except Exception:
                    LOG.exception(
                        "host %s cycle failed unexpectedly",
                        self.runtime.args.host,
                    )
                else:
                    log_cycle_result(self.runtime, result)
                if self.once:
                    return
                self.stop_event.wait(
                    max(0.0, self.interval - (time.monotonic() - started))
                )
        finally:
            self.runtime.collector.close()


def log_cycle_result(runtime: NodeRuntime, result: CycleResult) -> None:
    LOG.info(
        "host %s cycle: %d interfaces (%d baselines), "
        "%d instances (%d baselines), %d disks (%d baselines), "
        "%d unresolved/non-Nova ports, %d cached ports; "
        "queries: discovery=%.3fs resolution=%.3fs total=%.3fs",
        runtime.args.host,
        result.emitted,
        result.baselines,
        result.instances,
        result.instance_baselines,
        result.disks,
        result.disk_baselines,
        result.skipped,
        len(runtime.resolver.cache) if runtime.resolver is not None else 0,
        result.discovery_seconds,
        result.resolution_seconds,
        result.query_seconds,
    )


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    LOG.info("enabled metrics: %s", ",".join(sorted(args.metrics)))
    LOG.info(
        "configured %d hosts; each host has a dedicated scrape thread",
        len(args.hosts),
    )
    kafka_producer = None
    if args.publish_kafka:
        try:
            from confluent_kafka import Producer
        except ImportError:
            LOG.error("install confluent-kafka or omit --publish-kafka")
            return 2
        producer_conf = {"bootstrap.servers": args.bootstrap_servers, "linger.ms": 50}
        if args.sasl_username and args.sasl_password:
            producer_conf.update({
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanism": "SCRAM-SHA-256",
                "sasl.username": args.sasl_username,
                "sasl.password": args.sasl_password,
            })
        kafka_producer = Producer(producer_conf)

    try:
        ovsdb = OVSDBClients(args) if "network" in args.metrics else None
    except CommandError as err:
        LOG.error("agent startup failed: %s", exception_text(err))
        return 2
    runtimes = build_node_runtimes(args, ovsdb)
    snapshots = SnapshotStore(
        OVNBindingSnapshot(ovsdb.list_port_bindings()) if ovsdb is not None else None
    )
    producer = QueuedPublisher(kafka_producer) if kafka_producer is not None else None
    workers = [
        HostWorker(runtime, producer, snapshots, args.interval, once=args.once)
        for runtime in runtimes
    ]
    started_workers: list[HostWorker] = []
    try:
        for worker in workers:
            worker.start()
            started_workers.append(worker)
        if args.once:
            for worker in started_workers:
                worker.join()
            return 0
        while True:
            time.sleep(args.interval)
            if ovsdb is not None:
                try:
                    snapshots.update(
                        OVNBindingSnapshot(ovsdb.list_port_bindings())
                    )
                except (CommandError, RuntimeError) as err:
                    LOG.error(
                        "OVN SBDB snapshot refresh failed; keeping previous snapshot: %s",
                        exception_text(err),
                    )
            if producer is not None:
                try:
                    outstanding = producer.flush(30)
                    if outstanding:
                        LOG.error(
                            "Kafka timed out with %d undelivered messages",
                            outstanding,
                        )
                except RuntimeError as err:
                    LOG.error("Kafka flush failed: %s", exception_text(err))
    except KeyboardInterrupt:
        return 0
    finally:
        for worker in started_workers:
            worker.stop()
        for worker in started_workers:
            worker.join()
        if producer is not None:
            try:
                producer.close()
            except RuntimeError as err:
                LOG.error("Kafka publisher shutdown failed: %s", exception_text(err))


if __name__ == "__main__":
    sys.exit(main())
