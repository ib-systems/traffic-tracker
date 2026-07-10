#!/usr/bin/env python3
"""PoC compute-node traffic agent for ML2/OVN.

Reads bulk per-vNIC counters from libvirt, obtains the Neutron port UUID from
the domain XML or the OVN tap-name convention, and resolves active ports in
OVN Northbound:

    Logical_Switch_Port.name == port UUID
    external_ids["neutron:network_name"] == neutron-<network UUID>

Each poll reports the reset-aware traffic delta since the prior poll. The first
observation of an interface is a zero-value baseline.
"""

from __future__ import annotations

import argparse
import configparser
import json
import logging
import os
import socket
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any


LOG = logging.getLogger("ovn-traffic-agent")


@dataclass(frozen=True)
class PortMetadata:
    port_uuid: str
    network_uuid: str
    instance_uuid: str


@dataclass(frozen=True)
class VNICSample:
    instance_uuid: str
    interface_name: str
    port_uuid: str
    rx: int
    tx: int


@dataclass(frozen=True)
class CycleResult:
    emitted: int
    skipped: int
    baselines: int
    discovery_seconds: float
    resolution_seconds: float

    @property
    def query_seconds(self) -> float:
        return self.discovery_seconds + self.resolution_seconds


class CommandError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish libvirt vNIC counters enriched by OVS and OVN NBDB."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: %(default)s)",
    )
    parser.add_argument(
        "--topic", default=os.getenv("KAFKA_TOPIC", "port-stats"),
        help="Kafka topic (default: %(default)s)",
    )
    parser.add_argument(
        "--interval", type=float, default=float(os.getenv("POLL_INTERVAL", "10")),
        help="Seconds between polls (default: %(default)s)",
    )
    parser.add_argument(
        "--host",
        default=os.getenv("HOSTNAME", socket.gethostname()),
        help="Host label sent with each sample (default: local hostname)",
    )
    parser.add_argument(
        "--region-name",
        default=os.getenv("REGION_NAME", "RegionOne"),
        help="OpenStack region sent with each sample (default: %(default)s)",
    )
    parser.add_argument(
        "--libvirt-uri",
        default=os.getenv("LIBVIRT_URI", "qemu:///system"),
        help="Read-only libvirt connection URI (default: %(default)s)",
    )
    parser.add_argument(
        "--libvirt-auth-file",
        default=os.getenv("LIBVIRT_AUTH_FILE"),
        help="INI file containing SASL credentials for a remote libvirt URI",
    )
    parser.add_argument(
        "--libvirt-username",
        default=os.getenv("LIBVIRT_USERNAME"),
        help="SASL username for a remote libvirt URI",
    )
    parser.add_argument(
        "--libvirt-password",
        default=os.getenv("LIBVIRT_PASSWORD"),
        help="SASL password for a remote libvirt URI",
    )
    parser.add_argument(
        "--libvirt-auth-section",
        default=os.getenv("LIBVIRT_AUTH_SECTION", "credentials-default"),
        help="Section in --libvirt-auth-file (default: %(default)s)",
    )
    parser.add_argument(
        "--ovn-nb-db",
        default=os.getenv("OVN_NB_DB"),
        required=os.getenv("OVN_NB_DB") is None,
        help="OVN NBDB connection string; comma-separated remotes are supported",
    )
    parser.add_argument(
        "--ovsdb-private-key", default=os.getenv("OVSDB_PRIVATE_KEY"),
        help="PEM private key for an ssl: OVSDB remote",
    )
    parser.add_argument(
        "--ovsdb-certificate", default=os.getenv("OVSDB_CERTIFICATE"),
        help="PEM certificate for an ssl: OVSDB remote",
    )
    parser.add_argument(
        "--ovsdb-ca-cert", default=os.getenv("OVSDB_CA_CERT"),
        help="PEM CA certificate for an ssl: OVSDB remote",
    )
    parser.add_argument("--command-timeout", type=int, default=5)
    parser.add_argument("--once", action="store_true", help="Run one poll then exit")
    parser.add_argument(
        "--publish-kafka",
        action="store_true",
        help="Also publish cumulative samples to Kafka for the ClickHouse pipeline",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("--interval must be positive")
    if not args.region_name:
        parser.error("--region-name must not be empty")
    if bool(args.libvirt_username) != bool(args.libvirt_password):
        parser.error("--libvirt-username and --libvirt-password must be used together")
    return args


class OVSDBClients:
    """Persistent native OVSDB IDL client for OVN Northbound."""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        try:
            from ovs import stream
            from ovsdbapp.backend.ovs_idl import connection
            from ovsdbapp.schema.ovn_northbound import impl_idl as ovn_impl
        except ImportError as err:
            raise CommandError(
                "install ovsdbapp and python3-openvswitch for direct OVSDB access"
            ) from err
        try:
            if any(remote.strip().startswith("ssl:") for remote in args.ovn_nb_db.split(",")):
                if not all((args.ovsdb_private_key, args.ovsdb_certificate, args.ovsdb_ca_cert)):
                    raise CommandError(
                        "an ssl: OVN NBDB remote requires --ovsdb-private-key, "
                        "--ovsdb-certificate, and --ovsdb-ca-cert"
                    )
                stream.Stream.ssl_set_private_key_file(args.ovsdb_private_key)
                stream.Stream.ssl_set_certificate_file(args.ovsdb_certificate)
                stream.Stream.ssl_set_ca_cert_file(args.ovsdb_ca_cert)
            nb_idl = connection.OvsdbIdl.from_server(
                args.ovn_nb_db,
                "OVN_Northbound",
                helper_tables=["Logical_Switch_Port"],
                leader_only=False,
            )
            self.nb = ovn_impl.OvnNbApiIdlImpl(
                connection.Connection(nb_idl, args.command_timeout)
            )
        except Exception as err:
            raise CommandError(f"cannot connect to OVN NBDB: {err}") from err

    def find_lsp(self, port_uuid: str) -> list[dict[str, Any]]:
        try:
            return self.nb.db_find(
                "Logical_Switch_Port",
                ("name", "=", port_uuid),
                columns=("external_ids",),
            ).execute(check_error=True)
        except Exception as err:
            raise CommandError(f"NBDB lookup for {port_uuid} failed: {err}") from err

    def port_ids_for_taps(
        self, tap_keys: set[tuple[str, str]]
    ) -> dict[tuple[str, str], str]:
        """Map unresolved `(instance UUID, tap name)` pairs from NBDB.

        Nova tap names carry a prefix of the Neutron port UUID. Pairing that
        prefix with `neutron:device_id` avoids a local Open_vSwitch OVSDB
        query when a domain XML omits `virtualport/interfaceid`.
        """
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
        try:
            rows = self.nb.db_list(
                "Logical_Switch_Port", columns=("name", "external_ids")
            ).execute(check_error=True)
        except Exception as err:
            raise CommandError(f"NBDB tap-prefix lookup failed: {err}") from err
        port_ids: dict[tuple[str, str], str] = {}
        ambiguous: set[tuple[str, str]] = set()
        for row in rows:
            port_uuid = as_uuid(row.get("name"))
            external_ids = row.get("external_ids")
            if port_uuid is None or not isinstance(external_ids, dict):
                continue
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
                        LOG.warning("ambiguous NBDB tap-prefix match for %s", key[1])
                        port_ids.pop(key, None)
                        ambiguous.add(key)
                    elif key not in port_ids:
                        port_ids[key] = port_uuid
        return port_ids


def as_uuid(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        return str(uuid.UUID(value))
    except ValueError:
        return None


def as_counter(value: Any) -> int | None:
    try:
        counter = int(value)
    except (TypeError, ValueError):
        return None
    return counter if counter >= 0 else None


class OVNResolver:
    def __init__(self, ovsdb: OVSDBClients):
        self.ovsdb = ovsdb
        self.cache: dict[str, PortMetadata] = {}

    def resolve(self, port_uuid: str) -> PortMetadata | None:
        cached = self.cache.get(port_uuid)
        if cached is not None:
            return cached

        rows = self.ovsdb.find_lsp(port_uuid)
        if not rows:
            LOG.warning("OVN has no active Logical_Switch_Port for %s", port_uuid)
            return None
        if len(rows) != 1:
            LOG.warning("OVN returned %d Logical_Switch_Port rows for %s", len(rows), port_uuid)
            return None

        external_ids = rows[0].get("external_ids")
        if not isinstance(external_ids, dict):
            LOG.warning("OVN port %s has no external_ids", port_uuid)
            return None
        network_name = external_ids.get("neutron:network_name")
        if not isinstance(network_name, str) or not network_name.startswith("neutron-"):
            LOG.warning("OVN port %s has no Neutron logical-switch name", port_uuid)
            return None
        network_uuid = as_uuid(network_name.removeprefix("neutron-"))
        instance_uuid = as_uuid(external_ids.get("neutron:device_id"))
        if network_uuid is None or instance_uuid is None:
            LOG.warning("OVN port %s has invalid network or instance UUID metadata", port_uuid)
            return None
        if external_ids.get("neutron:device_owner") != "compute:nova":
            LOG.debug("skipping non-Nova port %s", port_uuid)
            return None

        metadata = PortMetadata(
            port_uuid=port_uuid,
            network_uuid=network_uuid,
            instance_uuid=instance_uuid,
        )
        self.cache[port_uuid] = metadata
        return metadata


class LibvirtCollector:
    """Bulk vNIC counters plus a cache of libvirt XML interface IDs."""

    def __init__(self, args: argparse.Namespace, ovsdb: OVSDBClients):
        self.args = args
        self.ovsdb = ovsdb
        self.port_ids: dict[tuple[str, str], str] = {}

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

    def read(self) -> list[VNICSample]:
        try:
            import libvirt
        except ImportError as err:
            raise CommandError("install python3-libvirt to collect libvirt vNIC metrics") from err

        try:
            connection = self._open_connection(libvirt)
        except libvirt.libvirtError as err:
            raise CommandError(f"cannot open libvirt URI {self.args.libvirt_uri}: {err}") from err
        if connection is None:
            raise CommandError(f"cannot open libvirt URI {self.args.libvirt_uri}")

        try:
            records = connection.getAllDomainStats(
                libvirt.VIR_DOMAIN_STATS_INTERFACE,
                libvirt.VIR_CONNECT_GET_ALL_DOMAINS_STATS_RUNNING,
            )
            pending: list[tuple[Any, str, str, int, int]] = []
            active_keys: set[tuple[str, str]] = set()
            for domain, stats in records:
                instance_uuid = as_uuid(domain.UUIDString())
                if instance_uuid is None:
                    LOG.warning("skipping libvirt domain with invalid UUID")
                    continue
                count = as_counter(stats.get("net.count")) or 0
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
            return samples
        finally:
            connection.close()

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
    resolver: OVNResolver,
    producer: Any,
    previous_counters: dict[tuple[str, str, str], tuple[int, int]],
) -> CycleResult:
    now = int(time.time())
    emitted = 0
    skipped = 0
    baselines = 0
    active_keys: set[tuple[str, str, str]] = set()
    discovery_started = time.monotonic()
    vnics = collector.read()
    discovery_seconds = time.monotonic() - discovery_started
    resolution_seconds = 0.0
    for vnic in vnics:
        resolution_started = time.monotonic()
        metadata = resolver.resolve(vnic.port_uuid)
        resolution_seconds += time.monotonic() - resolution_started
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
        baseline = previous is None
        if baseline:
            rx_delta = tx_delta = 0
            baselines += 1
        else:
            previous_rx, previous_tx = previous
            rx_delta = vnic.rx if vnic.rx < previous_rx else vnic.rx - previous_rx
            tx_delta = vnic.tx if vnic.tx < previous_tx else vnic.tx - previous_tx
        print(
            f"region_name={args.region_name} instance_uuid={vnic.instance_uuid} "
            f"network_uuid={metadata.network_uuid} "
            f"port_uuid={vnic.port_uuid} interface={vnic.interface_name} "
            f"rx_mib={rx_delta / 1024 / 1024:.3f} "
            f"tx_mib={tx_delta / 1024 / 1024:.3f} "
            f"total_mib={(rx_delta + tx_delta) / 1024 / 1024:.3f} "
            f"baseline={str(baseline).lower()}",
            flush=True,
        )
        sample = {
            "ts": now,
            "host": args.host,
            "region_name": args.region_name,
            "instance_uuid": vnic.instance_uuid,
            "port_uuid": metadata.port_uuid,
            "network_uuid": metadata.network_uuid,
            "rx": vnic.rx,
            "tx": vnic.tx,
            "is_baseline": baseline,
        }
        if args.publish_kafka:
            producer.produce(args.topic, key=vnic.port_uuid, value=json.dumps(sample).encode())
            producer.poll(0)
        previous_counters[key] = (vnic.rx, vnic.tx)
        emitted += 1
        LOG.debug("sampled %s on %s", vnic.port_uuid, vnic.interface_name)
    for key in tuple(previous_counters):
        if key not in active_keys:
            del previous_counters[key]
    if args.publish_kafka:
        outstanding = producer.flush(30)
        if outstanding:
            raise RuntimeError(f"Kafka timed out with {outstanding} undelivered messages")
    return CycleResult(
        emitted=emitted,
        skipped=skipped,
        baselines=baselines,
        discovery_seconds=discovery_seconds,
        resolution_seconds=resolution_seconds,
    )


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    producer = None
    if args.publish_kafka:
        try:
            from confluent_kafka import Producer
        except ImportError:
            LOG.error("install confluent-kafka or omit --publish-kafka")
            return 2
        producer = Producer({"bootstrap.servers": args.bootstrap_servers, "linger.ms": 50})

    ovsdb = OVSDBClients(args)
    collector = LibvirtCollector(args, ovsdb)
    resolver = OVNResolver(ovsdb)
    previous_counters: dict[tuple[str, str, str], tuple[int, int]] = {}
    while True:
        started = time.monotonic()
        try:
            result = run_cycle(
                args, collector, resolver, producer, previous_counters
            )
            LOG.info(
                "cycle: %d interfaces, %d baselines, %d unresolved/non-Nova ports, "
                "%d cached ports; queries: discovery=%.3fs resolution=%.3fs total=%.3fs",
                result.emitted,
                result.baselines,
                result.skipped,
                len(resolver.cache),
                result.discovery_seconds,
                result.resolution_seconds,
                result.query_seconds,
            )
        except (CommandError, RuntimeError) as err:
            LOG.error("cycle failed: %s", err)
        if args.once:
            return 0
        time.sleep(max(0.0, args.interval - (time.monotonic() - started)))


if __name__ == "__main__":
    sys.exit(main())
