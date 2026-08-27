import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { query } from './clickhouse';
import { formatBytes } from './charts/format';
import CpuChartA from './charts/apex/CpuChartA';
import RamChartA from './charts/apex/RamChartA';
import DiskChartA from './charts/apex/DiskChartA';
import NetworkChartA from './charts/apex/NetworkChartA';
import TopUsage from './TopUsage';
import NodeView from './NodeView';
import AllNodesView from './AllNodesView';
import './App.css';

const PAGES = [
  { key: 'instance', label: 'Instance', path: '/instance' },
  { key: 'all-nodes', label: 'All Nodes', path: '/all-nodes' },
  { key: 'node', label: 'Per Node', path: '/per-node' },
  { key: 'top-usage', label: 'Top Usage', path: '/top-usage' },
];
const PAGE_BY_PATH = Object.fromEntries(PAGES.map(page => [page.path, page.key]));
const PATH_BY_PAGE = Object.fromEntries(PAGES.map(page => [page.key, page.path]));
const RANGES = [
  { key: '6h', label: '6h', hours: 6 },
  { key: '12h', label: '12h', hours: 12 },
  { key: '24h', label: '24h', hours: 24 },
  { key: '7d', label: '7d', hours: 168 },
];

function normalizedPath(pathname) {
  return pathname.replace(/\/+$/, '') || '/';
}

function pageFromPath(pathname) {
  return PAGE_BY_PATH[normalizedPath(pathname)] || 'instance';
}

function cpuQuery(id, range) {
  if (range.hours <= 6) {
    return `
      SELECT toStartOfMinute(ts) AS minute,
        round(avg(cpu_pct), 2) AS cpu_pct,
        avg(memory_actual_bytes) AS memory_actual_avg_bytes,
        avg(memory_rss_bytes) AS memory_rss_avg_bytes,
        avg(memory_usable_bytes) AS memory_usable_avg_bytes
      FROM instance_samples
      WHERE instance_uuid = '${id}' AND ts >= now() - INTERVAL 6 HOUR
      GROUP BY minute ORDER BY minute`;
  }
  if (range.hours <= 24) {
    return `
      SELECT ts5 AS minute,
        cpu_avg_pct AS cpu_pct,
        memory_actual_avg_bytes, memory_rss_avg_bytes, memory_usable_avg_bytes
      FROM instance_metrics_5min FINAL
      WHERE instance_uuid = '${id}' AND ts5 >= now() - INTERVAL ${range.hours} HOUR
      ORDER BY minute`;
  }
  return `
    SELECT hour AS minute,
      cpu_avg_pct AS cpu_pct,
      memory_actual_avg_bytes, memory_rss_avg_bytes, memory_usable_avg_bytes
    FROM instance_metrics_hourly FINAL
    WHERE instance_uuid = '${id}' AND hour >= now() - INTERVAL ${range.hours} HOUR
    ORDER BY minute`;
}

function diskQuery(id, range) {
  if (range.hours <= 6) {
    return `
      SELECT toStartOfMinute(ts) AS minute, device,
        sum(ifNull(read_bytes, 0)) AS read_bytes,
        sum(ifNull(write_bytes, 0)) AS write_bytes,
        argMax(capacity_bytes, ts) AS capacity_last_bytes,
        argMax(allocation_bytes, ts) AS allocation_last_bytes
      FROM disk_samples
      WHERE instance_uuid = '${id}' AND ts >= now() - INTERVAL 6 HOUR
      GROUP BY minute, device ORDER BY minute`;
  }
  if (range.hours <= 24) {
    return `
      SELECT ts5 AS minute, device,
        read_bytes, write_bytes,
        capacity_last_bytes, allocation_last_bytes
      FROM disk_metrics_5min FINAL
      WHERE instance_uuid = '${id}' AND ts5 >= now() - INTERVAL ${range.hours} HOUR
      ORDER BY minute`;
  }
  return `
    SELECT hour AS minute, device,
      read_bytes, write_bytes,
      capacity_last_bytes, allocation_last_bytes
    FROM disk_metrics_hourly FINAL
    WHERE instance_uuid = '${id}' AND hour >= now() - INTERVAL ${range.hours} HOUR
    ORDER BY minute`;
}

function netQuery(id, range) {
  if (range.hours <= 6) {
    return `
      SELECT toStartOfMinute(ts) AS minute,
        sum(rx_bytes) AS rx_bytes, sum(tx_bytes) AS tx_bytes
      FROM port_samples
      WHERE instance_uuid = '${id}' AND ts >= now() - INTERVAL 6 HOUR
      GROUP BY minute ORDER BY minute`;
  }
  if (range.hours <= 24) {
    return `
      SELECT ts5 AS minute,
        sum(rx_bytes) AS rx_bytes, sum(tx_bytes) AS tx_bytes
      FROM port_volume_5min FINAL
      WHERE instance_uuid = '${id}' AND ts5 >= now() - INTERVAL ${range.hours} HOUR
      GROUP BY minute ORDER BY minute`;
  }
  return `
    SELECT hour AS minute,
      sum(rx_bytes) AS rx_bytes, sum(tx_bytes) AS tx_bytes
    FROM port_volume_hourly FINAL
    WHERE instance_uuid = '${id}' AND hour >= now() - INTERVAL ${range.hours} HOUR
    GROUP BY minute ORDER BY minute`;
}

export default function App() {
  const [page, setPage] = useState(() => pageFromPath(window.location.pathname));
  const [range, setRange] = useState(RANGES[0]);
  const [instances, setInstances] = useState([]);
  const [selectedInstance, setSelectedInstance] = useState(null);
  const [cpu, setCpu] = useState([]);
  const [ram, setRam] = useState([]);
  const [disk, setDisk] = useState([]);
  const [network, setNetwork] = useState([]);
  const [instanceHost, setInstanceHost] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [search, setSearch] = useState('');
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const searchRef = useRef(null);

  useEffect(() => {
    const syncPageFromLocation = () => {
      const nextPage = pageFromPath(window.location.pathname);
      const canonicalPath = PATH_BY_PAGE[nextPage];
      if (normalizedPath(window.location.pathname) !== canonicalPath) {
        window.history.replaceState(null, '', canonicalPath);
      }
      setPage(nextPage);
    };

    syncPageFromLocation();
    window.addEventListener('popstate', syncPageFromLocation);
    return () => window.removeEventListener('popstate', syncPageFromLocation);
  }, []);

  const navigateToPage = useCallback(nextPage => {
    const nextPath = PATH_BY_PAGE[nextPage];
    if (normalizedPath(window.location.pathname) !== nextPath) {
      window.history.pushState(null, '', nextPath);
    }
    setPage(nextPage);
  }, []);

  const filtered = useMemo(() => {
    if (!search) return instances.slice(0, 50);
    const q = search.toLowerCase();
    return instances.filter(id => id.toLowerCase().includes(q)).slice(0, 50);
  }, [instances, search]);

  useEffect(() => {
    query(`
      SELECT instance_uuid
      FROM (
        SELECT instance_uuid
        FROM port_samples
        WHERE ts >= now() - INTERVAL 6 HOUR
        UNION ALL
        SELECT instance_uuid
        FROM instance_samples
        WHERE ts >= now() - INTERVAL 6 HOUR
        UNION ALL
        SELECT instance_uuid
        FROM disk_samples
        WHERE ts >= now() - INTERVAL 6 HOUR
      )
      GROUP BY instance_uuid
      ORDER BY instance_uuid
      LIMIT 1000
    `).then(rows => {
      const uuids = rows.map(r => r.instance_uuid);
      setInstances(uuids);
      if (!selectedInstance && uuids.length > 0) setSelectedInstance(uuids[0]);
    }).catch(e => setError(e.message));
  }, []);

  const fetchMetrics = useCallback(async () => {
    if (!selectedInstance) return;
    setLoading(true);
    setError(null);
    try {
      const [cpuData, diskData, netData, hostData] = await Promise.all([
        query(cpuQuery(selectedInstance, range)),
        query(diskQuery(selectedInstance, range)),
        query(netQuery(selectedInstance, range)),
        query(`
          SELECT host
          FROM (
            SELECT host, ts FROM port_samples
            WHERE instance_uuid = '${selectedInstance}'
            UNION ALL
            SELECT host, ts FROM instance_samples
            WHERE instance_uuid = '${selectedInstance}'
            UNION ALL
            SELECT host, ts FROM disk_samples
            WHERE instance_uuid = '${selectedInstance}'
          )
          ORDER BY ts DESC
          LIMIT 1
        `),
      ]);
      setCpu(cpuData);
      setRam(cpuData);
      setDisk(diskData);
      setNetwork(netData);
      setInstanceHost(hostData.length ? hostData[0].host : null);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [selectedInstance, range]);

  useEffect(() => { fetchMetrics(); }, [fetchMetrics]);

  const netSummary = useMemo(() => {
    const rx = network.reduce((s, d) => s + (Number(d.rx_bytes) || 0), 0);
    const tx = network.reduce((s, d) => s + (Number(d.tx_bytes) || 0), 0);
    return { rx, tx };
  }, [network]);

  return (
    <div className="app">
      <header>
        <h1>Traffic Tracker</h1>
        <div className="controls">
          <div className="page-toggle">
            {PAGES.map(p => (
              <button
                key={p.key}
                className={p.key === page ? 'active' : ''}
                onClick={() => navigateToPage(p.key)}
              >{p.label}</button>
            ))}
          </div>
          {page === 'instance' && (
            <>
              <button
                className="nav-btn"
                disabled={!selectedInstance || instances.indexOf(selectedInstance) <= 0}
                onClick={() => setSelectedInstance(instances[instances.indexOf(selectedInstance) - 1])}
              >&lsaquo;</button>
              <div className="instance-search" ref={searchRef}>
                <input
                  type="text"
                  placeholder={selectedInstance || 'Search UUID...'}
                  value={search}
                  onChange={e => { setSearch(e.target.value); setDropdownOpen(true); }}
                  onFocus={() => setDropdownOpen(true)}
                  onBlur={() => setTimeout(() => setDropdownOpen(false), 150)}
                />
                {dropdownOpen && filtered.length > 0 && (
                  <ul className="instance-dropdown">
                    {filtered.map(id => (
                      <li
                        key={id}
                        className={id === selectedInstance ? 'selected' : ''}
                        onMouseDown={() => { setSelectedInstance(id); setSearch(''); setDropdownOpen(false); }}
                      >{id}</li>
                    ))}
                  </ul>
                )}
              </div>
              <button
                className="nav-btn"
                disabled={!selectedInstance || instances.indexOf(selectedInstance) >= instances.length - 1}
                onClick={() => setSelectedInstance(instances[instances.indexOf(selectedInstance) + 1])}
              >&rsaquo;</button>
              <div className="range-toggle">
                {RANGES.map(r => (
                  <button
                    key={r.key}
                    className={r.key === range.key ? 'active' : ''}
                    onClick={() => setRange(r)}
                  >{r.label}</button>
                ))}
              </div>
              <span className="range-label">{loading ? 'Loading...' : range.hours <= 6 ? '1-min' : range.hours <= 24 ? '5-min' : '1-hour'}</span>
            </>
          )}
        </div>
      </header>

      {page === 'instance' ? (
        <>
          {error && <div className="error">{error}</div>}
          {instanceHost && <div className="instance-host">Host: {instanceHost}</div>}

          <div className="grid">
            <div className="panel">
              <h2>CPU Utilization</h2>
              <CpuChartA data={cpu} />
            </div>
            <div className="panel">
              <h2>RAM Usage</h2>
              <RamChartA data={ram} />
            </div>
            <div className="panel">
              <h2>Disk I/O</h2>
              <DiskChartA data={disk} />
            </div>
            <div className="panel">
              <h2>Network Traffic</h2>
              <div className="net-summary">
                <span>RX: {formatBytes(netSummary.rx)}</span>
                <span>TX: {formatBytes(netSummary.tx)}</span>
                <span>Total: {formatBytes(netSummary.rx + netSummary.tx)}</span>
              </div>
              <NetworkChartA data={network} />
            </div>
          </div>
        </>
      ) : page === 'all-nodes' ? (
        <AllNodesView />
      ) : page === 'node' ? (
        <NodeView />
      ) : (
        <TopUsage />
      )}
    </div>
  );
}
