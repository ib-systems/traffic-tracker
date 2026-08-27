import { useState, useEffect, useCallback } from 'react';
import { query } from './clickhouse';
import { formatBytes } from './charts/format';
import DiskChartA from './charts/apex/DiskChartA';
import NetworkChartA from './charts/apex/NetworkChartA';

export default function NodeView() {
  const [hosts, setHosts] = useState([]);
  const [selectedHost, setSelectedHost] = useState(null);
  const [disk, setDisk] = useState([]);
  const [network, setNetwork] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    query(`
      SELECT host
      FROM (
        SELECT host
        FROM port_samples
        WHERE ts >= now() - INTERVAL 6 HOUR
        UNION ALL
        SELECT host
        FROM instance_samples
        WHERE ts >= now() - INTERVAL 6 HOUR
        UNION ALL
        SELECT host
        FROM disk_samples
        WHERE ts >= now() - INTERVAL 6 HOUR
      )
      GROUP BY host
      ORDER BY host
    `).then(rows => {
      const h = rows.map(r => r.host);
      setHosts(h);
      if (!selectedHost && h.length > 0) setSelectedHost(h[0]);
    }).catch(e => setError(e.message));
  }, []);

  const fetchMetrics = useCallback(async () => {
    if (!selectedHost) return;
    setLoading(true);
    setError(null);
    try {
      const [diskData, netData] = await Promise.all([
        query(`
          SELECT
            toStartOfMinute(ts) AS minute,
            'all' AS device,
            sum(ifNull(read_bytes, 0)) AS read_bytes,
            sum(ifNull(write_bytes, 0)) AS write_bytes,
            Null AS capacity_last_bytes,
            Null AS allocation_last_bytes
          FROM disk_samples
          WHERE host = '${selectedHost}'
            AND ts >= now() - INTERVAL 6 HOUR
          GROUP BY minute
          ORDER BY minute
        `),
        query(`
          SELECT
            toStartOfMinute(ts) AS minute,
            sum(rx_bytes) AS rx_bytes,
            sum(tx_bytes) AS tx_bytes
          FROM port_samples
          WHERE host = '${selectedHost}'
            AND ts >= now() - INTERVAL 6 HOUR
          GROUP BY minute
          ORDER BY minute
        `),
      ]);
      setDisk(diskData);
      setNetwork(netData);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [selectedHost]);

  useEffect(() => { fetchMetrics(); }, [fetchMetrics]);

  const netTotal = network.reduce((s, d) => s + (Number(d.rx_bytes) || 0) + (Number(d.tx_bytes) || 0), 0);
  const diskTotal = disk.reduce((s, d) => s + (Number(d.read_bytes) || 0) + (Number(d.write_bytes) || 0), 0);

  return (
    <div>
      <div className="controls" style={{ marginBottom: 16 }}>
        <select
          value={selectedHost || ''}
          onChange={e => setSelectedHost(e.target.value)}
          style={{ width: 240 }}
        >
          {hosts.map(h => (
            <option key={h} value={h}>{h}</option>
          ))}
        </select>
        <span className="range-label">{loading ? 'Loading...' : 'Last 6 hours, all instances'}</span>
      </div>

      {error && <div className="error">{error}</div>}

      <div className="grid">
        <div className="panel">
          <h2>Node Disk I/O</h2>
          <div className="net-summary">
            <span>Total: {formatBytes(diskTotal)}</span>
          </div>
          <DiskChartA data={disk} />
        </div>
        <div className="panel">
          <h2>Node Network Traffic</h2>
          <div className="net-summary">
            <span>Total: {formatBytes(netTotal)}</span>
          </div>
          <NetworkChartA data={network} />
        </div>
      </div>
    </div>
  );
}
