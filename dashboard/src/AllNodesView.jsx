import { useEffect, useState } from 'react';
import { query } from './clickhouse';
import { formatBytes } from './charts/format';
import NetworkChartA from './charts/apex/NetworkChartA';

export default function AllNodesView() {
  const [network, setNetwork] = useState([]);
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    Promise.all([
      query(`
        SELECT
          toStartOfMinute(ts) AS minute,
          sum(rx_bytes) AS rx_bytes,
          sum(tx_bytes) AS tx_bytes
        FROM port_samples
        WHERE ts >= now() - INTERVAL 6 HOUR
        GROUP BY minute
        ORDER BY minute
      `),
      query(`
        SELECT
          uniqExact(host) AS hosts,
          uniqExact(instance_uuid) AS instances,
          sum(rx_bytes) AS rx_bytes,
          sum(tx_bytes) AS tx_bytes
        FROM port_samples
        WHERE ts >= now() - INTERVAL 6 HOUR
      `),
    ]).then(([networkData, summaryData]) => {
      setNetwork(networkData);
      setSummary(summaryData[0] || null);
    }).catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const rx = Number(summary?.rx_bytes) || 0;
  const tx = Number(summary?.tx_bytes) || 0;

  return (
    <div>
      <p className="range-label fleet-range">
        {loading ? 'Loading...' : 'Last 6 hours, all nodes and instances'}
      </p>

      {error && <div className="error">{error}</div>}

      <div className="summary-grid">
        <div className="summary-stat"><span>Active nodes</span><strong>{summary?.hosts || 0}</strong></div>
        <div className="summary-stat"><span>Active instances</span><strong>{summary?.instances || 0}</strong></div>
        <div className="summary-stat"><span>RX</span><strong>{formatBytes(rx)}</strong></div>
        <div className="summary-stat"><span>TX</span><strong>{formatBytes(tx)}</strong></div>
        <div className="summary-stat"><span>Total</span><strong>{formatBytes(rx + tx)}</strong></div>
      </div>

      <div className="panel">
        <h2>Fleet Network Traffic</h2>
        <NetworkChartA data={network} />
      </div>
    </div>
  );
}
