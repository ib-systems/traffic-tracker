import { useState, useEffect } from 'react';
import { query } from './clickhouse';
import { formatBytes } from './charts/format';

const LIMIT = 10;

export default function TopUsage() {
  const [data, setData] = useState({ cpu: [], ram: [], net: [], disk: [] });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    Promise.all([
      query(`
        SELECT
          instance_uuid,
          round(avg(cpu_avg_pct), 2) AS avg_cpu_pct,
          round(max(cpu_max_pct), 2) AS max_cpu_pct
        FROM instance_metrics_hourly FINAL
        WHERE hour >= now() - INTERVAL 24 HOUR
        GROUP BY instance_uuid
        ORDER BY avg_cpu_pct DESC
        LIMIT ${LIMIT}
      `),
      query(`
        SELECT
          instance_uuid,
          avg(memory_rss_avg_bytes) AS avg_rss,
          max(memory_rss_max_bytes) AS max_rss
        FROM instance_metrics_hourly FINAL
        WHERE hour >= now() - INTERVAL 24 HOUR
        GROUP BY instance_uuid
        ORDER BY avg_rss DESC
        LIMIT ${LIMIT}
      `),
      query(`
        SELECT
          instance_uuid,
          avg(rx_bytes) AS avg_rx,
          avg(tx_bytes) AS avg_tx,
          avg(rx_bytes) + avg(tx_bytes) AS avg_total
        FROM port_volume_hourly FINAL
        WHERE hour >= now() - INTERVAL 24 HOUR
        GROUP BY instance_uuid
        ORDER BY avg_total DESC
        LIMIT ${LIMIT}
      `),
      query(`
        SELECT
          instance_uuid,
          avg(read_bytes) AS avg_read,
          avg(write_bytes) AS avg_write,
          avg(read_bytes) + avg(write_bytes) AS avg_total
        FROM disk_metrics_hourly FINAL
        WHERE hour >= now() - INTERVAL 24 HOUR
        GROUP BY instance_uuid
        ORDER BY avg_total DESC
        LIMIT ${LIMIT}
      `),
    ]).then(([cpu, ram, net, disk]) => {
      setData({ cpu, ram, net, disk });
    }).catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  return (
    <div>
      <p className="range-label" style={{ marginBottom: 16 }}>Average over last 24 hours</p>
      {error && <div className="error">{error}</div>}
      {loading && <div className="loading">Loading...</div>}

      <div className="grid">
        <div className="panel">
          <h2>Top CPU</h2>
          <table className="top-table">
            <thead><tr><th>Instance</th><th>Avg</th><th>Max</th></tr></thead>
            <tbody>
              {data.cpu.map(r => (
                <tr key={r.instance_uuid}>
                  <td className="uuid">{r.instance_uuid}</td>
                  <td>{r.avg_cpu_pct}%</td>
                  <td>{r.max_cpu_pct}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="panel">
          <h2>Top RAM (RSS)</h2>
          <table className="top-table">
            <thead><tr><th>Instance</th><th>Avg</th><th>Max</th></tr></thead>
            <tbody>
              {data.ram.map(r => (
                <tr key={r.instance_uuid}>
                  <td className="uuid">{r.instance_uuid}</td>
                  <td>{formatBytes(r.avg_rss)}</td>
                  <td>{formatBytes(r.max_rss)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="panel">
          <h2>Top Network (avg/hour)</h2>
          <table className="top-table">
            <thead><tr><th>Instance</th><th>RX</th><th>TX</th><th>Total</th></tr></thead>
            <tbody>
              {data.net.map(r => (
                <tr key={r.instance_uuid}>
                  <td className="uuid">{r.instance_uuid}</td>
                  <td>{formatBytes(r.avg_rx)}</td>
                  <td>{formatBytes(r.avg_tx)}</td>
                  <td>{formatBytes(r.avg_total)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="panel">
          <h2>Top Disk I/O (avg/hour)</h2>
          <table className="top-table">
            <thead><tr><th>Instance</th><th>Read</th><th>Write</th><th>Total</th></tr></thead>
            <tbody>
              {data.disk.map(r => (
                <tr key={r.instance_uuid}>
                  <td className="uuid">{r.instance_uuid}</td>
                  <td>{formatBytes(r.avg_read)}</td>
                  <td>{formatBytes(r.avg_write)}</td>
                  <td>{formatBytes(r.avg_total)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
