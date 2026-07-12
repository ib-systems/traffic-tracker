import Chart from 'react-apexcharts';
import { baseOptions, isDark, toTs, formatBytes } from './common';

export default function DiskChartA({ data }) {
  if (!data.length) return <div className="empty">No data</div>;
  const dark = isDark();

  const byMinute = {};
  for (const d of data) {
    const ts = toTs(d);
    if (!byMinute[ts]) byMinute[ts] = { ts, read: 0, write: 0 };
    byMinute[ts].read += Number(d.read_bytes) || 0;
    byMinute[ts].write += Number(d.write_bytes) || 0;
  }
  const sorted = Object.values(byMinute).sort((a, b) => a.ts - b.ts);

  const options = {
    ...baseOptions(dark),
    chart: { ...baseOptions(dark).chart, stacked: true },
    colors: ['#10b981', '#f59e0b'],
    yaxis: {
      ...baseOptions(dark).yaxis,
      labels: { ...baseOptions(dark).yaxis.labels, formatter: formatBytes },
    },
    tooltip: { ...baseOptions(dark).tooltip, shared: true, intersect: false, enabledOnSeries: [0, 1], y: { formatter: formatBytes } },
    states: { hover: { filter: { type: 'darken', value: 0.8 } }, active: { filter: { type: 'none' } } },
    xaxis: { ...baseOptions(dark).xaxis, tooltip: { enabled: false }, crosshairs: { show: false } },
    plotOptions: { bar: { columnWidth: '60%', borderRadius: 2 } },
    stroke: { width: 0 },
  };
  const series = [
    { name: 'Read', data: sorted.map(d => [d.ts, d.read]) },
    { name: 'Write', data: sorted.map(d => [d.ts, d.write]) },
  ];
  return <Chart type="bar" height={220} options={options} series={series} />;
}
