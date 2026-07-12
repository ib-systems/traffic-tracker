import Chart from 'react-apexcharts';
import { baseOptions, isDark, toTs, formatBytes } from './common';

export default function RamChartA({ data }) {
  if (!data.length) return <div className="empty">No data</div>;
  const dark = isDark();
  const options = {
    ...baseOptions(dark),
    colors: ['#8b5cf6', '#06b6d4'],
    yaxis: {
      ...baseOptions(dark).yaxis,
      min: 0,
      labels: { ...baseOptions(dark).yaxis.labels, formatter: formatBytes },
    },
    tooltip: { ...baseOptions(dark).tooltip, y: { formatter: formatBytes } },
    fill: { type: 'gradient', gradient: { shadeIntensity: 1, opacityFrom: 0.3, opacityTo: 0.05, stops: [0, 100] } },
  };
  const series = [
    { name: 'Allocated', data: data.map(d => [toTs(d), Number(d.memory_actual_avg_bytes) || 0]) },
    { name: 'RSS', data: data.map(d => [toTs(d), Number(d.memory_rss_avg_bytes) || 0]) },
  ];
  return <Chart type="area" height={220} options={options} series={series} />;
}
