import Chart from 'react-apexcharts';
import { baseOptions, isDark, toTs, formatBytes } from './common';

export default function NetworkChartA({ data }) {
  if (!data.length) return <div className="empty">No data</div>;
  const dark = isDark();
  const options = {
    ...baseOptions(dark),
    colors: ['#10b981', '#ef4444'],
    yaxis: {
      ...baseOptions(dark).yaxis,
      labels: { ...baseOptions(dark).yaxis.labels, formatter: formatBytes },
    },
    tooltip: { ...baseOptions(dark).tooltip, y: { formatter: formatBytes } },
    fill: { type: 'gradient', gradient: { shadeIntensity: 1, opacityFrom: 0.3, opacityTo: 0.05, stops: [0, 100] } },
  };
  const series = [
    { name: 'RX', data: data.map(d => [toTs(d), Number(d.rx_bytes) || 0]) },
    { name: 'TX', data: data.map(d => [toTs(d), Number(d.tx_bytes) || 0]) },
  ];
  return <Chart type="area" height={220} options={options} series={series} />;
}
