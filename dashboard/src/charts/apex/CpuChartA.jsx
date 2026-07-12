import Chart from 'react-apexcharts';
import { baseOptions, isDark, toTs } from './common';

export default function CpuChartA({ data }) {
  if (!data.length) return <div className="empty">No data</div>;
  const dark = isDark();
  const options = {
    ...baseOptions(dark),
    colors: ['#3b82f6'],
    yaxis: {
      ...baseOptions(dark).yaxis,
      min: 0, max: 100,
      labels: { ...baseOptions(dark).yaxis.labels, formatter: v => `${v.toFixed(0)}%` },
    },
    tooltip: { ...baseOptions(dark).tooltip, y: { formatter: v => `${v.toFixed(1)}%` } },
    fill: { type: 'gradient', gradient: { shadeIntensity: 1, opacityFrom: 0.3, opacityTo: 0.05, stops: [0, 100] } },
  };
  const series = [{ name: 'CPU', data: data.map(d => [toTs(d), Number(d.cpu_pct)]) }];
  return <Chart type="area" height={220} options={options} series={series} />;
}
