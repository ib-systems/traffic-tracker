import { formatBytes, toEpoch } from '../format';

export function baseOptions(isDark) {
  return {
    chart: {
      background: 'transparent',
      toolbar: { show: true, tools: { download: false, selection: true, zoom: true, zoomin: true, zoomout: true, pan: true, reset: true } },
      animations: { enabled: true, easing: 'easeinout', speed: 400 },
      zoom: { allowMouseWheelZoom: false },
      fontFamily: 'system-ui, sans-serif',
    },
    theme: { mode: isDark ? 'dark' : 'light' },
    grid: {
      borderColor: isDark ? '#374151' : '#e5e7eb',
      strokeDashArray: 3,
    },
    xaxis: {
      type: 'datetime',
      labels: { style: { fontSize: '11px', colors: isDark ? '#9ca3af' : '#4b5563' } },
    },
    yaxis: {
      labels: { style: { fontSize: '11px', colors: isDark ? '#9ca3af' : '#4b5563' } },
    },
    dataLabels: { enabled: false },
    markers: { size: 0 },
    tooltip: {
      theme: isDark ? 'dark' : 'light',
      x: { format: 'HH:mm dd MMM' },
    },
    stroke: { width: 2, curve: 'smooth' },
    legend: {
      fontSize: '12px',
      labels: { colors: isDark ? '#9ca3af' : '#4b5563' },
    },
  };
}

export function isDark() {
  return window.matchMedia('(prefers-color-scheme: dark)').matches;
}

export function toTs(d) {
  return toEpoch(d.minute);
}

export { formatBytes };
