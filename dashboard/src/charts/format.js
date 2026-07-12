export function toEpoch(v) {
  const d = new Date(v.replace(' ', 'T') + 'Z');
  return d.getTime();
}

export function formatTick(ts) {
  const d = new Date(ts);
  return `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`;
}

export function formatTooltipLabel(ts) {
  return new Date(ts).toUTCString();
}

export function formatBytes(v) {
  if (v == null || v === 0) return '0';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  let n = Number(v);
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return `${n.toFixed(2)} ${units[i]}`;
}
