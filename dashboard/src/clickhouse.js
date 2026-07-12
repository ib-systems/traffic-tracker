const CH_URL = '/ch';

export async function query(sql) {
  const res = await fetch(CH_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'text/plain' },
    body: sql + ' FORMAT JSON',
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`ClickHouse error: ${text}`);
  }
  const json = await res.json();
  return json.data;
}

