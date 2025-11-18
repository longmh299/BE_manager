// src/utils/sku.ts
export function makeAcronym(name: string) {
  const base = (name || 'SP')
    .normalize('NFKD').replace(/[\u0300-\u036f]/g, '') // bỏ dấu TV
    .replace(/[^a-zA-Z0-9 ]/g, ' ')
    .trim()
    .split(/\s+/)
    .map(w => (w[0] || '').toUpperCase())
    .join('');
  return base || 'SP';
}

export function todayTag(d = new Date()) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}${m}${day}`;
}

export function buildSkuFrom(name: string, seq: number) {
  const ac = makeAcronym(name);
  return `${ac}-${todayTag()}-${String(seq).padStart(4, '0')}`;
}
