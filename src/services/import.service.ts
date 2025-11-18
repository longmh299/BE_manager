import * as XLSX from 'xlsx';
import { PrismaClient } from '@prisma/client';
const prisma = new PrismaClient();

export async function importItemsFromBuffer(buf: Buffer) {
  const wb = XLSX.read(buf, { type: 'buffer' });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json<any>(ws, { defval: '' });

  const results: any[] = [];
  for (const r of rows) {
    const sku = String(r.sku || r.SKU || '').trim();
    const name = String(r.name || r.ten || '').trim();
    if (!sku || !name) continue;

    const unit = String(r.unit || 'pcs').trim();
    const price = String(r.price || '0').trim();
    const note = String(r.note || '').trim();

    const up = await prisma.item.upsert({
      where: { sku },
      update: { name, unit, price, note },
      create: { sku, name, unit, price, note }
    });
    results.push({ sku: up.sku, id: up.id });
  }
  return { count: results.length, items: results };
}
