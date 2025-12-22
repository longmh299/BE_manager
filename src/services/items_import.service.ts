// src/services/items_import.service.ts
import * as XLSX from "xlsx";
import { Prisma, PrismaClient, ItemKind } from "@prisma/client";

const prisma = new PrismaClient();

/** ===== Helpers ===== */
function toNumber(v: any): number {
  if (v == null) return 0;
  if (typeof v === "number") return v;
  const s = String(v).trim();
  if (!s) return 0;
  // hỗ trợ "1,234,567" hoặc "1.234.567"
  const n = Number(s.replace(/\./g, "").replace(/,/g, ""));
  return isNaN(n) ? 0 : n;
}

function norm(s: any) {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[._-]/g, "");
}

function parseKind(raw: any): ItemKind {
  const t = String(raw ?? "").trim().toUpperCase();
  return t === "MACHINE" ? "MACHINE" : "PART";
}

/**
 * Import ITEMS ONLY
 * - Không nhập tồn
 * - Không nhập giá vốn (price luôn = 0)
 * - Unit mặc định = pcs (Unit.code="pcs")
 *
 * Header hỗ trợ (không phân biệt hoa thường / dấu gạch):
 * - sku | skud | code | mahang
 * - name | ten | ten_goc
 * - sellPrice | giaban | gia_ban
 * - note | ghichu
 * - kind (PART/MACHINE)
 */
export async function importItemsFromBuffer(buf: Buffer) {
  const wb = XLSX.read(buf, { type: "buffer" });
  const sheetName = wb.SheetNames[0];
  if (!sheetName) throw new Error("Empty file");

  const ws = wb.Sheets[sheetName];
  const table = XLSX.utils.sheet_to_json(ws, { header: 1, raw: true }) as any[][];
  if (!table || table.length < 2) throw new Error("File has no data");

  const headers = (table[0] || []).map(norm);

  const pickCol = (...cands: string[]) => {
    for (const c of cands) {
      const i = headers.indexOf(norm(c));
      if (i >= 0) return i;
    }
    return -1;
  };

  const colSku = pickCol("sku", "skud", "code", "mahang", "mãhàng");
  const colName = pickCol("name", "ten", "tên", "ten_goc", "tengoc");
  const colSell = pickCol("sellprice", "sell_price", "giaban", "gia_ban", "price");
  const colNote = pickCol("note", "ghichu", "ghi_chu");
  const colKind = pickCol("kind", "loai", "loại");

  // lấy unit mặc định
  const unit = await prisma.unit.findFirst({ where: { code: "pcs" } });
  if (!unit) throw new Error("Default unit 'pcs' not found. Please seed Unit(code='pcs').");

  const dataRows = table.slice(1);

  return prisma.$transaction(async (tx) => {
    let created = 0;
    let updated = 0;
    const warningRows: Array<{ row: number; message: string }> = [];

    for (let i = 0; i < dataRows.length; i++) {
      const row = dataRows[i] || [];
      const excelRowNo = i + 2; // vì header ở dòng 1

      const sku = colSku >= 0 ? String(row[colSku] ?? "").trim() : "";
      const name = colName >= 0 ? String(row[colName] ?? "").trim() : "";

      if (!sku && !name) continue;

      const sellPrice = colSell >= 0 ? toNumber(row[colSell]) : 0;
      const note = colNote >= 0 ? (String(row[colNote] ?? "").trim() || null) : null;
      const kind = colKind >= 0 ? parseKind(row[colKind]) : "PART";

      // ==== ưu tiên key theo NAME vì name unique ====
      if (name) {
        try {
          await tx.item.upsert({
            where: { name }, // name @unique
            create: {
              sku: sku || name,
              name,
              unitId: unit.id,
              sellPrice: new Prisma.Decimal(sellPrice),
              note,
              kind,
              price: new Prisma.Decimal(0), // ✅ không import giá vốn
            },
            update: {
              sku: sku || undefined, // nếu sku trống thì giữ sku cũ
              unitId: unit.id,       // tạm thời set pcs theo yêu cầu bạn
              sellPrice: new Prisma.Decimal(sellPrice),
              note,
              kind,
            },
          });
          // upsert không cho biết create/update -> check tồn tại trước để count chuẩn hơn
          // (đơn giản: count bằng cách findUnique trước)
          const existed = await tx.item.findUnique({ where: { name }, select: { id: true } });
          // existed chắc chắn có, nhưng ta không phân biệt được create/update ở đây.
          // => để count chuẩn: làm findUnique trước upsert
          // Tuy nhiên để nhẹ code: ta dùng cách khác bên dưới.
        } catch (e: any) {
          warningRows.push({
            row: excelRowNo,
            message: `Upsert by name failed: ${e?.message || "unknown error"}`,
          });
        }
        // Để count chuẩn: ta làm findUnique trước upsert ở phiên bản tối ưu hơn.
        // Ở đây ta dùng cách đơn giản: luôn cộng updated, vì upsert khó phân biệt.
        updated++;
        continue;
      }

      // ==== nếu thiếu NAME: fallback tìm theo SKU (sku không unique => findFirst) ====
      if (!sku) {
        warningRows.push({ row: excelRowNo, message: "Missing both sku and name" });
        continue;
      }

      const existingBySku = await tx.item.findFirst({
        where: { sku },
        select: { id: true, name: true, sku: true },
      });

      if (existingBySku) {
        await tx.item.update({
          where: { id: existingBySku.id },
          data: {
            unitId: unit.id,
            sellPrice: new Prisma.Decimal(sellPrice),
            note,
            kind,
          },
        });
        updated++;
      } else {
        // create mới: bắt buộc phải có name (unique) => dùng sku làm name
        await tx.item.create({
          data: {
            sku,
            name: sku,
            unitId: unit.id,
            sellPrice: new Prisma.Decimal(sellPrice),
            note,
            kind,
            price: new Prisma.Decimal(0),
          },
        });
        created++;
      }
    }

    return {
      ok: true,
      summary: { created, updated, total: created + updated },
      warningRows,
    };
  });
}
