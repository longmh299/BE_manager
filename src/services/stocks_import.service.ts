// src/services/stocks_import.service.ts
import * as XLSX from "xlsx";
import { Prisma, PrismaClient, ItemKind } from "@prisma/client";

const prisma = new PrismaClient();

export type ImportMode = "replace" | "add";

function toNumber(v: any): number {
  if (v == null) return 0;
  if (typeof v === "number") return v;
  const s = String(v).trim();
  if (!s) return 0;
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

type TxLike = Prisma.TransactionClient | PrismaClient;

async function getDefaultUnitId(db: TxLike) {
  const u = await db.unit.findFirst({ where: { code: "pcs" } });
  if (!u) throw new Error("Default Unit(code='pcs') not found. Please seed Unit first.");
  return u.id;
}

/**
 * ✅ Lấy kho mặc định: kho warehouse tạo sớm nhất.
 * Nếu DB chưa có kho -> tự tạo wh-01.
 */
async function getOrCreateDefaultWarehouse(db: TxLike) {
  const wh = await db.location.findFirst({
    where: { kind: "warehouse" },
    orderBy: { createdAt: "asc" },
  });

  if (wh) return wh;

  return db.location.create({
    data: {
      code: "wh-01",
      name: "Kho mặc định",
      kind: "warehouse",
    },
  });
}

/**
 * ✅ Resolve location:
 * - Nếu file có code:
 *    - đúng -> dùng kho đó
 *    - sai -> fallback kho mặc định + warning
 * - Nếu file trống -> fallback kho mặc định
 */
async function resolveLocationId(
  db: TxLike,
  params: {
    fileLocCode?: any;
    defaultLocationId: string;
    locCache: Map<string, string>;
    warningRows: Array<{ row: number; message: string }>;
    excelRowNo: number;
  }
) {
  const { fileLocCode, defaultLocationId, locCache, warningRows, excelRowNo } = params;

  const locCode = String(fileLocCode ?? "").trim();
  if (!locCode) return defaultLocationId;

  if (locCache.has(locCode)) return locCache.get(locCode)!;

  const loc = await db.location.findFirst({ where: { code: locCode } });
  if (!loc) {
    warningRows.push({
      row: excelRowNo,
      message: `Location not found "${locCode}" -> fallback to default warehouse`,
    });
    return defaultLocationId;
  }

  locCache.set(locCode, loc.id);
  return loc.id;
}

function chunkArray<T>(arr: T[], size: number) {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function isNeonTxnError(err: any) {
  const msg = String(err?.message || "");
  return (
    msg.includes("Unable to start a transaction") ||
    msg.includes("Transaction not found") ||
    msg.includes("Transaction ID is invalid") ||
    msg.includes("old closed transaction")
  );
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function runWithRetry<T>(fn: () => Promise<T>, tries = 3) {
  let lastErr: any;
  for (let i = 0; i < tries; i++) {
    try {
      return await fn();
    } catch (e: any) {
      lastErr = e;
      if (!isNeonTxnError(e) || i === tries - 1) throw e;
      // backoff nhẹ
      await sleep(250 * (i + 1));
    }
  }
  throw lastErr;
}

/**
 * Import OPENING from Excel buffer
 * - Upsert Item theo name (vì name unique)
 * - Stock.qty = ton_dau (ưu tiên) hoặc ton_cuoi
 * - Stock.avgCost = gia_goc (nếu có)
 * - mode:
 *    - replace: set qty = qtyFile
 *    - add: qty = qtyOld + qtyFile
 *
 * ✅ Neon-safe: chunk nhỏ, tránh transaction dài.
 */
export async function importOpeningFromExcelBuffer(
  buf: Buffer,
  opts?: { mode?: ImportMode; locationId?: string; batchId?: string }
) {
  const mode: ImportMode = (opts?.mode ?? "replace") === "add" ? "add" : "replace";

  const wb = XLSX.read(buf, { type: "buffer" });
  const sheetName = wb.SheetNames[0];
  if (!sheetName) throw new Error("Empty workbook");
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

  // Columns theo file bạn
  const colName = pickCol("name", "ten", "tên");
  const colTenGoc = pickCol("ten_goc", "tengoc");
  const colSku = pickCol("sku", "skud", "code", "mahang", "mãhàng");
  const colTonDau = pickCol("ton_dau", "tondau", "tồnđầu");
  const colTonCuoi = pickCol("ton_cuoi", "toncuoi", "tồncuối", "ton");
  const colLoc = pickCol("location", "kho", "warehouse");
  const colKind = pickCol("kind", "loai", "loại");
  const colGiaGoc = pickCol("gia_goc", "giagoc", "basecost", "cost");

  const dataRows = table.slice(1);

  // ✅ preload master tối thiểu (ngoài transaction)
  const unitId = await getDefaultUnitId(prisma);
  const defaultWh = await getOrCreateDefaultWarehouse(prisma);

  // ✅ defaultLocationId:
  // - nếu opts.locationId có -> dùng luôn
  // - nếu không -> dùng kho mặc định
  const defaultLocationId = opts?.locationId ? opts.locationId : defaultWh.id;

  // cache để giảm query
  const locCache = new Map<string, string>();
  const itemCacheByName = new Map<string, { id: string; name: string; sku: string }>();

  const warningRows: Array<{ row: number; message: string }> = [];
  let createdItems = 0;
  let updatedItems = 0;
  let affectedStocks = 0;

  // ✅ chunk nhỏ để tránh giữ transaction lâu (Neon-friendly)
  const CHUNK_SIZE = 20;
  const chunks = chunkArray(dataRows, CHUNK_SIZE);

  for (let ci = 0; ci < chunks.length; ci++) {
    const part = chunks[ci];

    await runWithRetry(async () => {
      return prisma.$transaction(
        async (tx) => {
          for (let j = 0; j < part.length; j++) {
            const iGlobal = ci * CHUNK_SIZE + j;
            const excelRowNo = iGlobal + 2;
            const row = part[j] || [];

            const name = colName >= 0 ? String(row[colName] ?? "").trim() : "";
            const tenGoc = colTenGoc >= 0 ? String(row[colTenGoc] ?? "").trim() : "";
            const sku = colSku >= 0 ? String(row[colSku] ?? "").trim() : "";

            const keyName = (tenGoc || name || sku).trim();
            if (!keyName) continue;

            const kind = colKind >= 0 ? parseKind(row[colKind]) : "PART";

            // qty: ưu tiên ton_dau, fallback ton_cuoi
            const qty =
              colTonDau >= 0 && String(row[colTonDau] ?? "").trim() !== ""
                ? toNumber(row[colTonDau])
                : colTonCuoi >= 0
                ? toNumber(row[colTonCuoi])
                : 0;

            const baseCost = colGiaGoc >= 0 ? toNumber(row[colGiaGoc]) : 0;

            // ✅ location: nếu sai -> fallback về kho mặc định + warning
            const locationId = await resolveLocationId(tx, {
              fileLocCode: colLoc >= 0 ? row[colLoc] : "",
              defaultLocationId,
              locCache,
              warningRows,
              excelRowNo,
            });

            // ==== upsert item theo NAME (name unique) ====
            let item = itemCacheByName.get(keyName);
            if (!item) {
              const existed = await tx.item.findUnique({
                where: { name: keyName },
                select: { id: true, name: true, sku: true },
              });

              if (existed) {
                const up = await tx.item.update({
                  where: { id: existed.id },
                  data: {
                    sku: sku || existed.sku, // sku không unique => chỉ update nếu có
                    unitId,
                    kind,
                    // ❗ price (giá vốn) không import vào Item
                  },
                  select: { id: true, name: true, sku: true },
                });
                item = up;
                updatedItems++;
              } else {
                const created = await tx.item.create({
                  data: {
                    sku: sku || keyName,
                    name: keyName,
                    unitId,
                    kind,
                    price: new Prisma.Decimal(0),
                    sellPrice: new Prisma.Decimal(0),
                  },
                  select: { id: true, name: true, sku: true },
                });
                item = created;
                createdItems++;
              }

              itemCacheByName.set(keyName, item);
            }

            // ==== upsert stock ====
            const deltaQty = new Prisma.Decimal(qty);

            if (mode === "replace") {
              await tx.stock.upsert({
                where: { itemId_locationId: { itemId: item.id, locationId } },
                create: {
                  itemId: item.id,
                  locationId,
                  qty: deltaQty,
                  avgCost: new Prisma.Decimal(baseCost || 0),
                },
                update: {
                  qty: deltaQty,
                  ...(baseCost > 0 ? { avgCost: new Prisma.Decimal(baseCost) } : {}),
                },
              });
            } else {
              await tx.stock.upsert({
                where: { itemId_locationId: { itemId: item.id, locationId } },
                create: {
                  itemId: item.id,
                  locationId,
                  qty: deltaQty,
                  avgCost: new Prisma.Decimal(baseCost || 0),
                },
                update: {
                  qty: { increment: deltaQty },
                  ...(baseCost > 0 ? { avgCost: new Prisma.Decimal(baseCost) } : {}),
                },
              });
            }

            affectedStocks++;

            if (qty === 0) {
              warningRows.push({
                row: excelRowNo,
                message: `Qty = 0 for "${keyName}" (still imported item)`,
              });
            }
          }
        },
        // ✅ tăng chút maxWait/timeout cho Neon, nhưng transaction vẫn ngắn vì chunk nhỏ
        { maxWait: 10000, timeout: 20000 }
      );
    }, 3);
  }

  return {
    ok: true,
    summary: {
      mode,
      createdItems,
      updatedItems,
      affectedStocks,
      warnings: warningRows.length,
      defaultWarehouse: { id: defaultWh.id, code: defaultWh.code, name: defaultWh.name },
    },
    warningRows,
    batchId: opts?.batchId ?? null,
  };
}
