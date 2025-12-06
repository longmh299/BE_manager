// src/services/stocks_import.service.ts
import { PrismaClient, ItemKind } from "@prisma/client";
import * as XLSX from "xlsx";

const prisma = new PrismaClient();

// ---------- helpers ----------
function toNumber(v: any): number {
  if (v == null) return 0;
  if (typeof v === "number") return v;
  const s = String(v).trim();
  if (!s) return 0;
  const n = Number(s.replace(/\./g, "").replace(/,/g, ""));
  return isNaN(n) ? 0 : n;
}

function norm(s: string) {
  return s
    ?.toString()
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[._-]/g, "");
}

/**
 * Convert text kind trong file → enum ItemKind
 */
function parseItemKind(raw?: string): ItemKind {
  const t = (raw || "").toString().trim().toLowerCase();

  if (!t) return "PART";

  if (
    t === "machine" ||
    t === "máy" ||
    t === "may" ||
    t === "mm" ||
    t === "maymoc" ||
    t === "máymóc"
  ) {
    return "MACHINE";
  }

  // Cho phép chỉ cần bắt đầu bằng "m" là coi là MACHINE
  if (t.startsWith("m")) return "MACHINE";

  return "PART";
}

/**
 * Khi không có cột kind, suy luận theo tên:
 * tên bắt đầu bằng "máy", "may", hoặc chứa "machine" → MACHINE
 */
function inferKindFromName(name?: string): ItemKind {
  const t = (name || "").toString().trim().toLowerCase();
  if (!t) return "PART";

  if (
    t.startsWith("máy") ||
    t.startsWith("may ") ||
    t.startsWith("may-") ||
    t.startsWith("may_") ||
    t.includes("machine")
  ) {
    return "MACHINE";
  }

  return "PART";
}

/** Dùng enum để suy ra prefix cho SKU (LK / MM) */
function kindToPrefixFromEnum(kind: ItemKind): "LK" | "MM" {
  return kind === "MACHINE" ? "MM" : "LK";
}

function toSlugBase(s: string) {
  // 40 ký tự để hạn chế trùng slug
  return (
    (s || "SP")
      .normalize("NFD")
      .replace(/\p{Diacritic}/gu, "")
      .replace(/[^a-zA-Z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .toUpperCase()
      .slice(0, 40) || "SP"
  );
}

async function ensureUniqueSkuFromName(
  name: string,
  prefix: "LK" | "MM"
): Promise<string> {
  const base = `${prefix}-${toSlugBase(name)}`;
  let n = 1;
  while (true) {
    const sku = n === 1 ? base : `${base}-${n}`;
    const found = await prisma.item.findUnique({ where: { sku } });
    if (!found) return sku;
    n++;
  }
}

async function getSingleWarehouseId(): Promise<string> {
  const wh = await prisma.location.findFirst({
    where: { kind: "warehouse" },
    orderBy: { createdAt: "asc" },
  });
  if (!wh) throw new Error("No warehouse Location found.");
  return wh.id;
}

// ---------- JSON rows importer (/opening) ----------
export async function importOpeningStocks(_p: PrismaClient, body: any) {
  const modeRaw = (body?.mode ?? "set").toString().toLowerCase();
  const mode: "replace" | "add" = modeRaw === "adjust" ? "add" : "replace";
  const rows = Array.isArray(body?.rows) ? body.rows : null;
  if (!rows?.length) throw new Error("rows must be a non-empty array");

  let locationId: string;
  const code = (body?.warehouseCode ?? "").toString().trim();
  if (code) {
    const found = await prisma.location.findFirst({ where: { code } });
    if (!found) throw new Error(`Warehouse not found: ${code}`);
    locationId = found.id;
  } else {
    locationId = await getSingleWarehouseId();
  }

  let affectedStocks = 0;
  for (const r of rows) {
    const sku = (r?.sku ?? "").toString().trim();
    const qty = toNumber(r?.qty);
    if (!sku) continue;

    const item = await prisma.item.findUnique({ where: { sku } });
    if (!item) continue;

    const key = { itemId: item.id, locationId };
    const old = await prisma.stock.findUnique({
      where: { itemId_locationId: key },
    });

    if (mode === "replace") {
      if (old) {
        await prisma.stock.update({
          where: { itemId_locationId: key },
          data: { qty: qty as any },
        });
      } else {
        await prisma.stock.create({ data: { ...key, qty: qty as any } });
      }
    } else {
      if (old) {
        await prisma.stock.update({
          where: { itemId_locationId: key },
          data: { qty: Number(old.qty) + qty as any },
        });
      } else {
        await prisma.stock.create({ data: { ...key, qty: qty as any } });
      }
    }
    affectedStocks++;
  }

  return { ok: true, summary: { affectedStocks, mode } };
}

// ---------- ONE-FILE importer (/opening-onefile) ----------
export async function importOpeningOneFile(
  buf: Buffer,
  opts: { mode: "replace" | "add" } = { mode: "replace" }
) {
  const wb = XLSX.read(buf, { type: "buffer" });
  const sheetName = wb.SheetNames[0];
  if (!sheetName) throw new Error("Empty workbook");
  const ws = wb.Sheets[sheetName];

  const headers = (XLSX.utils.sheet_to_json(ws, {
    header: 1,
    raw: true,
  })[0] || []) as string[];
  if (!headers.length) throw new Error("Missing header row");

  const idx: Record<string, number> = {};
  headers.forEach((h, i) => (idx[norm(h)] = i));

  const colSku =
    idx["sku"] ?? idx["skud"] ?? idx["mahang"] ?? idx["mahàng"] ?? idx["code"];
  const colName =
    idx["name"] ??
    idx["tenhang"] ??
    idx["tênhàng"] ??
    idx["ten"] ??
    idx["tên"];
  const colQty =
    idx["qty"] ??
    idx["ton"] ??
    idx["tồn"] ??
    idx["tondau"] ??
    idx["tồnđầu"] ??
    idx["toncuoi"] ??
    idx["tồncuối"];
  const colSell = idx["sellprice"] ?? idx["giaban"] ?? idx["gia"] ?? idx["price"];
  const colNote = idx["note"] ?? idx["ghichu"] ?? idx["ghichú"];
  const colKind = idx["kind"] ?? idx["loai"] ?? idx["loại"];

  if (colQty === undefined) {
    throw new Error(
      "Header must contain quantity column (qty/ton/tonDau/tonCuoi)"
    );
  }

  const whId = await getSingleWarehouseId();
  const rows = XLSX.utils.sheet_to_json(ws, {
    header: 1,
    raw: true,
  }) as any[][];

  let createdItems = 0;
  let updatedItems = 0; // hiện tại sẽ luôn = 0 cho PART & MACHINE
  let affectedStocks = 0;

  const hasKindColumn = colKind !== undefined;

  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];

    const rawSku = colSku !== undefined ? row[colSku] ?? "" : "";
    let sku = String(rawSku || "").trim();

    const name =
      colName !== undefined ? String(row[colName] ?? "").trim() : "";
    const qty = toNumber(row[colQty]);
    const sell = colSell !== undefined ? toNumber(row[colSell]) : 0;
    const note =
      colNote !== undefined
        ? (String(row[colNote] ?? "").trim() || undefined)
        : undefined;

    // ---- Xác định kind ----
    let kindEnum: ItemKind;
    if (hasKindColumn) {
      const rawKind = String(row[colKind] ?? "");
      if (rawKind && rawKind.toString().trim() !== "") {
        kindEnum = parseItemKind(rawKind);
      } else {
        kindEnum = inferKindFromName(name);
      }
    } else {
      kindEnum = inferKindFromName(name);
    }

    const prefix = kindToPrefixFromEnum(kindEnum);

    if (!sku && !name) continue;

    // ====== CẢ PART & MACHINE: 1 dòng Excel = 1 Item, luôn gen SKU unique ======
    let baseSku = sku;

    if (kindEnum === "PART") {
      // PART: ưu tiên gắn thêm slug tên vào sau sku
      if (name) {
        const slug = toSlugBase(name);
        if (baseSku) {
          baseSku = `${baseSku}-${slug}`;
        } else {
          baseSku = `${prefix}-${slug}`; // LK-...
        }
      }
    } else {
      // MACHINE: giữ mã máy làm gốc, nếu không có thì gen từ name
      if (!baseSku && name) {
        const slug = toSlugBase(name);
        baseSku = `${prefix}-${slug}`; // MM-...
      }
    }

    if (!baseSku) {
      // fallback cuối
      baseSku = await ensureUniqueSkuFromName(name || "SP", prefix);
    }

    // Đảm bảo SKU duy nhất: base, nếu trùng -> base-2, base-3, ...
    let finalSku = baseSku;
    let counter = 1;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const exists = await prisma.item.findUnique({
        where: { sku: finalSku },
      });
      if (!exists) break;
      counter++;
      finalSku = `${baseSku}-${counter}`;
    }

    const item = await prisma.item.create({
      data: {
        sku: finalSku,
        name: name || finalSku,
        unit: "pcs",
        price: 0 as any,
        sellPrice: sell as any,
        note: note,
        kind: kindEnum,
      } as any,
    });
    createdItems++;

    const key = { itemId: item.id, locationId: whId };
    const old = await prisma.stock.findUnique({
      where: { itemId_locationId: key },
    });

    if (opts.mode === "replace") {
      if (old) {
        await prisma.stock.update({
          where: { itemId_locationId: key },
          data: { qty: qty as any },
        });
      } else {
        await prisma.stock.create({
          data: { ...key, qty: qty as any },
        });
      }
    } else {
      if (old) {
        await prisma.stock.update({
          where: { itemId_locationId: key },
          data: { qty: Number(old.qty) + qty as any },
        });
      } else {
        await prisma.stock.create({
          data: { ...key, qty: qty as any },
        });
      }
    }

    affectedStocks++;
  }

  return {
    ok: true,
    summary: {
      createdItems,
      updatedItems,
      affectedStocks,
      mode: opts.mode,
    },
  };
}
