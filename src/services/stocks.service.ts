// src/services/stocks.service.ts
import { PrismaClient, ItemKind, Prisma } from "@prisma/client";

const prisma = new PrismaClient();

function toNum(d: Prisma.Decimal | number | string | null | undefined): number {
  if (d == null) return 0;
  if (typeof d === "number") return d;
  return Number(d.toString());
}

// ✅ Bỏ dấu tiếng Việt — Postgres "contains ... insensitive" chỉ bỏ qua hoa/thường,
// KHÔNG bỏ dấu, nên gõ "may ep" sẽ không tìm ra "Máy ép" nếu lọc thẳng bằng SQL.
// Dùng để lọc lại trong JS sau khi lấy danh sách item theo `kind`.
const VN_DIACRITIC_MAP: Record<string, string> = {
  à: "a", á: "a", ạ: "a", ả: "a", ã: "a", â: "a", ầ: "a", ấ: "a", ậ: "a", ẩ: "a", ẫ: "a",
  ă: "a", ằ: "a", ắ: "a", ặ: "a", ẳ: "a", ẵ: "a",
  è: "e", é: "e", ẹ: "e", ẻ: "e", ẽ: "e", ê: "e", ề: "e", ế: "e", ệ: "e", ể: "e", ễ: "e",
  ì: "i", í: "i", ị: "i", ỉ: "i", ĩ: "i",
  ò: "o", ó: "o", ọ: "o", ỏ: "o", õ: "o", ô: "o", ồ: "o", ố: "o", ộ: "o", ổ: "o", ỗ: "o",
  ơ: "o", ờ: "o", ớ: "o", ợ: "o", ở: "o", ỡ: "o",
  ù: "u", ú: "u", ụ: "u", ủ: "u", ũ: "u", ư: "u", ừ: "u", ứ: "u", ự: "u", ử: "u", ữ: "u",
  ỳ: "y", ý: "y", ỵ: "y", ỷ: "y", ỹ: "y",
  đ: "d",
};

function normalizeVN(raw: string): string {
  const lower = (raw || "").trim().toLowerCase();
  const noDiacritic = lower.replace(/[à-ỹđ]/g, (c) => VN_DIACRITIC_MAP[c] || c);
  return noDiacritic.replace(/\s+/g, " ").trim();
}

/**
 * -------- LIST TỒN KHO CHI TIẾT THEO KHO (/stocks) --------
 */
export type GetStocksParams = {
  itemId?: string;
  locationId?: string;
  q?: string;
  page?: number;
  pageSize?: number;
  kind?: ItemKind | "PART" | "MACHINE";
};

export async function getStocks(params: GetStocksParams) {
  const { itemId, locationId, q, page = 1, pageSize = 500, kind } = params;

  const where: any = {};

  if (itemId) where.itemId = itemId;
  if (locationId) where.locationId = locationId;

  if (q && q.trim()) {
    const keyword = q.trim();
    where.item = {
      ...(where.item || {}),
      OR: [
        { sku: { contains: keyword, mode: "insensitive" } },
        { name: { contains: keyword, mode: "insensitive" } },
      ],
    };
  }

  if (kind) {
    const k =
      typeof kind === "string"
        ? (kind.toUpperCase() as ItemKind)
        : (kind as ItemKind);
    where.item = {
      ...(where.item || {}),
      kind: k,
    };
  }

  const skip = (page - 1) * pageSize;
  const take = pageSize;

  const [rows, total] = await Promise.all([
    prisma.stock.findMany({
      where,
      include: {
        item: {
          include: {
            unit: true, // ✅ NEW: lấy unit relation
          },
        },
        location: true,
      },
      orderBy: {
        item: {
          sku: "asc",
        },
      },
      skip,
      take,
    }),
    prisma.stock.count({ where }),
  ]);

  return { rows, total };
}

/**
 * -------- TỔNG HỢP TỒN THEO ITEM (/stocks/summary-by-item) --------
 *
 * - totalQty > 0: avgCost = sum(qty*avgCost)/sum(qty)
 * - totalQty = 0: lấy "avgCost gần nhất", ưu tiên avgCost != 0 để tránh pick nhầm kho mới (avgCost=0)
 */
export type GetStockSummaryParams = {
  q?: string;
  kind?: ItemKind | "PART" | "MACHINE";
  page?: number;
  pageSize?: number;
};

export async function getStockSummaryByItem(params: GetStockSummaryParams = {}) {
  const { q, kind, page = 1, pageSize = 50 } = params;

  const whereItem: any = {};

  if (kind) {
    const k =
      typeof kind === "string"
        ? (kind.toUpperCase() as ItemKind)
        : (kind as ItemKind);
    whereItem.kind = k;
  }

  if (q && q.trim()) {
    // ✅ FIX: trước đây dùng `contains ... insensitive` của Postgres — chỉ bỏ
    // qua hoa/thường, KHÔNG bỏ dấu tiếng Việt, nên gõ không dấu sẽ ra rỗng dù
    // sản phẩm có tồn thật. Lấy trước danh sách theo `kind`, lọc lại bằng
    // normalizeVN (bỏ dấu, tách từ, không cần đúng thứ tự) rồi mới phân trang.
    const qTokens = normalizeVN(q).split(" ").filter(Boolean);

    const candidates = await prisma.item.findMany({
      where: kind ? { kind: whereItem.kind } : {},
      select: { id: true, sku: true, name: true },
    });

    const matchedIds = candidates
      .filter((it) => {
        const hay = normalizeVN(`${it.sku || ""} ${it.name || ""}`);
        return qTokens.every((t) => hay.includes(t));
      })
      .map((it) => it.id);

    whereItem.id = { in: matchedIds }; // rỗng -> Prisma trả 0 dòng, không lỗi
  }

  const skip = (page - 1) * pageSize;
  const take = pageSize;

  const [items, total] = await Promise.all([
    prisma.item.findMany({
      where: whereItem,
      orderBy: { sku: "asc" },
      skip,
      take,
      include: {
        unit: true,
        stocks: { select: { qty: true, avgCost: true, updatedAt: true } },
      },
    }),
    prisma.item.count({ where: whereItem }),
  ]);

  const rows = items.map((item) => {
    const stocks = item.stocks || [];

    const totalQty = stocks.reduce((sum, s) => sum + toNum(s.qty as any), 0);

    // giá trị tồn theo từng kho: sum(qty * avgCost)
    const totalValue = stocks.reduce((sum, s) => {
      const qty = toNum(s.qty as any);
      const avg = toNum(s.avgCost as any);
      return sum + qty * avg;
    }, 0);

    let avgCost = 0;

    if (totalQty > 0) {
      avgCost = totalValue / totalQty;
    } else {
      // totalQty = 0: ưu tiên stock row có avgCost != 0
      const candidates =
        stocks.filter((s) => toNum((s as any).avgCost) > 0).length > 0
          ? stocks.filter((s) => toNum((s as any).avgCost) > 0)
          : stocks;

      if (candidates.length > 0) {
        const latest = candidates.reduce((best, cur) => {
          const tb = best?.updatedAt ? new Date(best.updatedAt).getTime() : 0;
          const tc = cur?.updatedAt ? new Date(cur.updatedAt).getTime() : 0;
          return tc > tb ? cur : best;
        }, candidates[0]);

        avgCost = toNum((latest as any).avgCost);
      } else {
        avgCost = 0;
      }
    }

    return {
      itemId: item.id,
      sku: item.sku,
      note: item.note ?? "",
      name: item.name,
      unit: item.unit?.code ?? "pcs",
      unitName: item.unit?.name ?? "Cái",
      kind: item.kind,
      sellPrice: item.sellPrice,
      totalQty,

      avgCost, // giá vốn TB (hoặc giá vốn gần nhất nếu qty=0)
      stockValue: totalValue, // giá trị tồn
    };
  });

  return { rows, total };
}