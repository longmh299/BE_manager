// src/services/stocks.service.ts
import { PrismaClient, ItemKind, Prisma } from "@prisma/client";

const prisma = new PrismaClient();

function toNum(d: Prisma.Decimal | number | string | null | undefined): number {
  if (d == null) return 0;
  if (typeof d === "number") return d;
  return Number(d.toString());
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
      typeof kind === "string" ? (kind.toUpperCase() as ItemKind) : (kind as ItemKind);
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
 * ✅ FIX LỖ HỔNG:
 * - Nếu totalQty = 0 thì KHÔNG trả avgCost=0 nữa.
 * - Thay vào đó lấy "avgCost gần nhất" từ stock (updatedAt mới nhất).
 * - Vì avgCost là master-cost theo tồn/movement, hết hàng vẫn nên giữ giá vốn gần nhất để dashboard không bị 0.
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

  if (q && q.trim()) {
    const keyword = q.trim();
    whereItem.OR = [
      { sku: { contains: keyword, mode: "insensitive" } },
      { name: { contains: keyword, mode: "insensitive" } },
    ];
  }

  if (kind) {
    const k =
      typeof kind === "string" ? (kind.toUpperCase() as ItemKind) : (kind as ItemKind);
    whereItem.kind = k;
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
        // ✅ lấy cả qty + avgCost + updatedAt để tính giá vốn TB + giá trị tồn + fallback giá vốn khi qty=0
        stocks: { select: { qty: true, avgCost: true, updatedAt: true } },
      },
    }),
    prisma.item.count({ where: whereItem }),
  ]);

  const rows = items.map((item) => {
    const stocks = item.stocks || [];

    const totalQty = stocks.reduce((sum, s) => sum + toNum(s.qty as any), 0);

    // ✅ bình quân gia quyền theo tồn: sum(qty*avgCost)/sum(qty)
    const totalValue = stocks.reduce((sum, s) => {
      const qty = toNum(s.qty as any);
      const avg = toNum(s.avgCost as any);
      return sum + qty * avg;
    }, 0);

    // ✅ FIX: nếu hết hàng (totalQty=0) thì lấy avgCost gần nhất theo updatedAt
    let avgCost = 0;
    if (totalQty > 0) {
      avgCost = totalValue / totalQty;
    } else {
      if (stocks.length > 0) {
        const latest = [...stocks].sort((a, b) => {
          const ta = a.updatedAt ? new Date(a.updatedAt).getTime() : 0;
          const tb = b.updatedAt ? new Date(b.updatedAt).getTime() : 0;
          return tb - ta;
        })[0];
        avgCost = toNum((latest as any).avgCost);
      } else {
        avgCost = 0;
      }
    }

    return {
      itemId: item.id,
      sku: item.sku,
      name: item.name,
      unit: item.unit?.code ?? "pcs",
      unitName: item.unit?.name ?? "Cái",
      kind: item.kind,
      sellPrice: item.sellPrice,
      totalQty,

      // ✅ NEW cho FE admin
      avgCost, // giá vốn TB (hoặc giá vốn gần nhất nếu qty=0)
      stockValue: totalValue, // giá trị tồn (qty=0 => 0 là đúng)
    };
  });

  return { rows, total };
}
