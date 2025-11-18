// src/services/stocks.service.ts
import { PrismaClient, ItemKind } from '@prisma/client';

const prisma = new PrismaClient();

/**
 * -------- LIST TỒN KHO CHI TIẾT THEO KHO (/stocks) --------
 */

export type GetStocksParams = {
  itemId?: string;
  locationId?: string;
  q?: string;
  page?: number;
  pageSize?: number;
  // Lọc theo loại hàng: PART / MACHINE (tuỳ chọn)
  kind?: ItemKind | 'PART' | 'MACHINE';
};

export async function getStocks(params: GetStocksParams) {
  const {
    itemId,
    locationId,
    q,
    page = 1,
    pageSize = 500,
    kind,
  } = params;

  const where: any = {};

  if (itemId) {
    where.itemId = itemId;
  }
  if (locationId) {
    where.locationId = locationId;
  }

  // Lọc theo từ khoá sku / name
  if (q && q.trim()) {
    const keyword = q.trim();
    where.item = {
      ...(where.item || {}),
      OR: [
        { sku: { contains: keyword, mode: 'insensitive' } },
        { name: { contains: keyword, mode: 'insensitive' } },
      ],
    };
  }

  // Lọc theo kind = PART / MACHINE
  if (kind) {
    const k =
      typeof kind === 'string'
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
        item: true,
        location: true,
      },
      orderBy: {
        item: {
          sku: 'asc',
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
 * - Đi từ bảng Item
 * - include stocks rồi cộng Qty
 * - Lọc được theo:
 *    + q: sku / tên
 *    + kind: PART / MACHINE
 * - Hỗ trợ phân trang page, pageSize
 * => Máy nào không có record Stock vẫn xuất hiện với totalQty = 0
 */

export type GetStockSummaryParams = {
  q?: string;
  kind?: ItemKind | 'PART' | 'MACHINE';
  page?: number;
  pageSize?: number;
};

export async function getStockSummaryByItem(
  params: GetStockSummaryParams = {},
) {
  const {
    q,
    kind,
    page = 1,
    pageSize = 50,
  } = params;

  const whereItem: any = {};

  // Tìm kiếm theo sku / tên
  if (q && q.trim()) {
    const keyword = q.trim();
    whereItem.OR = [
      { sku: { contains: keyword, mode: 'insensitive' } },
      { name: { contains: keyword, mode: 'insensitive' } },
    ];
  }

  // Lọc theo loại hàng: PART / MACHINE
  if (kind) {
    const k =
      typeof kind === 'string'
        ? (kind.toUpperCase() as ItemKind)
        : (kind as ItemKind);
    whereItem.kind = k;
  }

  const skip = (page - 1) * pageSize;
  const take = pageSize;

  const [items, total] = await Promise.all([
    prisma.item.findMany({
      where: whereItem,
      orderBy: { sku: 'asc' },
      skip,
      take,
      include: {
        stocks: true, // để cộng qty tất cả kho
      },
    }),
    prisma.item.count({ where: whereItem }),
  ]);

  const rows = items.map((item) => {
    const totalQty = (item.stocks || []).reduce(
      (sum, s) => sum + Number(s.qty),
      0,
    );

    return {
      itemId: item.id,
      sku: item.sku,
      name: item.name,
      unit: item.unit,
      kind: item.kind,
      totalQty,
    };
  });

  return { rows, total };
}
