// src/services/stockcounts.service.ts
import { PrismaClient, Prisma, MovementType } from "@prisma/client";

const prisma = new PrismaClient();

/** Chuẩn hoá số lượng về Decimal (nhận string/number/Decimal). */
function toDecimal(n: string | number | Prisma.Decimal): Prisma.Decimal {
  if (n instanceof Prisma.Decimal) return n;
  if (typeof n === "number") return new Prisma.Decimal(n);
  return new Prisma.Decimal((n ?? "0").toString().trim());
}

// ================== LIST STOCK COUNTS ==================

export type ListStockCountsParams = {
  locationId?: string;
  status?: string; // "draft" | "posted"
  page?: number;
  pageSize?: number;
  q?: string;
};

/**
 * Danh sách phiếu kiểm kê
 */
export async function listStockCounts(params: ListStockCountsParams) {
  const { locationId, status, page = 1, pageSize = 20, q } = params;

  const where: Prisma.StockCountWhereInput = {};
  if (locationId) where.locationId = locationId;
  if (status) where.status = status;
  if (q?.trim()) {
    where.OR = [
      { refNo: { contains: q.trim(), mode: "insensitive" } },
      { note: { contains: q.trim(), mode: "insensitive" } },
    ];
  }

  const [total, rows] = await Promise.all([
    prisma.stockCount.count({ where }),
    prisma.stockCount.findMany({
      where,
      include: { location: true },
      orderBy: { createdAt: "desc" },
      skip: (page - 1) * pageSize,
      take: pageSize,
    }),
  ]);

  return { total, rows, page, pageSize };
}

// ================== CREATE STOCK COUNT (AUTO LINES) ==================

/**
 * Tạo phiếu kiểm kê + auto sinh dòng cho TOÀN BỘ ITEM
 * - locationId: kho cần kiểm (bạn hiện tại chỉ có 1 kho, nhưng để sẵn cho tương lai)
 * - includeZero: true -> bao gồm cả item đang có bookQty = 0
 */
export async function createStockCountWithLines(input: {
  locationId: string;
  refNo?: string;
  note?: string;
  includeZero?: boolean;
}) {
  const { locationId, refNo, note, includeZero = false } = input;

  // 1️⃣ Đảm bảo kho tồn tại
  const location = await prisma.location.findUnique({
    where: { id: locationId },
  });
  if (!location) {
    throw new Error("Location not found");
  }

  // 2️⃣ Lấy toàn bộ item + tồn hiện tại của kho
  const [items, stocks] = await Promise.all([
    prisma.item.findMany({
      orderBy: { sku: "asc" },
    }),
    prisma.stock.findMany({
      where: { locationId },
    }),
  ]);

  const stockMap = new Map<string, Prisma.Decimal>();
  for (const s of stocks) {
    stockMap.set(s.itemId, toDecimal(s.qty));
  }

  // 3️⃣ Tạo danh sách dòng theo item
  const itemLines = items
    .map((item) => {
      const bookQty = stockMap.get(item.id) ?? new Prisma.Decimal(0);
      return { itemId: item.id, bookQty };
    })
    .filter((row) => (includeZero ? true : !row.bookQty.isZero()));

  // 4️⃣ Tạo StockCount + lines trong transaction
  return prisma.$transaction(async (tx) => {
    const generatedRef =
      refNo && refNo.trim().length > 0
        ? refNo.trim()
        : `KK-${new Date().toISOString().slice(0, 10)}`; // VD: KK-2025-11-18

    const sc = await tx.stockCount.create({
      data: {
        refNo: generatedRef, // không để null vì refNo @unique
        note: note ?? null,
        status: "draft",
        location: {
          connect: { id: locationId },
        },
      },
    });

    if (itemLines.length) {
      await tx.stockCountLine.createMany({
        data: itemLines.map((l) => ({
          stockCountId: sc.id,
          itemId: l.itemId,
          countedQty: new Prisma.Decimal(0), // thực đếm mặc định = 0
        })),
      });
    }

    const full = await tx.stockCount.findUnique({
      where: { id: sc.id },
      include: {
        location: true,
        lines: {
          include: { item: true },
          orderBy: { item: { sku: "asc" } },
        },
      },
    });

    return full;
  });
}

// ================== GET DETAIL (BOOK QTY + DIFF) ==================

/**
 * Lấy chi tiết phiếu kiểm kê + bookQty + diff (thực - sổ)
 * - bookQty lấy từ bảng Stock hiện tại (theo locationId)
 */
export async function getStockCountDetail(id: string) {
  const sc = await prisma.stockCount.findUnique({
    where: { id },
    include: {
      location: true,
      lines: {
        include: { item: true },
        orderBy: { item: { sku: "asc" } },
      },
    },
  });

  if (!sc) throw new Error("StockCount not found");

  const locationId = sc.locationId;
  const itemIds = sc.lines.map((l) => l.itemId);

  if (!itemIds.length) {
    return {
      ...sc,
      lines: sc.lines.map((l) => ({
        ...l,
        bookQty: "0",
        diff: toDecimal(l.countedQty).toString(),
      })),
    };
  }

  const stocks = await prisma.stock.findMany({
    where: {
      locationId,
      itemId: { in: itemIds },
    },
  });

  const stockMap = new Map<string, Prisma.Decimal>();
  for (const s of stocks) {
    stockMap.set(s.itemId, toDecimal(s.qty));
  }

  const lines = sc.lines.map((l) => {
    const bookQty = stockMap.get(l.itemId) ?? new Prisma.Decimal(0);
    const countedQty = toDecimal(l.countedQty);
    const diff = countedQty.minus(bookQty);

    return {
      ...l,
      bookQty: bookQty.toString(),
      diff: diff.toString(),
    };
  });

  return { ...sc, lines };
}

// ================== UPDATE COUNTED QTY ==================

/**
 * Cập nhật countedQty (số thực đếm) của 1 dòng kiểm kê
 */
export async function updateStockCountLine(
  lineId: string,
  patch: { countedQty?: string | number }
) {
  const data: Prisma.StockCountLineUpdateInput = {};
  if (patch.countedQty !== undefined) {
    data.countedQty = toDecimal(patch.countedQty);
  }

  const updated = await prisma.stockCountLine.update({
    where: { id: lineId },
    data,
    include: {
      item: true,
      stockCount: true,
    },
  });

  return updated;
}

// ================== POST STOCK COUNT (ADJUST MOVEMENT) ==================

/**
 * Post phiếu kiểm kê:
 * - Tính chênh lệch (countedQty - bookQty) theo từng item tại location
 * - Nếu diff != 0 -> tạo Movement ADJUST + MovementLine + cập nhật bảng Stock
 * - Đánh dấu StockCount.status = 'posted'
 */
export async function postStockCount(
  stockCountId: string,
  opts?: { movementRefNo?: string; movementNote?: string }
) {
  return prisma.$transaction(async (tx) => {
    const sc = await tx.stockCount.findUnique({
      where: { id: stockCountId },
      include: {
        lines: true,
        location: true,
      },
    });

    if (!sc) throw new Error("StockCount not found");
    if (sc.status === "posted") {
      throw new Error("StockCount already posted");
    }

    const locationId = sc.locationId;
    const itemIds = sc.lines.map((l) => l.itemId);

    const stocks = await tx.stock.findMany({
      where: {
        locationId,
        itemId: { in: itemIds },
      },
    });

    const stockMap = new Map<string, Prisma.Decimal>();
    for (const s of stocks) {
      stockMap.set(s.itemId, toDecimal(s.qty));
    }

    type DiffRow = {
      itemId: string;
      bookQty: Prisma.Decimal;
      countedQty: Prisma.Decimal;
      diff: Prisma.Decimal;
    };

    const diffs: DiffRow[] = [];

    for (const line of sc.lines) {
      const bookQty = stockMap.get(line.itemId) ?? new Prisma.Decimal(0);
      const countedQty = toDecimal(line.countedQty);
      const diff = countedQty.minus(bookQty);

      if (!diff.isZero()) {
        diffs.push({ itemId: line.itemId, bookQty, countedQty, diff });
      }
    }

    // Nếu không chênh lệch -> chỉ cần đổi status
    if (diffs.length === 0) {
      const updated = await tx.stockCount.update({
        where: { id: sc.id },
        data: {
          status: "posted",
        },
      });
      return { stockCount: updated, movement: null };
    }

    // Tạo movement ADJUST, posted = true
    const refBase =
      opts?.movementRefNo ??
      (sc.refNo ? `ADJ-${sc.refNo}` : `ADJ-${new Date().toISOString().slice(0, 10)}`);
    const note =
      opts?.movementNote ??
      `Điều chỉnh tồn kho theo kiểm kê ${sc.refNo ?? sc.id}`;

    const movement = await tx.movement.create({
      data: {
        type: MovementType.ADJUST,
        refNo: refBase,
        note,
        posted: true,
      },
    });

    // Tạo movement lines + cập nhật Stock
    for (const row of diffs) {
      const current = stockMap.get(row.itemId) ?? new Prisma.Decimal(0);
      const newQty = current.plus(row.diff);
      stockMap.set(row.itemId, newQty);

      // cập nhật bảng Stock (upsert)
      await tx.stock.upsert({
        where: {
          itemId_locationId: {
            itemId: row.itemId,
            locationId,
          },
        },
        create: {
          itemId: row.itemId,
          locationId,
          qty: newQty,
        },
        update: {
          qty: newQty,
        },
      });

      // movement line:
      // - nếu diff > 0: coi như nhập vào kho (IN từ "ngoài")
      // - nếu diff < 0: coi như xuất khỏi kho (OUT ra "ngoài")
      const isIncrease = row.diff.greaterThan(0);
      const absQty = row.diff.abs();

      await tx.movementLine.create({
        data: {
          movementId: movement.id,
          itemId: row.itemId,
          fromLocationId: isIncrease ? null : locationId,
          toLocationId: isIncrease ? locationId : null,
          qty: absQty, // luôn là số dương, hướng nằm ở from/to
          note: `Chênh lệch kiểm kê (sổ: ${row.bookQty.toString()} / thực: ${row.countedQty.toString()})`,
        },
      });
    }

    const updatedSC = await tx.stockCount.update({
      where: { id: sc.id },
      data: {
        status: "posted",
      },
    });

    return { stockCount: updatedSC, movement };
  });
}
