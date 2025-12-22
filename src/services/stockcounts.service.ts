// src/services/stockcounts.service.ts
import { PrismaClient, Prisma, MovementType } from "@prisma/client";
import { auditLog, type AuditCtx } from "./audit.service";

const prisma = new PrismaClient();

/** Chuẩn hoá số lượng về Decimal */
function toDecimal(n: string | number | Prisma.Decimal): Prisma.Decimal {
  if (n instanceof Prisma.Decimal) return n;
  if (typeof n === "number") return new Prisma.Decimal(n);
  return new Prisma.Decimal((n ?? "0").toString().trim());
}

/** Decimal -> number/string an toàn cho audit JSON */
function decToStr(v: any): string {
  try {
    if (v == null) return "0";
    if (v instanceof Prisma.Decimal) return v.toString();
    return String(v);
  } catch {
    return "0";
  }
}

/** Build refNo movement ADJUST cho stock count (đảm bảo deterministic để truy vết lại khi đã posted) */
function buildAdjustRefNo(sc: { refNo: string | null; id: string }) {
  // Ưu tiên refNo nếu có, fallback id (cuid) => unique ổn
  return `ADJ-${sc.refNo && sc.refNo.trim() ? sc.refNo.trim() : sc.id}`;
}

// ================== LIST STOCK COUNTS ==================

export type ListStockCountsParams = {
  locationId?: string;
  status?: string; // "draft" | "posted"
  page?: number;
  pageSize?: number;
  q?: string;
};

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

// ================== CREATE STOCK COUNT ==================

export async function createStockCountWithLines(
  input: {
    locationId: string;
    refNo?: string;
    note?: string;
    includeZero?: boolean;
  },
  auditCtx?: AuditCtx
) {
  const { locationId, refNo, note, includeZero = false } = input;

  return prisma.$transaction(async (tx) => {
    const location = await tx.location.findUnique({
      where: { id: locationId },
    });
    if (!location) throw new Error("Location not found");

    const items = await tx.item.findMany({
      orderBy: { sku: "asc" },
    });

    const stocks = await tx.stock.findMany({
      where: { locationId },
    });

    const stockMap = new Map<string, Prisma.Decimal>();
    for (const s of stocks) stockMap.set(s.itemId, toDecimal(s.qty));

    const itemLines = items
      .map((item) => ({
        itemId: item.id,
        bookQty: stockMap.get(item.id) ?? new Prisma.Decimal(0),
      }))
      .filter((row) => (includeZero ? true : !row.bookQty.isZero()));

    const generatedRef = refNo?.trim() || `KK-${new Date().toISOString().slice(0, 10)}`;

    const sc = await tx.stockCount.create({
      data: {
        refNo: generatedRef,
        note: note ?? null,
        status: "draft",
        locationId,
      },
    });

    if (itemLines.length) {
      await tx.stockCountLine.createMany({
        data: itemLines.map((l) => ({
          stockCountId: sc.id,
          itemId: l.itemId,
          countedQty: new Prisma.Decimal(0),
        })),
      });
    }

    // ✅ AUDIT: create stock count
    await auditLog(tx, {
      userId: auditCtx?.userId,
      userRole: auditCtx?.userRole,
      action: "STOCKCOUNT_CREATE",
      entity: "StockCount",
      entityId: sc.id,
      before: null,
      after: {
        id: sc.id,
        refNo: sc.refNo,
        status: sc.status,
        locationId,
        includeZero,
        lineCount: itemLines.length,
        note: note ?? null,
      },
      meta: auditCtx?.meta,
    });

    return tx.stockCount.findUnique({
      where: { id: sc.id },
      include: {
        location: true,
        lines: {
          include: { item: true },
          orderBy: { item: { sku: "asc" } },
        },
      },
    });
  });
}

// ================== GET DETAIL ==================

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

  /**
   * ✅ IMPORTANT:
   * - Nếu phiếu đã POSTED: không lấy bookQty từ Stock hiện tại (vì đã bị update bằng countedQty).
   *   Thay vào đó, truy vết Movement ADJUST đã tạo lúc post, lấy diff theo movement lines.
   * - Nếu DRAFT: lấy bookQty từ Stock hiện tại như cũ.
   */
  if (String(sc.status).toLowerCase() === "posted") {
    const adjRefNo = buildAdjustRefNo({ refNo: sc.refNo, id: sc.id });

    const mv = await prisma.movement.findFirst({
      where: { type: MovementType.ADJUST, refNo: adjRefNo },
      select: { id: true },
    });

    // Nếu không tìm thấy movement (trường hợp dữ liệu cũ), fallback về cách cũ (diff sẽ = 0)
    if (mv) {
      const mvLines = await prisma.movementLine.findMany({
        where: {
          movementId: mv.id,
          itemId: { in: itemIds },
        },
        select: {
          itemId: true,
          qty: true,
          fromLocationId: true,
          toLocationId: true,
        },
      });

      // diffSigned per item
      // +qty nếu tăng (toLocationId = locationId)
      // -qty nếu giảm (fromLocationId = locationId)
      const diffMap = new Map<string, Prisma.Decimal>();
      for (const l of mvLines) {
        const qty = toDecimal(l.qty);
        const inc = l.toLocationId === locationId;
        const dec = l.fromLocationId === locationId;
        const signed = inc ? qty : dec ? qty.negated() : new Prisma.Decimal(0);

        if (!signed.isZero()) {
          diffMap.set(l.itemId, (diffMap.get(l.itemId) ?? new Prisma.Decimal(0)).plus(signed));
        }
      }

      const lines = sc.lines.map((l) => {
        const countedQty = toDecimal(l.countedQty);
        const diffSigned = diffMap.get(l.itemId) ?? new Prisma.Decimal(0);
        const bookQtyAtPost = countedQty.minus(diffSigned);

        return {
          ...l,
          bookQty: bookQtyAtPost.toString(),
          diff: diffSigned.toString(),
        };
      });

      return { ...sc, lines };
    }
  }

  // ===== DRAFT (hoặc POSTED nhưng không truy vết được movement) => fallback: lấy stock hiện tại =====
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

// ================== UPDATE LINE ==================

export async function updateStockCountLine(
  lineId: string,
  patch: { countedQty?: string | number },
  auditCtx?: AuditCtx
) {
  const existing = await prisma.stockCountLine.findUnique({
    where: { id: lineId },
    include: { stockCount: true, item: true },
  });
  if (!existing) throw new Error("StockCountLine not found");

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

  // ✅ AUDIT: update countedQty (chỉ log khi thật sự có patch)
  if (patch.countedQty !== undefined) {
    await auditLog(prisma, {
      userId: auditCtx?.userId,
      userRole: auditCtx?.userRole,
      action: "STOCKCOUNT_UPDATE_LINE",
      entity: "StockCountLine",
      entityId: updated.id,
      before: {
        stockCountId: existing.stockCountId,
        stockCountRefNo: existing.stockCount?.refNo ?? null,
        itemId: existing.itemId,
        sku: existing.item?.sku ?? null,
        itemName: existing.item?.name ?? null,
        countedQty: decToStr(existing.countedQty),
      },
      after: {
        stockCountId: updated.stockCountId,
        stockCountRefNo: updated.stockCount?.refNo ?? null,
        itemId: updated.itemId,
        sku: updated.item?.sku ?? null,
        itemName: updated.item?.name ?? null,
        countedQty: decToStr(updated.countedQty),
      },
      meta: auditCtx?.meta,
    });
  }

  return updated;
}

// ================== POST STOCK COUNT ==================

export async function postStockCount(
  stockCountId: string,
  opts?: { movementRefNo?: string; movementNote?: string },
  auditCtx?: AuditCtx
) {
  return prisma.$transaction(async (tx) => {
    const sc = await tx.stockCount.findUnique({
      where: { id: stockCountId },
      include: { lines: true },
    });

    if (!sc) throw new Error("StockCount not found");
    if (sc.status === "posted") throw new Error("StockCount already posted");

    const locationId = sc.locationId;
    const itemIds = sc.lines.map((l) => l.itemId);

    const stocks = await tx.stock.findMany({
      where: {
        locationId,
        itemId: { in: itemIds },
      },
    });

    const stockMap = new Map<string, Prisma.Decimal>();
    for (const s of stocks) stockMap.set(s.itemId, toDecimal(s.qty));

    const diffs = sc.lines
      .map((l) => {
        const book = stockMap.get(l.itemId) ?? new Prisma.Decimal(0);
        const counted = toDecimal(l.countedQty);
        return { itemId: l.itemId, book, counted, diff: counted.minus(book) };
      })
      .filter((r) => !r.diff.isZero());

    // Nếu không chênh lệch -> chỉ đổi status
    if (!diffs.length) {
      const updated = await tx.stockCount.update({
        where: { id: sc.id },
        data: { status: "posted" },
      });

      // ✅ AUDIT: post stock count (no diffs)
      await auditLog(tx, {
        userId: auditCtx?.userId,
        userRole: auditCtx?.userRole,
        action: "STOCKCOUNT_POST",
        entity: "StockCount",
        entityId: sc.id,
        before: { status: sc.status },
        after: { status: "posted" },
        meta: {
          ...(auditCtx?.meta || {}),
          refNo: sc.refNo,
          locationId,
          movementCreated: false,
          diffCount: 0,
        },
      });

      return { stockCount: updated, movementId: null };
    }

    const refNo = opts?.movementRefNo ?? buildAdjustRefNo({ refNo: sc.refNo, id: sc.id });
    const note =
      opts?.movementNote ?? `Điều chỉnh tồn kho theo kiểm kê ${sc.refNo || sc.id}`;

    const movement = await tx.movement.create({
      data: {
        type: MovementType.ADJUST,
        refNo,
        note,
        posted: true,
      },
    });

    for (const r of diffs) {
      const newQty = r.book.plus(r.diff);

      await tx.stock.upsert({
        where: {
          itemId_locationId: { itemId: r.itemId, locationId },
        },
        create: {
          itemId: r.itemId,
          locationId,
          qty: newQty,
        },
        update: { qty: newQty },
      });

      const isIncrease = r.diff.greaterThan(0);

      await tx.movementLine.create({
        data: {
          movementId: movement.id,
          itemId: r.itemId,
          fromLocationId: isIncrease ? null : locationId,
          toLocationId: isIncrease ? locationId : null,
          qty: r.diff.abs(),
          note: `Chênh lệch kiểm kê (sổ ${r.book.toString()} / thực ${r.counted.toString()})`,
        },
      });
    }

    const updated = await tx.stockCount.update({
      where: { id: sc.id },
      data: { status: "posted" },
    });

    // ✅ AUDIT: post stock count (with movement)
    await auditLog(tx, {
      userId: auditCtx?.userId,
      userRole: auditCtx?.userRole,
      action: "STOCKCOUNT_POST",
      entity: "StockCount",
      entityId: sc.id,
      before: { status: sc.status },
      after: { status: "posted", movementId: movement.id, movementRefNo: movement.refNo },
      meta: {
        ...(auditCtx?.meta || {}),
        refNo: sc.refNo,
        locationId,
        movementCreated: true,
        movementId: movement.id,
        movementRefNo: movement.refNo,
        diffCount: diffs.length,
        // tránh log quá nặng: chỉ log summary + top 20 item diff
        diffPreview: diffs.slice(0, 20).map((d) => ({
          itemId: d.itemId,
          bookQty: d.book.toString(),
          countedQty: d.counted.toString(),
          diff: d.diff.toString(),
        })),
      },
    });

    return { stockCount: updated, movementId: movement.id };
  });
}
