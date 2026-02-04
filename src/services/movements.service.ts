// src/services/movements.service.ts
import { Prisma, PrismaClient, MovementType } from "@prisma/client";
import { auditLog, type AuditCtx } from "./audit.service";
import {
  ensureDateNotLocked,
  ensureMovementLineNotLocked,
  ensureMovementNotLocked,
} from "./periodLock.service";

const prisma = new PrismaClient();

function httpError(status: number, message: string) {
  const err: any = new Error(message);
  err.status = status;
  err.statusCode = status;
  return err;
}

/** Chuẩn hoá về Prisma.Decimal */
function toDecimal(n: string | number | Prisma.Decimal | null | undefined): Prisma.Decimal {
  if (n instanceof Prisma.Decimal) return n;
  if (typeof n === "number") return new Prisma.Decimal(n);
  return new Prisma.Decimal((n ?? "0").toString().trim() || "0");
}

function parseDateInput(v: any): Date | null {
  if (!v) return null;
  if (v instanceof Date) return v;
  const d = new Date(String(v));
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

/** Decimal -> number an toàn cho audit JSON */
function decToNum(v: any): number {
  if (v == null) return 0;
  try {
    if (v instanceof Prisma.Decimal) return Number(v.toString());
    const n = typeof v === "number" ? v : Number(String(v));
    return Number.isFinite(n) ? n : 0;
  } catch {
    return 0;
  }
}

function assertPositive(d: Prisma.Decimal, msg: string) {
  if (d.lte(0)) throw httpError(400, msg);
}

/** ============================================================
 * ✅ pick kho mặc định (Location đầu tiên)
 * - dùng khi movement.warehouseId chưa set
 * ============================================================ */
async function getDefaultLocationId(tx: Prisma.TransactionClient): Promise<string> {
  // ưu tiên createdAt nếu có, không thì fallback id
  // (nếu schema Location của m không có createdAt, Prisma sẽ báo -> đổi orderBy về id)
  try {
    const loc = await tx.location.findFirst({
      orderBy: { createdAt: "asc" } as any,
      select: { id: true },
    });
    if (loc?.id) return loc.id;
  } catch {
    // ignore, fallback
  }

  const loc2 = await tx.location.findFirst({
    orderBy: { id: "asc" },
    select: { id: true },
  });
  if (!loc2?.id) throw httpError(400, "Chưa có kho/location.");
  return loc2.id;
}

/** ============================================================
 * LIST movements
 * ============================================================ */
export async function listMovements(q = "", page = 1, pageSize = 20) {
  const where: Prisma.MovementWhereInput = q
    ? {
        OR: [
          { refNo: { contains: q, mode: "insensitive" } },
          { note: { contains: q, mode: "insensitive" } },
        ],
      }
    : {};

  const [rows, total] = await Promise.all([
    prisma.movement.findMany({
      where,
      orderBy: [{ occurredAt: "desc" }, { createdAt: "desc" }],
      skip: (page - 1) * pageSize,
      take: pageSize,
      include: {
        lines: { include: { item: true, fromLoc: true, toLoc: true } },
        invoice: true,
      },
    }),
    prisma.movement.count({ where }),
  ]);

  return { rows, total, page, pageSize };
}

/** ============================================================
 * GET BY ID
 * ============================================================ */
export async function getMovementById(
  id: string,
  opts?: { includeLines?: boolean; includeInvoice?: boolean }
) {
  return prisma.movement.findUniqueOrThrow({
    where: { id },
    include: {
      ...(opts?.includeLines && {
        lines: { include: { item: true, fromLoc: true, toLoc: true } },
      }),
      ...(opts?.includeInvoice && { invoice: true }),
    },
  });
}

/** ============================================================
 * CREATE DRAFT
 * ============================================================ */
export async function createDraft(
  type: MovementType,
  payload: {
    refNo?: string;
    note?: string;
    occurredAt?: string | Date;
    warehouseId?: string | null; // ✅ vẫn giữ field, nhưng không bắt buộc
    invoiceId?: string | null;
  },
  auditCtx?: AuditCtx
) {
  let baseRef = payload.refNo?.trim();
  if (!baseRef || baseRef.length < 3) baseRef = `MV-${Date.now()}`;

  const occurredAt = parseDateInput(payload.occurredAt) ?? new Date();

  await ensureDateNotLocked(occurredAt, "tạo chứng từ");

  let attempt = 0;
  while (true) {
    const refNo =
      attempt === 0 ? baseRef : `${baseRef}-${String(attempt).padStart(2, "0")}`;

    try {
      const created = await prisma.movement.create({
        data: {
          type,
          refNo,
          note: payload.note ?? null,
          posted: false,
          occurredAt,
          postedAt: null,
          warehouseId: payload.warehouseId ?? null,
          invoiceId: payload.invoiceId ?? null,
        } as any,
      });

      await auditLog(prisma, {
        userId: auditCtx?.userId,
        userRole: auditCtx?.userRole,
        action: "MOVEMENT_CREATE",
        entity: "Movement",
        entityId: created.id,
        before: null,
        after: {
          id: created.id,
          type: created.type,
          refNo: created.refNo,
          note: created.note,
          posted: created.posted,
          occurredAt: (created as any).occurredAt?.toISOString?.() ?? null,
          warehouseId: (created as any).warehouseId ?? null,
          invoiceId: (created as any).invoiceId ?? null,
        },
        meta: auditCtx?.meta,
      });

      return created;
    } catch (e: any) {
      if (
        e instanceof Prisma.PrismaClientKnownRequestError &&
        e.code === "P2002" &&
        Array.isArray(e.meta?.target) &&
        e.meta.target.includes("refNo")
      ) {
        attempt += 1;
        continue;
      }
      throw e;
    }
  }
}

/** ============================================================
 * ADD LINE
 * - ✅ ADJUST: qty là qtyDelta (+/-), qty != 0
 * - ✅ REVALUE: unitCost > 0, qty forced = 0
 * - IN/OUT/TRANSFER giữ như cũ (qty > 0)
 * ============================================================ */
export async function addLine(
  movementId: string,
  input: {
    itemId: string;
    fromLocationId?: string | null;
    toLocationId?: string | null;
    qty?: string | number; // ✅ cho phép undefined với REVALUE
    note?: string;
    unitCost?: string | number | null;
  },
  auditCtx?: AuditCtx
) {
  await ensureMovementNotLocked(movementId);

  const mv = await prisma.movement.findUnique({
    where: { id: movementId },
    select: { id: true, posted: true, type: true },
  });
  if (!mv) throw httpError(404, "Movement not found");
  if (mv.posted) throw httpError(409, "Chứng từ đã post, không được thêm dòng.");

  const t = String(mv.type || "").toUpperCase();

  // ✅ chuẩn hoá input theo type
  let qty = toDecimal(input.qty ?? 0);
  const unitCost = input.unitCost == null ? null : toDecimal(input.unitCost);

  if (t === "ADJUST") {
    // ✅ ADJUST: qtyDelta != 0 (âm/dương)
    if (qty.eq(0)) throw httpError(400, "ADJUST: qtyDelta phải khác 0 (âm/dương).");
    // single-warehouse adjust: không dùng from/to nữa -> cấm để tránh hiểu thành transfer
    if (input.fromLocationId || input.toLocationId) {
      throw httpError(400, "ADJUST: hệ thống 1 kho, không dùng from/to. Chỉ nhập SL (+/-) và (tuỳ chọn) giá vốn.");
    }
    // unitCost optional
  } else if (t === "REVALUE") {
    // ✅ REVALUE: không đổi qty
    qty = new Prisma.Decimal(0);
    if (unitCost == null || unitCost.lte(0)) {
      throw httpError(400, "REVALUE: unitCost (giá vốn mới) phải > 0.");
    }
    // from/to không dùng
  } else {
    // giữ logic cũ cho IN/OUT/TRANSFER/...
    assertPositive(qty, "Qty must be > 0");
  }

  const created = await prisma.movementLine.create({
    data: {
      movementId,
      itemId: input.itemId,
      fromLocationId: input.fromLocationId ?? null,
      toLocationId: input.toLocationId ?? null,
      qty,
      note: input.note ?? null,
      unitCost: unitCost,
    },
  });

  await auditLog(prisma, {
    userId: auditCtx?.userId,
    userRole: auditCtx?.userRole,
    action: "MOVEMENT_LINE_ADD",
    entity: "Movement",
    entityId: movementId,
    before: null,
    after: {
      line: {
        id: created.id,
        movementId: created.movementId,
        itemId: created.itemId,
        fromLocationId: created.fromLocationId,
        toLocationId: created.toLocationId,
        qty: decToNum(created.qty),
        unitCost: created.unitCost == null ? null : decToNum(created.unitCost),
        note: created.note,
      },
    },
    meta: auditCtx?.meta,
  });

  return created;
}

/** ============================================================
 * UPDATE LINE
 * - ✅ ADJUST: qtyDelta != 0
 * - ✅ REVALUE: qty forced 0, unitCost > 0
 * ============================================================ */
export async function updateLine(
  lineId: string,
  patch: {
    itemId?: string;
    fromLocationId?: string | null;
    toLocationId?: string | null;
    qty?: string | number;
    note?: string | null;
    unitCost?: string | number | null;
  },
  auditCtx?: AuditCtx
) {
  await ensureMovementLineNotLocked(lineId);

  const before = await prisma.movementLine.findUnique({
    where: { id: lineId },
    include: { movement: { select: { posted: true, type: true } } },
  });
  if (!before) throw httpError(404, "Movement line not found");
  if ((before as any).movement?.posted) throw httpError(409, "Chứng từ đã post, không được sửa dòng.");

  const mvType = String((before as any).movement?.type || "").toUpperCase();

  const data: Prisma.MovementLineUpdateInput = {};

  if (patch.itemId !== undefined) data.item = { connect: { id: patch.itemId } };

  if (patch.note !== undefined) data.note = patch.note;

  if (mvType === "ADJUST") {
    if (patch.fromLocationId !== undefined || patch.toLocationId !== undefined) {
      if (patch.fromLocationId || patch.toLocationId) {
        throw httpError(400, "ADJUST: hệ thống 1 kho, không dùng from/to.");
      }
      // nếu user gửi null cũng ok: giữ null
      data.fromLoc = { disconnect: true };
      data.toLoc = { disconnect: true };
    }

    if (patch.qty !== undefined) {
      const q = toDecimal(patch.qty);
      if (q.eq(0)) throw httpError(400, "ADJUST: qtyDelta phải khác 0 (âm/dương).");
      data.qty = q;
    }
    if (patch.unitCost !== undefined) {
      data.unitCost = patch.unitCost == null ? null : toDecimal(patch.unitCost);
    }
  } else if (mvType === "REVALUE") {
    // qty forced 0
    data.qty = new Prisma.Decimal(0);

    if (patch.fromLocationId !== undefined || patch.toLocationId !== undefined) {
      if (patch.fromLocationId || patch.toLocationId) {
        throw httpError(400, "REVALUE: không dùng from/to.");
      }
      data.fromLoc = { disconnect: true };
      data.toLoc = { disconnect: true };
    }

    if (patch.unitCost !== undefined) {
      const uc = patch.unitCost == null ? null : toDecimal(patch.unitCost);
      if (uc == null || uc.lte(0)) throw httpError(400, "REVALUE: unitCost (giá vốn mới) phải > 0.");
      data.unitCost = uc;
    } else {
      // nếu không patch unitCost thì vẫn phải đang có unitCost hợp lệ
      const uc0 = before.unitCost == null ? null : toDecimal(before.unitCost);
      if (uc0 == null || uc0.lte(0)) throw httpError(400, "REVALUE: unitCost (giá vốn mới) phải > 0.");
    }
  } else {
    // type cũ giữ logic như trước
    if (patch.fromLocationId !== undefined) {
      data.fromLoc = patch.fromLocationId
        ? { connect: { id: patch.fromLocationId } }
        : { disconnect: true };
    }
    if (patch.toLocationId !== undefined) {
      data.toLoc = patch.toLocationId
        ? { connect: { id: patch.toLocationId } }
        : { disconnect: true };
    }
    if (patch.qty !== undefined) data.qty = toDecimal(patch.qty);
    if (patch.unitCost !== undefined) data.unitCost = patch.unitCost == null ? null : toDecimal(patch.unitCost);
  }

  const updated = await prisma.movementLine.update({ where: { id: lineId }, data });

  await auditLog(prisma, {
    userId: auditCtx?.userId,
    userRole: auditCtx?.userRole,
    action: "MOVEMENT_LINE_UPDATE",
    entity: "Movement",
    entityId: before.movementId,
    before: {
      line: {
        id: before.id,
        movementId: before.movementId,
        itemId: before.itemId,
        fromLocationId: before.fromLocationId,
        toLocationId: before.toLocationId,
        qty: decToNum(before.qty),
        unitCost: before.unitCost == null ? null : decToNum(before.unitCost),
        costTotal: (before as any).costTotal == null ? null : decToNum((before as any).costTotal),
        note: before.note,
      },
    },
    after: {
      line: {
        id: updated.id,
        movementId: updated.movementId,
        itemId: updated.itemId,
        fromLocationId: updated.fromLocationId,
        toLocationId: updated.toLocationId,
        qty: decToNum(updated.qty),
        unitCost: updated.unitCost == null ? null : decToNum(updated.unitCost),
        costTotal: (updated as any).costTotal == null ? null : decToNum((updated as any).costTotal),
        note: updated.note,
      },
    },
    meta: auditCtx?.meta,
  });

  return updated;
}

/** ============================================================
 * DELETE LINE
 * ============================================================ */
export async function deleteLine(lineId: string, auditCtx?: AuditCtx) {
  await ensureMovementLineNotLocked(lineId);

  const before = await prisma.movementLine.findUnique({
    where: { id: lineId },
    include: { movement: { select: { posted: true } } },
  });
  if (!before) throw httpError(404, "Movement line not found");
  if ((before as any).movement?.posted) throw httpError(409, "Chứng từ đã post, không được xóa dòng.");

  const deleted = await prisma.movementLine.delete({ where: { id: lineId } });

  await auditLog(prisma, {
    userId: auditCtx?.userId,
    userRole: auditCtx?.userRole,
    action: "MOVEMENT_LINE_DELETE",
    entity: "Movement",
    entityId: before.movementId,
    before: {
      line: {
        id: before.id,
        movementId: before.movementId,
        itemId: before.itemId,
        fromLocationId: before.fromLocationId,
        toLocationId: before.toLocationId,
        qty: decToNum(before.qty),
        unitCost: before.unitCost == null ? null : decToNum(before.unitCost),
        costTotal: (before as any).costTotal == null ? null : decToNum((before as any).costTotal),
        note: before.note,
      },
    },
    after: { deletedLineId: deleted.id },
    meta: auditCtx?.meta,
  });

  return deleted;
}

/** ============================================================
 * POST MOVEMENT
 * - ✅ Nếu movement.warehouseId null => auto pick Location đầu tiên
 * - ✅ ADJUST: qtyDelta (+/-)
 *    + qtyDelta > 0: tăng tồn; nếu có unitCost>0 => tính lại avgCost theo bình quân
 *    + qtyDelta < 0: giảm tồn; avgCost giữ nguyên; check đủ tồn
 * - ✅ REVALUE: set avgCost mới; qty không đổi
 * - IN/OUT/TRANSFER giữ logic cũ (tương thích)
 * ============================================================ */
export async function postMovement(movementId: string, auditCtx?: AuditCtx) {
  await ensureMovementNotLocked(movementId);

  const result = await prisma.$transaction(async (tx) => {
    const mv = await tx.movement.findUnique({
      where: { id: movementId },
      include: { lines: true },
    });
    if (!mv) throw httpError(404, "Movement not found");

    if (mv.posted) {
      const full = await tx.movement.findUniqueOrThrow({
        where: { id: movementId },
        include: { lines: { include: { item: true, fromLoc: true, toLoc: true } }, invoice: true },
      });
      return { alreadyPosted: true as const, movement: full, audit: null as any };
    }

    // ✅ resolve locationId = warehouseId || default location
    const locationId = (mv as any).warehouseId
      ? String((mv as any).warehouseId)
      : await getDefaultLocationId(tx);

    async function getOrCreateStock(itemId: string) {
      return tx.stock.upsert({
        where: { itemId_locationId: { itemId, locationId } },
        create: {
          itemId,
          locationId,
          qty: new Prisma.Decimal(0),
          avgCost: new Prisma.Decimal(0),
        },
        update: {},
      });
    }

    // audit snapshot before
    const pairs = Array.from(new Set(mv.lines.map((l) => l.itemId))).map((itemId) => ({
      itemId,
      locationId,
    }));

    const beforeStocks = await Promise.all(
      pairs.map(async (p) => {
        const s = await tx.stock.findUnique({
          where: { itemId_locationId: { itemId: p.itemId, locationId: p.locationId } },
          select: { itemId: true, locationId: true, qty: true, avgCost: true },
        });
        const row =
          s ?? { itemId: p.itemId, locationId: p.locationId, qty: new Prisma.Decimal(0), avgCost: new Prisma.Decimal(0) };

        return { itemId: row.itemId, locationId: row.locationId, qty: decToNum(row.qty), avgCost: decToNum(row.avgCost) };
      })
    );

    const mvType = String(mv.type || "").toUpperCase();

    for (const line of mv.lines) {
      // ========= REVALUE =========
      if (mvType === "REVALUE") {
        const uc = line.unitCost == null ? null : toDecimal(line.unitCost);
        if (uc == null || uc.lte(0)) throw httpError(400, "REVALUE: unitCost (giá vốn mới) phải > 0.");

        const stock = await getOrCreateStock(line.itemId);

        // qty không đổi, avgCost set mới
        await tx.stock.update({
          where: { itemId_locationId: { itemId: line.itemId, locationId } },
          data: { avgCost: uc },
        });

        await tx.movementLine.update({
          where: { id: line.id },
          data: { qty: new Prisma.Decimal(0), unitCost: uc, costTotal: new Prisma.Decimal(0) },
        });

        continue;
      }

      // ========= ADJUST (qtyDelta +/-) =========
      if (mvType === "ADJUST") {
        const delta = toDecimal(line.qty);
        if (delta.eq(0)) throw httpError(400, "ADJUST: qtyDelta phải khác 0 (âm/dương).");

        // cấm from/to trong 1 kho
        if (line.fromLocationId || line.toLocationId) {
          throw httpError(400, "ADJUST: hệ thống 1 kho, không dùng from/to.");
        }

        const stock = await getOrCreateStock(line.itemId);

        if (delta.gt(0)) {
          const addQty = delta;

          // nếu user nhập unitCost thì tính lại avgCost; nếu không -> dùng avgCost hiện tại
          const uc = line.unitCost == null ? null : toDecimal(line.unitCost);
          const useCost = uc && uc.gt(0) ? uc : stock.avgCost;

          const newQty = stock.qty.add(addQty);
          const newAvg =
            stock.qty.eq(0)
              ? useCost
              : stock.qty.mul(stock.avgCost).add(addQty.mul(useCost)).div(newQty);

          await tx.stock.update({
            where: { itemId_locationId: { itemId: line.itemId, locationId } },
            data: { qty: newQty, avgCost: newAvg },
          });

          await tx.movementLine.update({
            where: { id: line.id },
            data: { unitCost: useCost, costTotal: addQty.mul(useCost) },
          });
        } else {
          const subQty = delta.abs();
          if (stock.qty.lt(subQty)) throw httpError(400, "ADJUST (-): Không đủ tồn để giảm.");

          const newQty = stock.qty.sub(subQty);

          await tx.stock.update({
            where: { itemId_locationId: { itemId: line.itemId, locationId } },
            data: { qty: newQty }, // avgCost giữ nguyên
          });

          await tx.movementLine.update({
            where: { id: line.id },
            data: { unitCost: stock.avgCost, costTotal: subQty.mul(stock.avgCost) },
          });
        }

        continue;
      }

      // ========= giữ logic cũ cho các type khác =========
      const qty = toDecimal(line.qty);
      assertPositive(qty, "Qty must be > 0");

      // TRANSFER
      if (mvType === "TRANSFER") {
        if (!line.fromLocationId || !line.toLocationId) {
          throw httpError(400, "TRANSFER requires fromLocationId and toLocationId");
        }
        if (line.fromLocationId === line.toLocationId) {
          throw httpError(400, "TRANSFER from/to cannot be same");
        }

        const fromStock = await tx.stock.upsert({
          where: { itemId_locationId: { itemId: line.itemId, locationId: line.fromLocationId } },
          create: { itemId: line.itemId, locationId: line.fromLocationId, qty: new Prisma.Decimal(0), avgCost: new Prisma.Decimal(0) },
          update: {},
        });

        if (fromStock.qty.lt(qty)) throw httpError(400, "Not enough stock to transfer");

        const unitCost = fromStock.avgCost;
        const costTotal = qty.mul(unitCost);

        await tx.stock.update({
          where: { itemId_locationId: { itemId: line.itemId, locationId: line.fromLocationId } },
          data: { qty: fromStock.qty.sub(qty) },
        });

        const toStock = await tx.stock.upsert({
          where: { itemId_locationId: { itemId: line.itemId, locationId: line.toLocationId } },
          create: { itemId: line.itemId, locationId: line.toLocationId, qty: new Prisma.Decimal(0), avgCost: new Prisma.Decimal(0) },
          update: {},
        });

        const newQty = toStock.qty.add(qty);
        const newAvg = toStock.qty.eq(0)
          ? unitCost
          : toStock.qty.mul(toStock.avgCost).add(qty.mul(unitCost)).div(newQty);

        await tx.stock.update({
          where: { itemId_locationId: { itemId: line.itemId, locationId: line.toLocationId } },
          data: { qty: newQty, avgCost: newAvg },
        });

        await tx.movementLine.update({ where: { id: line.id }, data: { unitCost, costTotal } });
        continue;
      }

      // IN
      if (mvType === "IN") {
        const toLoc = line.toLocationId ?? locationId; // default theo kho
        if (!toLoc) throw httpError(400, "IN requires toLocationId");
        if (line.unitCost == null) throw httpError(400, "IN requires unitCost");

        const unitCost = toDecimal(line.unitCost);
        const costTotal = qty.mul(unitCost);

        const stock = await tx.stock.upsert({
          where: { itemId_locationId: { itemId: line.itemId, locationId: toLoc } },
          create: { itemId: line.itemId, locationId: toLoc, qty: new Prisma.Decimal(0), avgCost: new Prisma.Decimal(0) },
          update: {},
        });

        const newQty = stock.qty.add(qty);
        const newAvg = stock.qty.eq(0)
          ? unitCost
          : stock.qty.mul(stock.avgCost).add(qty.mul(unitCost)).div(newQty);

        await tx.stock.update({
          where: { itemId_locationId: { itemId: line.itemId, locationId: toLoc } },
          data: { qty: newQty, avgCost: newAvg },
        });

        await tx.movementLine.update({ where: { id: line.id }, data: { unitCost, costTotal } });
        continue;
      }

      // OUT
      if (mvType === "OUT") {
        const fromLoc = line.fromLocationId ?? locationId; // default theo kho
        if (!fromLoc) throw httpError(400, "OUT requires fromLocationId");

        const stock = await tx.stock.upsert({
          where: { itemId_locationId: { itemId: line.itemId, locationId: fromLoc } },
          create: { itemId: line.itemId, locationId: fromLoc, qty: new Prisma.Decimal(0), avgCost: new Prisma.Decimal(0) },
          update: {},
        });

        if (stock.qty.lt(qty)) throw httpError(400, "Not enough stock");

        const unitCost = stock.avgCost;
        const costTotal = qty.mul(unitCost);

        await tx.stock.update({
          where: { itemId_locationId: { itemId: line.itemId, locationId: fromLoc } },
          data: { qty: stock.qty.sub(qty) },
        });

        await tx.movementLine.update({ where: { id: line.id }, data: { unitCost, costTotal } });
        continue;
      }

      throw httpError(400, `Unsupported movement type: ${mv.type}`);
    }

    await tx.movement.update({
      where: { id: movementId },
      data: { posted: true, postedAt: new Date() },
    });

    const movementAfter = await tx.movement.findUniqueOrThrow({
      where: { id: movementId },
      include: { lines: { include: { item: true, fromLoc: true, toLoc: true } }, invoice: true },
    });

    const afterStocks = await Promise.all(
      pairs.map(async (p) => {
        const s = await tx.stock.findUnique({
          where: { itemId_locationId: { itemId: p.itemId, locationId: p.locationId } },
          select: { itemId: true, locationId: true, qty: true, avgCost: true },
        });
        const row =
          s ?? { itemId: p.itemId, locationId: p.locationId, qty: new Prisma.Decimal(0), avgCost: new Prisma.Decimal(0) };

        return { itemId: row.itemId, locationId: row.locationId, qty: decToNum(row.qty), avgCost: decToNum(row.avgCost) };
      })
    );

    return {
      alreadyPosted: false as const,
      movement: movementAfter,
      audit: {
        movementId,
        refNo: mv.refNo,
        type: mv.type,
        lineCount: mv.lines.length,
        beforeStocks,
        afterStocks,
        locationIdApplied: locationId,
      },
    };
  });

  if (result.alreadyPosted) return result.movement;

  await auditLog(prisma, {
    userId: auditCtx?.userId,
    userRole: auditCtx?.userRole,
    action: "MOVEMENT_POST",
    entity: "Movement",
    entityId: movementId,
    before: { stocks: result.audit?.beforeStocks },
    after: {
      stocks: result.audit?.afterStocks,
      movement: {
        id: result.movement.id,
        refNo: result.movement.refNo,
        type: result.movement.type,
        posted: result.movement.posted,
        postedAt: (result.movement as any).postedAt?.toISOString?.() ?? null,
        occurredAt: (result.movement as any).occurredAt?.toISOString?.() ?? null,
        warehouseId: (result.movement as any).warehouseId ?? null,
      },
      locationIdApplied: result.audit?.locationIdApplied,
    },
    meta: auditCtx?.meta,
  });

  return result.movement;
}
