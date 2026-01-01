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
function toDecimal(
  n: string | number | Prisma.Decimal | null | undefined
): Prisma.Decimal {
  if (n instanceof Prisma.Decimal) return n;
  if (typeof n === "number") return new Prisma.Decimal(n);
  return new Prisma.Decimal((n ?? "0").toString().trim() || "0");
}

function assertPositive(d: Prisma.Decimal, msg: string) {
  if (d.lte(0)) throw httpError(400, msg);
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

/** ------------------------------------------------------------------
 * LIST movements
 * ------------------------------------------------------------------ */
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
      orderBy: [
        { occurredAt: "desc" }, // ✅ chuẩn theo ngày phát sinh
        { createdAt: "desc" },
      ],
      skip: (page - 1) * pageSize,
      take: pageSize,
      include: {
        lines: {
          include: {
            item: true,
            fromLoc: true,
            toLoc: true,
          },
        },
        invoice: true,
      },
    }),
    prisma.movement.count({ where }),
  ]);

  return { rows, total, page, pageSize };
}

/** ------------------------------------------------------------------
 * GET BY ID
 * ------------------------------------------------------------------ */
export async function getMovementById(
  id: string,
  opts?: { includeLines?: boolean; includeInvoice?: boolean }
) {
  return prisma.movement.findUniqueOrThrow({
    where: { id },
    include: {
      ...(opts?.includeLines && {
        lines: {
          include: { item: true, fromLoc: true, toLoc: true },
        },
      }),
      ...(opts?.includeInvoice && { invoice: true }),
    },
  });
}

/** ------------------------------------------------------------------
 * CREATE DRAFT
 * Nếu refNo trùng (P2002) sẽ tự thêm -01, -02...
 * ✅ occurredAt (ngày phát sinh) để lock kỳ theo ngày chứng từ
 * ------------------------------------------------------------------ */
export async function createDraft(
  type: MovementType,
  payload: { refNo?: string; note?: string; occurredAt?: string | Date },
  auditCtx?: AuditCtx
) {
  let baseRef = payload.refNo?.trim();
  if (!baseRef || baseRef.length < 3) baseRef = `MV-${Date.now()}`;

  const occurredAt = parseDateInput(payload.occurredAt) ?? new Date();

  // ✅ chặn tạo chứng từ backdate vào kỳ đã khóa
  await ensureDateNotLocked(occurredAt, "tạo chứng từ");

  let attempt = 0;
  // eslint-disable-next-line no-constant-condition
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
        },
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

/** ------------------------------------------------------------------
 * ADD LINE
 * ✅ chặn nếu movement thuộc kỳ khóa hoặc đã posted
 * ------------------------------------------------------------------ */
export async function addLine(
  movementId: string,
  input: {
    itemId: string;
    fromLocationId?: string | null;
    toLocationId?: string | null;
    qty: string | number;
    note?: string;
    unitCost?: string | number | null; // cho IN/ADJUST-IN nếu muốn nhập
  },
  auditCtx?: AuditCtx
) {
  // ✅ lock theo occurredAt
  await ensureMovementNotLocked(movementId);

  const mv = await prisma.movement.findUnique({
    where: { id: movementId },
    select: { posted: true },
  });
  if (!mv) throw httpError(404, "Movement not found");
  if (mv.posted) throw httpError(409, "Chứng từ đã post, không được thêm dòng.");

  const created = await prisma.movementLine.create({
    data: {
      movementId,
      itemId: input.itemId,
      fromLocationId: input.fromLocationId ?? null,
      toLocationId: input.toLocationId ?? null,
      qty: toDecimal(input.qty),
      note: input.note ?? null,
      unitCost: input.unitCost == null ? null : toDecimal(input.unitCost),
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

/** ------------------------------------------------------------------
 * UPDATE LINE
 * ✅ chặn nếu line thuộc kỳ khóa hoặc movement đã posted
 * ------------------------------------------------------------------ */
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
    include: { movement: { select: { posted: true } } },
  });
  if (!before) throw httpError(404, "Movement line not found");
  if ((before as any).movement?.posted)
    throw httpError(409, "Chứng từ đã post, không được sửa dòng.");

  const data: Prisma.MovementLineUpdateInput = {};

  if (patch.itemId !== undefined) {
    data.item = { connect: { id: patch.itemId } };
  }

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
  if (patch.note !== undefined) data.note = patch.note;
  if (patch.unitCost !== undefined) {
    data.unitCost = patch.unitCost == null ? null : toDecimal(patch.unitCost);
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
        costTotal:
          (before as any).costTotal == null ? null : decToNum((before as any).costTotal),
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
        costTotal:
          (updated as any).costTotal == null ? null : decToNum((updated as any).costTotal),
        note: updated.note,
      },
    },
    meta: auditCtx?.meta,
  });

  return updated;
}

/** ------------------------------------------------------------------
 * DELETE LINE
 * ✅ chặn nếu line thuộc kỳ khóa hoặc movement đã posted
 * ------------------------------------------------------------------ */
export async function deleteLine(lineId: string, auditCtx?: AuditCtx) {
  await ensureMovementLineNotLocked(lineId);

  const before = await prisma.movementLine.findUnique({
    where: { id: lineId },
    include: { movement: { select: { posted: true } } },
  });
  if (!before) throw httpError(404, "Movement line not found");
  if ((before as any).movement?.posted)
    throw httpError(409, "Chứng từ đã post, không được xóa dòng.");

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
        costTotal:
          (before as any).costTotal == null ? null : decToNum((before as any).costTotal),
        note: before.note,
      },
    },
    after: { deletedLineId: deleted.id },
    meta: auditCtx?.meta,
  });

  return deleted;
}

/** ------------------------------------------------------------------
 * POST MOVEMENT (APPLY STOCK + AVG COST)
 * ------------------------------------------------------------------ */
export async function postMovement(movementId: string, auditCtx?: AuditCtx) {
  // ✅ lock theo occurredAt
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
        include: {
          lines: { include: { item: true, fromLoc: true, toLoc: true } },
          invoice: true,
        },
      });
      return {
        alreadyPosted: true as const,
        movement: full,
        audit: null as any,
      };
    }

    async function getOrCreateStock(itemId: string, locationId: string) {
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

    type Pair = { itemId: string; locationId: string };
    const pairsMap = new Map<string, Pair>();

    for (const line of mv.lines) {
      if (line.fromLocationId) {
        pairsMap.set(`${line.itemId}__${line.fromLocationId}`, {
          itemId: line.itemId,
          locationId: line.fromLocationId,
        });
      }
      if (line.toLocationId) {
        pairsMap.set(`${line.itemId}__${line.toLocationId}`, {
          itemId: line.itemId,
          locationId: line.toLocationId,
        });
      }
    }

    const pairs = Array.from(pairsMap.values());

    const beforeStocks = await Promise.all(
      pairs.map(async (p) => {
        const s = await tx.stock.findUnique({
          where: { itemId_locationId: { itemId: p.itemId, locationId: p.locationId } },
          select: { itemId: true, locationId: true, qty: true, avgCost: true },
        });

        const row =
          s ?? {
            itemId: p.itemId,
            locationId: p.locationId,
            qty: new Prisma.Decimal(0),
            avgCost: new Prisma.Decimal(0),
          };

        return {
          itemId: row.itemId,
          locationId: row.locationId,
          qty: decToNum(row.qty),
          avgCost: decToNum(row.avgCost),
        };
      })
    );

    for (const line of mv.lines) {
      const qty = toDecimal(line.qty);
      assertPositive(qty, "Qty must be > 0");

      // TRANSFER
      if (mv.type === "TRANSFER") {
        if (!line.fromLocationId || !line.toLocationId) {
          throw httpError(400, "TRANSFER requires fromLocationId and toLocationId");
        }
        if (line.fromLocationId === line.toLocationId) {
          throw httpError(400, "TRANSFER from/to cannot be same");
        }

        const fromStock = await getOrCreateStock(line.itemId, line.fromLocationId);
        if (fromStock.qty.lt(qty)) throw httpError(400, "Not enough stock to transfer");

        const unitCost = fromStock.avgCost;
        const costTotal = qty.mul(unitCost);

        // OUT (✅ KHÔNG reset avgCost khi qty -> 0, để giữ giá vốn gần nhất)
        const newFromQty = fromStock.qty.sub(qty);
        await tx.stock.update({
          where: {
            itemId_locationId: {
              itemId: line.itemId,
              locationId: line.fromLocationId,
            },
          },
          data: { qty: newFromQty },
        });

        // IN
        const toStock = await getOrCreateStock(line.itemId, line.toLocationId);
        const newQty = toStock.qty.add(qty);
        const newAvg = toStock.qty.eq(0)
          ? unitCost
          : toStock.qty.mul(toStock.avgCost).add(qty.mul(unitCost)).div(newQty);

        await tx.stock.update({
          where: {
            itemId_locationId: {
              itemId: line.itemId,
              locationId: line.toLocationId,
            },
          },
          data: { qty: newQty, avgCost: newAvg },
        });

        await tx.movementLine.update({
          where: { id: line.id },
          data: { unitCost, costTotal },
        });

        continue;
      }

      // IN
      if (mv.type === "IN") {
        if (!line.toLocationId) throw httpError(400, "IN requires toLocationId");
        if (line.unitCost == null) throw httpError(400, "IN requires unitCost");

        const unitCost = toDecimal(line.unitCost);
        const costTotal = qty.mul(unitCost);

        const stock = await getOrCreateStock(line.itemId, line.toLocationId);
        const newQty = stock.qty.add(qty);
        const newAvg = stock.qty.eq(0)
          ? unitCost
          : stock.qty.mul(stock.avgCost).add(qty.mul(unitCost)).div(newQty);

        await tx.stock.update({
          where: {
            itemId_locationId: {
              itemId: line.itemId,
              locationId: line.toLocationId,
            },
          },
          data: { qty: newQty, avgCost: newAvg },
        });

        await tx.movementLine.update({
          where: { id: line.id },
          data: { unitCost, costTotal },
        });

        continue;
      }

      // OUT
      if (mv.type === "OUT") {
        if (!line.fromLocationId) throw httpError(400, "OUT requires fromLocationId");

        const stock = await getOrCreateStock(line.itemId, line.fromLocationId);
        if (stock.qty.lt(qty)) throw httpError(400, "Not enough stock");

        const unitCost = stock.avgCost;
        const costTotal = qty.mul(unitCost);

        const newQty = stock.qty.sub(qty);

        // ✅ KHÔNG reset avgCost khi qty -> 0
        await tx.stock.update({
          where: {
            itemId_locationId: {
              itemId: line.itemId,
              locationId: line.fromLocationId,
            },
          },
          data: { qty: newQty },
        });

        await tx.movementLine.update({
          where: { id: line.id },
          data: { unitCost, costTotal },
        });

        continue;
      }

      // ADJUST
      if (mv.type === "ADJUST") {
        const hasIn = !!line.toLocationId;
        const hasOut = !!line.fromLocationId;

        if (!hasIn && !hasOut) {
          throw httpError(400, "ADJUST requires fromLocationId or toLocationId");
        }

        if (hasIn && hasOut) {
          throw httpError(
            400,
            "ADJUST chỉ cho phép 1 chiều (IN hoặc OUT). Nếu chuyển kho hãy dùng TRANSFER."
          );
        }

        if (hasOut) {
          const stock = await getOrCreateStock(line.itemId, line.fromLocationId!);
          if (stock.qty.lt(qty)) throw httpError(400, "Not enough stock for ADJUST OUT");

          const unitCost = stock.avgCost;
          const costTotal = qty.mul(unitCost);

          const newQty = stock.qty.sub(qty);

          // ✅ KHÔNG reset avgCost khi qty -> 0
          await tx.stock.update({
            where: {
              itemId_locationId: {
                itemId: line.itemId,
                locationId: line.fromLocationId!,
              },
            },
            data: { qty: newQty },
          });

          await tx.movementLine.update({
            where: { id: line.id },
            data: { unitCost, costTotal },
          });
        }

        if (hasIn) {
          if (line.unitCost == null) throw httpError(400, "ADJUST IN requires unitCost");

          const unitCost = toDecimal(line.unitCost);
          const costTotal = qty.mul(unitCost);

          const stock = await getOrCreateStock(line.itemId, line.toLocationId!);
          const newQty = stock.qty.add(qty);
          const newAvg = stock.qty.eq(0)
            ? unitCost
            : stock.qty.mul(stock.avgCost).add(qty.mul(unitCost)).div(newQty);

          await tx.stock.update({
            where: {
              itemId_locationId: {
                itemId: line.itemId,
                locationId: line.toLocationId!,
              },
            },
            data: { qty: newQty, avgCost: newAvg },
          });

          await tx.movementLine.update({
            where: { id: line.id },
            data: { unitCost, costTotal },
          });
        }

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
      include: {
        lines: { include: { item: true, fromLoc: true, toLoc: true } },
        invoice: true,
      },
    });

    const afterStocks = await Promise.all(
      pairs.map(async (p) => {
        const s = await tx.stock.findUnique({
          where: { itemId_locationId: { itemId: p.itemId, locationId: p.locationId } },
          select: { itemId: true, locationId: true, qty: true, avgCost: true },
        });

        const row =
          s ?? {
            itemId: p.itemId,
            locationId: p.locationId,
            qty: new Prisma.Decimal(0),
            avgCost: new Prisma.Decimal(0),
          };

        return {
          itemId: row.itemId,
          locationId: row.locationId,
          qty: decToNum(row.qty),
          avgCost: decToNum(row.avgCost),
        };
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
      },
    },
    meta: auditCtx?.meta,
  });

  return result.movement;
}
