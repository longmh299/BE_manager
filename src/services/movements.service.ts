// src/services/movements.service.ts
import { Prisma, PrismaClient, MovementType } from "@prisma/client";

const prisma = new PrismaClient();

/** Chuẩn hoá số lượng về Decimal (nhận string/number). */
function toDecimal(n: string | number): Prisma.Decimal {
  if (typeof n === "number") return new Prisma.Decimal(n);
  return new Prisma.Decimal((n ?? "0").toString().trim());
}

/** ------------------------------------------------------------------
 * LIST movements (có include lines + item + fromLoc/toLoc để xem nhanh)
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
      orderBy: { createdAt: "desc" },
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
      },
    }),
    prisma.movement.count({ where }),
  ]);

  return { rows, total, page, pageSize };
}

/** ------------------------------------------------------------------
 * GET BY ID — chọn includeLines/includeInvoice bằng spread-conditional
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
          include: {
            item: true,
            fromLoc: true,
            toLoc: true,
          },
        },
      }),
      ...(opts?.includeInvoice && { invoice: true }),
    },
  });
}

/** ------------------------------------------------------------------
 * Tạo movement draft.
 * Nếu refNo trùng (P2002) sẽ tự động thêm hậu tố -01, -02, ... và retry.
 * ------------------------------------------------------------------ */
export async function createDraft(
  type: MovementType,
  payload: { refNo?: string; note?: string }
) {
  let baseRef = payload.refNo?.trim();
  if (!baseRef || baseRef.length < 3) {
    baseRef = `MV-${Date.now()}`;
  }

  let attempt = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const refNo =
      attempt === 0 ? baseRef : `${baseRef}-${String(attempt).padStart(2, "0")}`;

    try {
      return await prisma.movement.create({
        data: {
          type,
          refNo,
          note: payload.note ?? null,
          posted: false,
        },
      });
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
 * Thêm 1 dòng cho movement.
 * ------------------------------------------------------------------ */
export async function addLine(
  movementId: string,
  input: {
    itemId: string;
    fromLocationId?: string | null;
    toLocationId?: string | null;
    qty: string | number;
    note?: string;
  }
) {
  return prisma.movementLine.create({
    data: {
      movementId,
      itemId: input.itemId,
      fromLocationId: input.fromLocationId ?? null,
      toLocationId: input.toLocationId ?? null,
      qty: toDecimal(input.qty),
      note: input.note ?? null,
    },
  });
}

/** ------------------------------------------------------------------
 * Cập nhật 1 dòng movement (SL / item / from / to / note).
 * ------------------------------------------------------------------ */
export async function updateLine(
  lineId: string,
  patch: {
    itemId?: string;
    fromLocationId?: string | null;
    toLocationId?: string | null;
    qty?: string | number;
    note?: string | null;
  }
) {
  const data: Prisma.MovementLineUpdateInput = {};

  if (patch.itemId !== undefined) {
    data.item = { connect: { id: patch.itemId } };
  }

  // fromLoc (theo schema mới)
  if (patch.fromLocationId !== undefined) {
    data.fromLoc = patch.fromLocationId
      ? { connect: { id: patch.fromLocationId } }
      : { disconnect: true };
  }

  // toLoc (theo schema mới)
  if (patch.toLocationId !== undefined) {
    data.toLoc = patch.toLocationId
      ? { connect: { id: patch.toLocationId } }
      : { disconnect: true };
  }

  if (patch.qty !== undefined) data.qty = toDecimal(patch.qty);
  if (patch.note !== undefined) data.note = patch.note;

  return prisma.movementLine.update({
    where: { id: lineId },
    data,
  });
}

/** ------------------------------------------------------------------
 * Xoá 1 dòng movement.
 * ------------------------------------------------------------------ */
export async function deleteLine(lineId: string) {
  return prisma.movementLine.delete({ where: { id: lineId } });
}

/** ------------------------------------------------------------------
 * Đăng (post) movement.
 * ------------------------------------------------------------------ */
export async function postMovement(movementId: string) {
  return prisma.movement.update({
    where: { id: movementId },
    data: { posted: true },
    include: {
      lines: {
        include: {
          item: true,
          fromLoc: true,
          toLoc: true,
        },
      },
    },
  });
}
