// src/services/periodLock.service.ts
import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

/**
 * Lấy ngày đã khoá gần nhất (max closedUntil)
 * Nếu chưa khoá lần nào -> null
 */
export async function getClosedUntil(): Promise<Date | null> {
  const row = await prisma.periodLock.findFirst({
    orderBy: { closedUntil: "desc" },
  });
  return row?.closedUntil ?? null;
}

/**
 * Kiểm tra 1 movement có thuộc kỳ đã khoá không
 */
export async function isMovementLocked(movementId: string): Promise<boolean> {
  const closedUntil = await getClosedUntil();
  if (!closedUntil) return false;

  const mv = await prisma.movement.findUnique({
    where: { id: movementId },
    select: { createdAt: true },
  });
  if (!mv) return false;

  return mv.createdAt <= closedUntil;
}

/**
 * Kiểm tra 1 dòng movement line có thuộc kỳ đã khoá không
 */
export async function isMovementLineLocked(lineId: string): Promise<boolean> {
  const closedUntil = await getClosedUntil();
  if (!closedUntil) return false;

  const line = await prisma.movementLine.findUnique({
    where: { id: lineId },
    include: { movement: { select: { createdAt: true } } },
  });
  if (!line || !line.movement) return false;

  return line.movement.createdAt <= closedUntil;
}

/**
 * Nếu movement thuộc kỳ đã khoá -> throw error
 */
export async function ensureMovementNotLocked(movementId: string) {
  if (await isMovementLocked(movementId)) {
    const closedUntil = await getClosedUntil();
    throw Object.assign(new Error(
      `Chứng từ thuộc kỳ đã khoá đến ${closedUntil?.toISOString().slice(0, 10) || ""}, không được phép sửa/xoá/post.`
    ), { status: 400 });
  }
}

/**
 * Nếu movement line thuộc kỳ đã khoá -> throw error
 */
export async function ensureMovementLineNotLocked(lineId: string) {
  if (await isMovementLineLocked(lineId)) {
    const closedUntil = await getClosedUntil();
    throw Object.assign(new Error(
      `Dòng chứng từ thuộc kỳ đã khoá đến ${closedUntil?.toISOString().slice(0, 10) || ""}, không được phép sửa/xoá.`
    ), { status: 400 });
  }
}
