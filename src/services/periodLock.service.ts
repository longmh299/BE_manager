// src/services/periodLock.service.ts
import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

export const VN_TZ = "Asia/Ho_Chi_Minh";

function fmtDateVN(d: Date) {
  // en-CA -> YYYY-MM-DD
  return new Intl.DateTimeFormat("en-CA", { timeZone: VN_TZ }).format(d);
}

/** ✅ Format YYYY-MM theo giờ VN */
function fmtMonthVN(d: Date) {
  const y = new Intl.DateTimeFormat("en-CA", { timeZone: VN_TZ, year: "numeric" }).format(d);
  const m = new Intl.DateTimeFormat("en-CA", { timeZone: VN_TZ, month: "2-digit" }).format(d);
  return `${y}-${m}`;
}

/**
 * ✅ NEW: Ensure date thuộc THÁNG HIỆN TẠI theo giờ VN
 * - Dùng cho rule: chỉ được sửa chứng từ trong tháng hiện tại
 */
export async function ensureDateInCurrentMonthVN(date: Date, actionLabel: string) {
  const now = new Date();
  const mNow = fmtMonthVN(now);
  const mDate = fmtMonthVN(date);

  if (mNow !== mDate) {
    throw Object.assign(
      new Error(
        `Chỉ được phép ${actionLabel} trong tháng hiện tại (VN). Ngày chứng từ: ${fmtDateVN(
          date
        )} (tháng ${mDate}), tháng hiện tại: ${mNow}.`
      ),
      { status: 400, statusCode: 400 }
    );
  }
}

/**
 * Lấy ngày đã khoá gần nhất (max closedUntil)
 * Nếu chưa khoá lần nào -> null
 *
 * ✅ ASSUME: closedUntil đã được lưu đúng "cuối ngày VN" từ locks.routes.ts
 */
export async function getClosedUntil(): Promise<Date | null> {
  const row = await prisma.periodLock.findFirst({
    orderBy: { closedUntil: "desc" },
  });
  return row?.closedUntil ?? null;
}

async function getClosedUntilEOD(): Promise<Date | null> {
  // ✅ do NOT recompute end-of-day again (tránh lệch ngày vì timezone)
  return getClosedUntil();
}

/**
 * Kiểm tra 1 movement có thuộc kỳ đã khoá không
 * ✅ Rule: effectiveDate <= closedUntil => LOCKED
 * - effectiveDate ưu tiên occurredAt (ngày phát sinh), fallback createdAt
 */
export async function isMovementLocked(movementId: string): Promise<boolean> {
  const closedUntil = await getClosedUntilEOD();
  if (!closedUntil) return false;

  const mv = await prisma.movement.findUnique({
    where: { id: movementId },
    select: { createdAt: true, occurredAt: true },
  });
  if (!mv) return false;

  const effective = mv.occurredAt ?? mv.createdAt;
  return effective.getTime() <= closedUntil.getTime();
}

/**
 * Kiểm tra 1 dòng movement line có thuộc kỳ đã khoá không
 */
export async function isMovementLineLocked(lineId: string): Promise<boolean> {
  const closedUntil = await getClosedUntilEOD();
  if (!closedUntil) return false;

  const line = await prisma.movementLine.findUnique({
    where: { id: lineId },
    include: { movement: { select: { createdAt: true, occurredAt: true } } },
  });
  if (!line || !line.movement) return false;

  const effective = (line.movement as any).occurredAt ?? line.movement.createdAt;
  return effective.getTime() <= closedUntil.getTime();
}

/**
 * Nếu movement thuộc kỳ đã khoá -> throw error
 */
export async function ensureMovementNotLocked(movementId: string) {
  if (await isMovementLocked(movementId)) {
    const closedUntil = await getClosedUntilEOD();
    throw Object.assign(
      new Error(
        `Chứng từ thuộc kỳ đã khoá đến ${closedUntil ? fmtDateVN(closedUntil) : ""}, không được phép sửa/xoá/post.`
      ),
      { status: 400, statusCode: 400 }
    );
  }
}

/**
 * Nếu movement line thuộc kỳ đã khoá -> throw error
 */
export async function ensureMovementLineNotLocked(lineId: string) {
  if (await isMovementLineLocked(lineId)) {
    const closedUntil = await getClosedUntilEOD();
    throw Object.assign(
      new Error(
        `Dòng chứng từ thuộc kỳ đã khoá đến ${closedUntil ? fmtDateVN(closedUntil) : ""}, không được phép sửa/xoá.`
      ),
      { status: 400, statusCode: 400 }
    );
  }
}

/* =========================================================
   ✅ Lock helpers for StockCount / generic date check
========================================================= */

export async function isDateLocked(date: Date): Promise<boolean> {
  const closedUntil = await getClosedUntilEOD();
  if (!closedUntil) return false;
  return date.getTime() <= closedUntil.getTime();
}

export async function ensureDateNotLocked(date: Date, actionLabel: string) {
  if (await isDateLocked(date)) {
    const closedUntil = await getClosedUntilEOD();
    throw Object.assign(
      new Error(
        `Kỳ sổ đã khoá đến ${closedUntil ? fmtDateVN(closedUntil) : ""}, không được phép ${actionLabel}.`
      ),
      { status: 400, statusCode: 400 }
    );
  }
}

/**
 * Kiểm tra StockCount thuộc kỳ đã khoá không (dựa vào createdAt)
 * (nếu sau này bạn có countDate thì nên dùng countDate thay vì createdAt)
 */
export async function isStockCountLocked(stockCountId: string): Promise<boolean> {
  const closedUntil = await getClosedUntilEOD();
  if (!closedUntil) return false;

  const sc = await prisma.stockCount.findUnique({
    where: { id: stockCountId },
    select: { createdAt: true },
  });
  if (!sc) return false;

  return sc.createdAt.getTime() <= closedUntil.getTime();
}

export async function ensureStockCountNotLocked(stockCountId: string) {
  if (await isStockCountLocked(stockCountId)) {
    const closedUntil = await getClosedUntilEOD();
    throw Object.assign(
      new Error(
        `Phiếu kiểm kê thuộc kỳ đã khoá đến ${closedUntil ? fmtDateVN(closedUntil) : ""}, không được phép sửa/xoá/post.`
      ),
      { status: 400, statusCode: 400 }
    );
  }
}

export async function isStockCountLineLocked(lineId: string): Promise<boolean> {
  const closedUntil = await getClosedUntilEOD();
  if (!closedUntil) return false;

  const line = await prisma.stockCountLine.findUnique({
    where: { id: lineId },
    include: { stockCount: { select: { createdAt: true } } },
  });
  if (!line || !line.stockCount) return false;

  return line.stockCount.createdAt.getTime() <= closedUntil.getTime();
}

export async function ensureStockCountLineNotLocked(lineId: string) {
  if (await isStockCountLineLocked(lineId)) {
    const closedUntil = await getClosedUntilEOD();
    throw Object.assign(
      new Error(
        `Dòng kiểm kê thuộc kỳ đã khoá đến ${closedUntil ? fmtDateVN(closedUntil) : ""}, không được phép sửa.`
      ),
      { status: 400, statusCode: 400 }
    );
  }
}
