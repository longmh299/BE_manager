// src/services/periodLock.service.ts
import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

export const VN_TZ = "Asia/Ho_Chi_Minh";

/**
 * ✅ Role-based rolling period lock (theo NGÀY VN)
 * - admin: 90 ngày
 * - accountant: 7 ngày
 * - default (role khác / thiếu): 7 ngày
 */
export const ADMIN_LOCK_DAYS = 90;
export const ACCOUNTANT_LOCK_DAYS = 7;
export const DEFAULT_LOCK_DAYS = 7; // đổi thành 90 nếu muốn user thường cũng 90

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
 * Convert 'YYYY-MM-DD' (ngày VN) -> Date tại 00:00 VN (ISO +07:00)
 * => Date object (UTC nội bộ) nhưng đại diện đúng mốc 00:00 VN.
 */
function vnMidnightToUTC(ymd: string) {
  return new Date(`${ymd}T00:00:00+07:00`);
}

/**
 * Cộng/trừ ngày trên trục NGÀY VN một cách ổn định:
 * - input: ymd 'YYYY-MM-DD'
 * - deltaDays: +/- N
 * - output: ymd 'YYYY-MM-DD' (VN)
 */
function addDaysYMD_VN(ymd: string, deltaDays: number) {
  const d = new Date(`${ymd}T00:00:00+07:00`);
  d.setDate(d.getDate() + deltaDays);
  return fmtDateVN(d);
}

function todayYMDVN(now = new Date()) {
  return fmtDateVN(now);
}

function normalizeRole(role?: string | null) {
  return String(role || "").trim().toLowerCase();
}

function lockDaysForRole(role?: string | null) {
  const r = normalizeRole(role);
  if (r === "admin") return ADMIN_LOCK_DAYS;
  if (r === "accountant") return ACCOUNTANT_LOCK_DAYS;
  return DEFAULT_LOCK_DAYS;
}

/* =========================================================
   ✅ Rule: Rolling lock (source of truth)
   Mốc khoá = 00:00 VN của (todayVN - lockDays)
========================================================= */

/**
 * ✅ lockedUntil = 00:00 VN của (todayVN - lockDays)
 * Ví dụ: hôm nay (VN) 2026-02-02, admin(90)
 * => lockedUntilYMD = 2025-11-04
 * => lockedUntilDate = 2025-11-04 00:00 (VN)
 */
function rollingLockedUntil(now = new Date(), userRole?: string | null) {
  const days = lockDaysForRole(userRole);
  const todayVN = todayYMDVN(now);
  const lockedYMD = addDaysYMD_VN(todayVN, -days);
  return { days, lockedYMD, lockedDate: vnMidnightToUTC(lockedYMD) };
}

/**
 * ✅ So lock theo NGÀY VN (YYYY-MM-DD) để tránh lệch do giờ/UTC:
 * LOCK nếu invYMD <= lockedYMD
 */
function isLockedByRollingWindow(date: Date, now = new Date(), userRole?: string | null) {
  const invYMD = fmtDateVN(date);
  const { lockedYMD } = rollingLockedUntil(now, userRole);
  return invYMD <= lockedYMD;
}

/**
 * Generic date check (rolling lock theo role)
 */
export async function isDateLocked(date: Date, userRole?: string | null): Promise<boolean> {
  if (!date) return false;
  return isLockedByRollingWindow(date, new Date(), userRole);
}

export async function ensureDateNotLocked(
  date: Date,
  actionLabel: string,
  userRole?: string | null
) {
  if (await isDateLocked(date, userRole)) {
    const { days, lockedYMD } = rollingLockedUntil(new Date(), userRole);
    throw Object.assign(
      new Error(
        `Chứng từ quá hạn chỉnh sửa (${days} ngày). ` +
          `Mốc khoá (VN): ${lockedYMD}. ` +
          `Ngày chứng từ: ${fmtDateVN(date)}. Không được phép ${actionLabel}.`
      ),
      { status: 400, statusCode: 400 }
    );
  }
}

/**
 * ✅ Ensure date thuộc THÁNG HIỆN TẠI theo giờ VN
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

/* =========================================================
   Legacy DB-based period lock (kept for compatibility / UI)
   NOTE: Not used to determine lock anymore.
========================================================= */

export async function getClosedUntil(): Promise<Date | null> {
  const row = await prisma.periodLock.findFirst({
    orderBy: { closedUntil: "desc" },
  });
  return row?.closedUntil ?? null;
}

async function getClosedUntilEOD(): Promise<Date | null> {
  return getClosedUntil();
}

/* =========================================================
   ✅ Movement lock (giữ rolling 90 ngày như bản cũ, KHÔNG theo role)
========================================================= */

// Nếu m muốn movement cũng theo 90/7 thì nói, còn hiện tại giữ nguyên 90 như logic cũ.
export const PERIOD_LOCK_DAYS = 90;

function rollingLockedUntil90(now = new Date()) {
  const todayVN = todayYMDVN(now);
  const lockedYMD = addDaysYMD_VN(todayVN, -PERIOD_LOCK_DAYS);
  return { lockedYMD, lockedDate: vnMidnightToUTC(lockedYMD) };
}

function isLockedByRollingWindow90(date: Date, now = new Date()) {
  const invYMD = fmtDateVN(date);
  const { lockedYMD } = rollingLockedUntil90(now);
  return invYMD <= lockedYMD;
}

export async function isMovementLocked(movementId: string): Promise<boolean> {
  const mv = await prisma.movement.findUnique({
    where: { id: movementId },
    select: { createdAt: true, occurredAt: true },
  });
  if (!mv) return false;

  const effective = mv.occurredAt ?? mv.createdAt;
  return isLockedByRollingWindow90(effective, new Date());
}

export async function isMovementLineLocked(lineId: string): Promise<boolean> {
  const line = await prisma.movementLine.findUnique({
    where: { id: lineId },
    select: { movement: { select: { createdAt: true, occurredAt: true } } },
  });
  if (!line || !line.movement) return false;

  const effective = line.movement.occurredAt ?? line.movement.createdAt;
  return isLockedByRollingWindow90(effective, new Date());
}

export async function ensureMovementNotLocked(movementId: string) {
  if (await isMovementLocked(movementId)) {
    const { lockedYMD } = rollingLockedUntil90(new Date());
    throw Object.assign(
      new Error(
        `Chứng từ quá hạn chỉnh sửa (${PERIOD_LOCK_DAYS} ngày). ` +
          `Mốc khoá (VN): ${lockedYMD}. Không được phép sửa/xoá/post.`
      ),
      { status: 400, statusCode: 400 }
    );
  }
}

export async function ensureMovementLineNotLocked(lineId: string) {
  if (await isMovementLineLocked(lineId)) {
    const { lockedYMD } = rollingLockedUntil90(new Date());
    throw Object.assign(
      new Error(
        `Dòng chứng từ quá hạn chỉnh sửa (${PERIOD_LOCK_DAYS} ngày). ` +
          `Mốc khoá (VN): ${lockedYMD}. Không được phép sửa/xoá.`
      ),
      { status: 400, statusCode: 400 }
    );
  }
}

/* =========================================================
   ✅ StockCount locks (giữ rolling 90 ngày như bản cũ)
========================================================= */

export async function isStockCountLocked(stockCountId: string): Promise<boolean> {
  const sc = await prisma.stockCount.findUnique({
    where: { id: stockCountId },
    select: { createdAt: true },
  });
  if (!sc) return false;

  return isLockedByRollingWindow90(sc.createdAt, new Date());
}

export async function ensureStockCountNotLocked(stockCountId: string) {
  if (await isStockCountLocked(stockCountId)) {
    const { lockedYMD } = rollingLockedUntil90(new Date());
    throw Object.assign(
      new Error(
        `Phiếu kiểm kê quá hạn chỉnh sửa (${PERIOD_LOCK_DAYS} ngày). ` +
          `Mốc khoá (VN): ${lockedYMD}. Không được phép sửa/xoá/post.`
      ),
      { status: 400, statusCode: 400 }
    );
  }
}

export async function isStockCountLineLocked(lineId: string): Promise<boolean> {
  const line = await prisma.stockCountLine.findUnique({
    where: { id: lineId },
    select: { stockCount: { select: { createdAt: true } } },
  });
  if (!line || !line.stockCount) return false;

  return isLockedByRollingWindow90(line.stockCount.createdAt, new Date());
}

export async function ensureStockCountLineNotLocked(lineId: string) {
  if (await isStockCountLineLocked(lineId)) {
    const { lockedYMD } = rollingLockedUntil90(new Date());
    throw Object.assign(
      new Error(
        `Dòng kiểm kê quá hạn chỉnh sửa (${PERIOD_LOCK_DAYS} ngày). ` +
          `Mốc khoá (VN): ${lockedYMD}. Không được phép sửa.`
      ),
      { status: 400, statusCode: 400 }
    );
  }
}

/* =========================================================
   Optional: expose rolling locked until for UI/debug
========================================================= */

export function getRollingLockedUntilDate(now = new Date(), userRole?: string | null) {
  return rollingLockedUntil(now, userRole).lockedDate;
}

// (optional) kept for compatibility (if any old code imports it)
export { getClosedUntilEOD };
