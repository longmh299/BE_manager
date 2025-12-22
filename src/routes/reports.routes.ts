// src/routes/reports.routes.ts
import { Router } from "express";
import { requireAuth, getUser } from "../middlewares/auth";
import { exportLedgerExcel, getLedger, getSalesLedger, exportSalesLedgerExcel } from "../services/reports.service";
import { MovementType, PaymentStatus } from "@prisma/client";

export const reportsRouter = Router();

function httpError(statusCode: number, message: string) {
  const err: any = new Error(message);
  err.statusCode = statusCode;
  return err;
}

function requireNotStaff(req: any) {
  const u = getUser(req)!;
  if (u.role === "staff") throw httpError(403, "Bạn không có quyền xem báo cáo.");
  return u;
}

function parseDateParam(v: any, endOfDay = false): Date | undefined {
  if (!v) return undefined;
  const s = String(v).trim();
  if (!s) return undefined;

  // yyyy-mm-dd
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    const d = new Date(s + "T00:00:00.000Z");
    if (Number.isNaN(d.getTime())) return undefined;
    if (endOfDay) d.setUTCHours(23, 59, 59, 999);
    return d;
  }

  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return undefined;
  if (endOfDay) d.setUTCHours(23, 59, 59, 999);
  return d;
}

function parseMovementType(v: any): MovementType | undefined {
  if (!v) return undefined;
  const s = String(v);
  if (s === "IN" || s === "OUT" || s === "TRANSFER" || s === "ADJUST") return s;
  return undefined;
}

function parsePaymentStatus(v: any): PaymentStatus | undefined {
  if (!v) return undefined;
  const s = String(v);
  if (s === "UNPAID" || s === "PARTIAL" || s === "PAID") return s as PaymentStatus;
  return undefined;
}

reportsRouter.use(requireAuth);

/**
 * GET /api/reports/ledger
 * staff: forbidden
 * accountant/admin: ok
 */
reportsRouter.get("/ledger", async (req, res, next) => {
  try {
    requireNotStaff(req);

    const { from = "", to = "", q = "", itemId = "", type = "" } = req.query as any;

    const data = await getLedger({
      from: parseDateParam(from, false),
      to: parseDateParam(to, true), // ✅ end-of-day
      q: q ? String(q) : undefined,
      itemId: itemId ? String(itemId) : undefined,
      type: parseMovementType(type),
    });

    res.json({ ok: true, data });
  } catch (e) {
    next(e);
  }
});

/**
 * GET /api/reports/ledger.xlsx
 * staff: forbidden
 * accountant/admin: ok
 */
reportsRouter.get("/ledger.xlsx", async (req, res, next) => {
  try {
    requireNotStaff(req);

    const { from = "", to = "", q = "", itemId = "", type = "" } = req.query as any;

    const buf = await exportLedgerExcel({
      from: parseDateParam(from, false),
      to: parseDateParam(to, true), // ✅ end-of-day (đồng bộ với API json)
      q: q ? String(q) : undefined,
      itemId: itemId ? String(itemId) : undefined,
      type: parseMovementType(type),
    });

    const now = new Date();
    const y = now.getFullYear();
    const m = String(now.getMonth() + 1).padStart(2, "0");
    const d = String(now.getDate()).padStart(2, "0");

    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="lich_su_xuat_nhap_${y}${m}${d}.xlsx"`);
    res.send(buf);
  } catch (e) {
    next(e);
  }
});

/**
 * GET /api/reports/sales-ledger
 * staff: forbidden
 * accountant/admin: ok
 *
 * query:
 * - from, to (yyyy-mm-dd)
 * - q (search code/khách/sản phẩm)
 * - saleUserId, techUserId
 * - paymentStatus: UNPAID|PARTIAL|PAID
 */
reportsRouter.get("/sales-ledger", async (req, res, next) => {
  try {
    requireNotStaff(req);

    const {
      from = "",
      to = "",
      q = "",
      saleUserId = "",
      techUserId = "",
      paymentStatus = "",
    } = req.query as any;

    const data = await getSalesLedger({
      from: parseDateParam(from, false),
      to: parseDateParam(to, true), // ✅ end-of-day
      q: q ? String(q) : undefined,
      saleUserId: saleUserId ? String(saleUserId) : undefined,
      techUserId: techUserId ? String(techUserId) : undefined,
      paymentStatus: parsePaymentStatus(paymentStatus),
    });

    res.json({ ok: true, data });
  } catch (e) {
    next(e);
  }
});

/**
 * GET /api/reports/sales-ledger.xlsx
 * staff: forbidden
 * accountant/admin: ok
 */
reportsRouter.get("/sales-ledger.xlsx", async (req, res, next) => {
  try {
    requireNotStaff(req);

    const {
      from = "",
      to = "",
      q = "",
      saleUserId = "",
      techUserId = "",
      paymentStatus = "",
    } = req.query as any;

    const buf = await exportSalesLedgerExcel({
      from: parseDateParam(from, false),
      to: parseDateParam(to, true), // ✅ end-of-day
      q: q ? String(q) : undefined,
      saleUserId: saleUserId ? String(saleUserId) : undefined,
      techUserId: techUserId ? String(techUserId) : undefined,
      paymentStatus: parsePaymentStatus(paymentStatus),
    });

    const now = new Date();
    const y = now.getFullYear();
    const m = String(now.getMonth() + 1).padStart(2, "0");
    const d = String(now.getDate()).padStart(2, "0");

    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="bang_ke_ban_${y}${m}${d}.xlsx"`);
    res.send(buf);
  } catch (e) {
    next(e);
  }
});
