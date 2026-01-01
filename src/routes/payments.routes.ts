
// src/routes/payments.routes.ts
import { Router } from "express";
import { requireAuth } from "../middlewares/auth";
import {
  createPaymentWithAllocations,
  getPaymentById,
  listPayments,
} from "../services/payments.service";

const r = Router();
r.use(requireAuth);

function getUserId(req: any): string | undefined {
  return req.user?.id || req.userId;
}

function getUserRole(req: any): string | undefined {
  return req.user?.role || req.userRole;
}

function buildAuditMeta(req: any) {
  return {
    ip: req.ip,
    userAgent: req.headers?.["user-agent"],
    path: req.originalUrl || req.url,
    method: req.method,
    params: req.params,
    query: req.query,
  };
}

function buildAuditCtx(req: any) {
  const userId = getUserId(req);
  const userRole = getUserRole(req);
  return userId
    ? {
        userId,
        userRole,
        meta: buildAuditMeta(req),
      }
    : undefined;
}

/**
 * Normalize allocation kind:
 * - Accept both "HOLD" and "WARRANTY_HOLD" as hold kind
 * - Default: "NORMAL"
 */
function normalizeKind(k: any) {
  const v = String(k ?? "NORMAL").toUpperCase();
  if (v === "HOLD" || v === "WARRANTY_HOLD") return "WARRANTY_HOLD";
  return "NORMAL";
}

/**
 * NOTE:
 * - vẫn giữ normalizePaymentType để tương thích legacy
 */
function normalizePaymentType(t: any) {
  const v = String(t ?? "").toUpperCase();
  return v === "PAYMENT" ? "PAYMENT" : "RECEIPT";
}

function isValidDateString(s: any) {
  if (!s) return false;
  const d = new Date(String(s));
  return !isNaN(d.getTime());
}

function nearlyEqual(a: number, b: number, eps = 0.0001) {
  return Math.abs(a - b) <= eps;
}

/**
 * ✅ AUTO-FIX SIGN for allocations by type
 * - RECEIPT: allocations must be >= 0  (thu)
 * - PAYMENT: allocations must be <= 0  (chi)
 *
 * FE có thể gửi dương/âm lẫn lộn -> route sẽ chuẩn hoá để không bị chặn.
 */
function normalizeAllocSign(type: "RECEIPT" | "PAYMENT", amt: number) {
  if (!Number.isFinite(amt) || amt === 0) return 0;
  if (type === "PAYMENT") return amt > 0 ? -amt : amt; // đảm bảo âm
  return amt < 0 ? Math.abs(amt) : amt; // đảm bảo dương
}

r.post("/", async (req, res) => {
  try {
    const userId = getUserId(req);
    const userRole = getUserRole(req);
    const auditCtx = buildAuditCtx(req);

    const body = req.body || {};

    if (!userId) {
      return res.status(401).json({ message: "Chưa đăng nhập." });
    }

    if (!body.partnerId) {
      return res.status(400).json({ message: "Thiếu partnerId." });
    }
    if (!isValidDateString(body.date)) {
      return res.status(400).json({ message: "Ngày (date) không hợp lệ." });
    }

    const amountRaw = Number(body.amount);
    if (!Number.isFinite(amountRaw) || amountRaw === 0) {
      return res
        .status(400)
        .json({ message: "Số tiền phiếu thu/chi không hợp lệ (khác 0)." });
    }

    /**
     * ✅ type:
     * - amountRaw âm => PAYMENT
     * - amountRaw dương => dùng body.type (default RECEIPT)
     *
     * Payment.amount lưu dương (cash), chiều thu/chi nằm ở type + allocations signed.
     */
    const type = (amountRaw < 0 ? "PAYMENT" : normalizePaymentType(body.type)) as
      | "RECEIPT"
      | "PAYMENT";

    // ✅ Payment.amount luôn dương
    const amount = Math.abs(amountRaw);

    const allocations = Array.isArray(body.allocations)
      ? body.allocations
          .map((a: any) => {
            const invoiceId = a?.invoiceId ? String(a.invoiceId) : "";
            const kind = normalizeKind(a?.kind);
            const amtRaw = Number(a?.amount ?? 0);

            // ✅ auto-fix sign theo type
            const amt = normalizeAllocSign(type, amtRaw);

            return {
              invoiceId,
              amount: amt, // signed
              kind,
            };
          })
          .filter((a: any) => {
            if (!a.invoiceId) return false;
            if (!Number.isFinite(a.amount)) return false;
            if (a.amount === 0) return false;
            return true;
          })
      : undefined;

    // ========= quick validations (nhẹ) =========
    if (allocations && allocations.length > 0) {
      // Sau normalizeAllocSign thì 2 check này gần như luôn pass,
      // nhưng giữ lại để bắt case input NaN/0 kỳ quặc.
      if (type === "RECEIPT" && allocations.some((x: any) => x.amount < 0)) {
        return res.status(400).json({
          message:
            "Phiếu THU (RECEIPT) không được có phân bổ âm. Nếu hoàn tiền hãy dùng phiếu CHI (PAYMENT).",
        });
      }

      if (type === "PAYMENT" && allocations.some((x: any) => x.amount > 0)) {
        return res.status(400).json({
          message:
            "Phiếu CHI (PAYMENT) không được có phân bổ dương. Nếu là thu tiền hãy dùng phiếu THU (RECEIPT).",
        });
      }

      // ✅ check tổng tiền theo “cash amount”
      if (type === "RECEIPT") {
        const expected = allocations.reduce(
          (s: number, x: any) => s + (Number(x.amount) || 0),
          0
        );
        if (!nearlyEqual(expected, amount)) {
          return res.status(400).json({
            message: `Tổng phân bổ (signed) = ${expected} phải bằng số tiền phiếu = ${amount}.`,
          });
        }
      } else {
        const expected = allocations.reduce(
          (s: number, x: any) => s + Math.abs(Number(x.amount) || 0),
          0
        );
        if (!nearlyEqual(expected, amount)) {
          return res.status(400).json({
            message: `Tổng phân bổ (sum abs allocations) = ${expected} phải bằng số tiền phiếu = ${amount}.`,
          });
        }
      }
    }

    const payment = await createPaymentWithAllocations(
      {
        date: body.date,
        partnerId: body.partnerId,
        type,
        amount, // dương
        accountId: body.accountId || undefined,
        method: body.method,
        refNo: body.refNo,
        note: body.note,
        allocations,
        createdById: userId,
      },
      auditCtx ?? { userId, userRole, meta: buildAuditMeta(req) }
    );

    res.json(payment);
  } catch (err: any) {
    console.error(err);
    res
      .status(400)
      .json({ message: err.message || "Không tạo được phiếu thanh toán" });
  }
});

r.get("/", async (req, res, next) => {
  try {
    const data = await listPayments({
      partnerId: req.query.partnerId as string | undefined,
      type: req.query.type as any,
      accountId: req.query.accountId as string | undefined,
      from: req.query.from as string | undefined,
      to: req.query.to as string | undefined,
    });
    res.json(data);
  } catch (err) {
    next(err);
  }
});

r.get("/:id", async (req, res, next) => {
  try {
    const payment = await getPaymentById(req.params.id);
    if (!payment) {
      return res.status(404).json({ message: "Không tìm thấy phiếu" });
    }
    res.json(payment);
  } catch (err) {
    next(err);
  }
});

export default r;
