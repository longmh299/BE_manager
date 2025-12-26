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
    // thêm chút context để trace (nhẹ)
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
 * - nhưng phía dưới sẽ có "auto-fix" theo dấu của amountRaw để tránh nhập âm mà vẫn THU
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
     * ✅ FIX: type phải khớp chiều tiền
     * - Nếu user nhập amount âm => auto chuyển thành PAYMENT (phiếu CHI)
     * - Nếu user nhập amount dương => giữ normalizePaymentType(body.type) (default RECEIPT)
     *
     * Lý do: route luôn dùng amount dương (cash amount), còn chiều thu/chi nằm ở type + allocations signed.
     */
    const type = amountRaw < 0 ? "PAYMENT" : normalizePaymentType(body.type);

    // ✅ Payment.amount luôn dương (tiền thực thu/chi)
    const amount = Math.abs(amountRaw);

    const allocations = Array.isArray(body.allocations)
      ? body.allocations
          .map((a: any) => {
            const invoiceId = a?.invoiceId ? String(a.invoiceId) : "";
            const kind = normalizeKind(a?.kind);
            const amt = Number(a?.amount ?? 0);

            return {
              invoiceId,
              amount: amt, // ✅ signed (RECEIPT +, PAYMENT -)
              kind,
            };
          })
          .filter((a: any) => {
            if (!a.invoiceId) return false;
            if (!Number.isFinite(a.amount)) return false;
            if (a.amount === 0) return false;

            // ✅ IMPORTANT:
            // HOLD/WARRANTY_HOLD cho phép signed. (RECEIPT dương, PAYMENT âm) -> service sẽ validate theo type.
            return true;
          })
      : undefined;

    // ========= quick validations ở route (nhẹ, không ép kind) =========
    // Mục tiêu: bắt lỗi obvious (sign sai), còn lại để service validate/cap.
    if (allocations && allocations.length > 0) {
      // RECEIPT: mọi allocation phải >= 0
      if (type === "RECEIPT" && allocations.some((x: any) => x.amount < 0)) {
        return res.status(400).json({
          message:
            "Phiếu THU (RECEIPT) không được có phân bổ âm. Nếu hoàn tiền hãy dùng phiếu CHI (PAYMENT).",
        });
      }

      // PAYMENT: mọi allocation phải <= 0
      if (type === "PAYMENT" && allocations.some((x: any) => x.amount > 0)) {
        return res.status(400).json({
          message:
            "Phiếu CHI (PAYMENT) không được có phân bổ dương. Nếu là thu tiền hãy dùng phiếu THU (RECEIPT).",
        });
      }

      // ✅ check tổng tiền theo “cash amount”
      // - RECEIPT: sum(signed allocations) == amount
      // - PAYMENT: sum(abs(allocation.amount)) == amount
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
        amount, // ✅ dương
        accountId: body.accountId || undefined,
        method: body.method,
        refNo: body.refNo,
        note: body.note,
        allocations,
        createdById: userId,
      },
      // ✅ audit context
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
