// src/routes/invoices.routes.ts
import { Router } from "express";
import { requireAuth, requireRole, getUser } from "../middlewares/auth";
import {
  listInvoices,
  getInvoiceById,
  createInvoice,
  updateInvoice,
  deleteInvoice,
  postInvoiceToStock,
  unpostInvoiceStock,
  updateInvoiceNote,
  approveInvoice,
  rejectInvoice,
  submitInvoice,
  recallInvoice,
  // ✅ NEW: admin reopen approved -> draft (service must implement)
  reopenApprovedInvoice,
  adminEditApprovedInvoiceInPlace,
  adminSaveAndPostInvoice,

} from "../services/invoices.service";

// ✅ NEW: limit edits to current month VN
import { ensureDateInCurrentMonthVN } from "../services/periodLock.service";

const r = Router();

/** ========================= AUDIT HELPERS ========================= **/

function buildAuditMeta(req: any) {
  return {
    ip: req.ip,
    userAgent: req.headers?.["user-agent"],
    path: req.originalUrl || req.url,
    method: req.method,
  };
}

/** ========================= RETURN META (BE computed) ========================= **/

function toNum(v: any) {
  if (v == null) return 0;
  const n = typeof v === "number" ? v : Number(String(v));
  return Number.isFinite(n) ? n : 0;
}

function roundMoney(n: number) {
  return Math.round((n + Number.EPSILON) * 100) / 100;
}

/**
 * ✅ returnMeta giúp FE render & filter lâu dài
 * - state:
 *   - FULL: (status=CANCELLED) OR (netTotal<=0 && returnedTotal>0)
 *   - PARTIAL: returnedTotal>0 && netTotal>0
 *   - NONE: returnedTotal<=0
 *
 * - debtIgnore: FULL => true
 * - collectible: max(0, baseTotal - holdAmount)
 *   baseTotal: netTotal fallback total
 *   holdAmount: warrantyHoldAmount (đã recompute theo netSubtotal ở service)
 */
function computeReturnMeta(inv: any) {
  const total = roundMoney(toNum(inv.total));
  const netTotal =
    inv.netTotal != null && toNum(inv.netTotal) >= 0 ? roundMoney(toNum(inv.netTotal)) : total;

  const returnedTotal =
    inv.returnedTotal != null && toNum(inv.returnedTotal) >= 0
      ? roundMoney(toNum(inv.returnedTotal))
      : 0;

  const baseSubtotal =
    inv.netSubtotal != null && toNum(inv.netSubtotal) >= 0
      ? roundMoney(toNum(inv.netSubtotal))
      : inv.subtotal != null
      ? roundMoney(toNum(inv.subtotal))
      : 0;

  let holdAmount = 0;
  if (inv.hasWarrantyHold === true) {
    holdAmount = roundMoney(toNum(inv.warrantyHoldAmount));
    if (holdAmount < 0) holdAmount = 0;
    if (holdAmount > baseSubtotal) holdAmount = baseSubtotal;
  }

  const collectible = Math.max(0, roundMoney(netTotal - holdAmount));

  let state: "NONE" | "PARTIAL" | "FULL" = "NONE";
  if (String(inv.status) === "CANCELLED") state = "FULL";
  else if (returnedTotal > 0 && netTotal <= 0.0001) state = "FULL";
  else if (returnedTotal > 0 && netTotal > 0.0001) state = "PARTIAL";

  const debtIgnore = state === "FULL";

  return {
    state,
    debtIgnore,
    returnedTotal,
    netTotal,
    holdAmount,
    collectible,
  };
}

/** ========================= SANITIZE (ẩn giá vốn theo role) ========================= **/

function sanitizeInvoiceForRole(inv: any, role: string) {
  if (!inv) return inv;

  // admin + accountant mới được xem giá vốn
  const canSeeCost = role === "admin" || role === "accountant";
  const hideItemPrice = role === "staff";

  const cloned = JSON.parse(JSON.stringify(inv));

  // ✅ attach returnMeta (BE computed)
  try {
    cloned.returnMeta = computeReturnMeta(cloned);
  } catch {
    cloned.returnMeta = undefined;
  }

  // invoice level
  if (!canSeeCost) {
    delete cloned.totalCost;
  }

  // invoice lines
  if (Array.isArray(cloned.lines)) {
    for (const l of cloned.lines) {
      if (!canSeeCost) {
        delete l.unitCost;
        delete l.costTotal;
      }
      if (l.item && hideItemPrice) {
        delete l.item.price; // giá vốn gốc item
      }
    }
  }

  // movements
  if (Array.isArray(cloned.movements)) {
    for (const mv of cloned.movements) {
      if (Array.isArray(mv.lines) && !canSeeCost) {
        for (const ml of mv.lines) {
          delete ml.unitCost;
          delete ml.costTotal;
        }
      }
    }
  }

  // warranty fields: staff được xem
  return cloned;
}

function sanitizeInvoiceListForRole(rows: any[], role: string) {
  return rows.map((x) => sanitizeInvoiceForRole(x, role));
}

/** ========================= HELPERS (RBAC) ========================= **/

async function mustGetInvoice(id: string) {
  const inv = await getInvoiceById(id);
  if (!inv) {
    const err: any = new Error("Invoice not found");
    err.statusCode = 404;
    throw err;
  }
  return inv;
}

function httpError(statusCode: number, message: string) {
  const err: any = new Error(message);
  err.statusCode = statusCode;
  return err;
}

function isStaff(role: string) {
  return role === "staff";
}

function isAdmin(role: string) {
  return role === "admin";
}

function ensureNoPurchaseReturnAccessOrThrow(invOrType: any, role: string) {
  const t = typeof invOrType === "string" ? invOrType : invOrType?.type;
  if (t === "PURCHASE_RETURN" && !isAdmin(role)) {
    throw httpError(403, "Xuất trả NCC chỉ dành cho ADMIN.");
  }
}

function ensureStaffOwnInvoiceOrThrow(inv: any, userId: string) {
  if (!inv.saleUserId || String(inv.saleUserId) !== String(userId)) {
    throw httpError(403, "Bạn không có quyền truy cập hóa đơn này.");
  }
}

function ensureDraftForStaffOrThrow(inv: any) {
  if (inv.status !== "DRAFT") {
    throw httpError(409, "Hóa đơn không còn là NHÁP nên không thể thao tác.");
  }
}

function ensureSubmittedForRecallOrThrow(inv: any) {
  if (inv.status !== "SUBMITTED") {
    throw httpError(409, "Chỉ có thể HỦY GỬI DUYỆT khi hóa đơn đang ở trạng thái CHỜ DUYỆT.");
  }
}

/**
 * ✅ NEW: Rule “chỉ cho phép sửa trong tháng”
 * - áp dụng cho các hành động thay đổi dữ liệu (edit/delete/submit/approve/reopen...)
 * - ưu tiên issueDate; fallback createdAt nếu thiếu (tránh crash)
 */
async function ensureInvoiceInCurrentMonthOrThrow(inv: any, actionLabel: string) {
  const d = inv?.issueDate ? new Date(inv.issueDate) : inv?.createdAt ? new Date(inv.createdAt) : null;
  if (!d) return; // nếu thiếu cả 2 thì bỏ qua (hiếm)
  await ensureDateInCurrentMonthVN(d, actionLabel);
}

/** ========================= ROUTES ========================= **/

r.use(requireAuth);

/**
 * GET /invoices
 */
r.get("/", async (req, res, next) => {
  try {
    const u = getUser(req)!;

    const {
      q = "",
      page = "1",
      pageSize = "20",
      type = "",
      saleUserId = "",
      techUserId = "",
      from = "",
      to = "",
      status = "",
      paymentStatus = "",
      receiveAccountId = "",
    } = req.query as any;

    if (String(type) === "PURCHASE_RETURN" && !isAdmin(u.role)) {
      throw httpError(403, "Xuất trả NCC chỉ dành cho ADMIN.");
    }

    const pageNum = Number(page) || 1;
    const sizeNum = Number(pageSize) || 20;

    const filter: any = {};
    if (type) filter.type = type;

    if (!isAdmin(u.role)) {
      filter.excludeTypes = ["PURCHASE_RETURN"];
    }

    if (isStaff(u.role)) {
      filter.saleUserId = u.id;
    } else {
      if (saleUserId) filter.saleUserId = saleUserId;
    }

    if (techUserId) filter.techUserId = techUserId;
    if (from) filter.from = new Date(from);
    if (to) filter.to = new Date(to);
    if (status) filter.status = status;
    if (paymentStatus) filter.paymentStatus = paymentStatus;
    if (receiveAccountId) filter.receiveAccountId = String(receiveAccountId);

    const result = await listInvoices(q ? String(q) : undefined, pageNum, sizeNum, filter);

    res.json({
      ok: true,
      data: sanitizeInvoiceListForRole(result.data, u.role),
      total: result.total,
      page: result.page,
      pageSize: result.pageSize,
    });
  } catch (e) {
    next(e);
  }
});

/**
 * GET /invoices/:id
 */
r.get("/:id", async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const inv = await mustGetInvoice(req.params.id);

    ensureNoPurchaseReturnAccessOrThrow(inv, u.role);

    if (isStaff(u.role)) {
      ensureStaffOwnInvoiceOrThrow(inv, u.id);
    }

    res.json({
      ok: true,
      data: sanitizeInvoiceForRole(inv, u.role),
    });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});

/**
 * POST /invoices
 */
r.post("/", async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const body = { ...(req.body || {}) };

    if (String(body.type || "") === "PURCHASE_RETURN" && !isAdmin(u.role)) {
      throw httpError(403, "Xuất trả NCC chỉ dành cho ADMIN.");
    }

    if (isStaff(u.role)) {
      body.saleUserId = u.id;

      // ✅ staff: cho tick bảo hành nhưng ép % = 5 nếu có
      if (body.hasWarrantyHold === true) body.warrantyHoldPct = 5;
      if (body.warrantyHoldPct !== undefined) delete body.warrantyHoldPct; // staff không tự set %
      if (body.hasWarrantyHold !== true) {
        body.hasWarrantyHold = false;
        body.warrantyHoldPct = 0;
      }
    }

    const data = await createInvoice(body, {
      userId: u.id,
      userRole: u.role,
      meta: buildAuditMeta(req),
    });

    res.json({ ok: true, data: sanitizeInvoiceForRole(data, u.role) });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});

/**
 * ✅ NEW: POST /invoices/:id/reopen
 * - ADMIN mở lại hoá đơn đã APPROVED (rollback stock + xoá movement) -> DRAFT để sửa
 * - Rule: chỉ trong THÁNG HIỆN TẠI (VN)
 */
r.post("/:id/reopen", requireRole("admin"), async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const id = req.params.id;

    const inv = await mustGetInvoice(id);
    ensureNoPurchaseReturnAccessOrThrow(inv, u.role);

    // ✅ chỉ trong tháng
    await ensureInvoiceInCurrentMonthOrThrow(inv, "mở lại để sửa hóa đơn");

    const data = await reopenApprovedInvoice(
      { invoiceId: id, actorId: u.id },
      { userId: u.id, userRole: u.role, meta: buildAuditMeta(req) }
    );

    res.json({
      ok: true,
      data: sanitizeInvoiceForRole(data, u.role),
    });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});

/**
 * PUT /invoices/:id
 */
r.put("/:id", async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const id = req.params.id;

    const inv = await mustGetInvoice(id);
    ensureNoPurchaseReturnAccessOrThrow(inv, u.role);

    if (req.body?.type === "PURCHASE_RETURN" && !isAdmin(u.role)) {
      throw httpError(403, "Xuất trả NCC chỉ dành cho ADMIN.");
    }

    // ✅ rule: chỉ trong tháng (mọi thao tác update)
    await ensureInvoiceInCurrentMonthOrThrow(inv, "sửa hóa đơn");

    const body = { ...(req.body || {}) };

    if (isStaff(u.role)) {
      ensureStaffOwnInvoiceOrThrow(inv, u.id);
      ensureDraftForStaffOrThrow(inv);

      // ✅ staff: cho tick bảo hành nhưng ép % = 5 nếu có
      if (body.hasWarrantyHold === true) body.warrantyHoldPct = 5;
      if (body.warrantyHoldPct !== undefined) delete body.warrantyHoldPct; // staff không tự set %
      if (body.hasWarrantyHold === false) {
        body.warrantyHoldPct = 0;
      }

      const data = await updateInvoice(id, body, {
        userId: u.id,
        userRole: u.role,
        meta: buildAuditMeta(req),
      });

      return res.json({ ok: true, data: sanitizeInvoiceForRole(data, u.role) });
    }

    const data = await updateInvoice(id, body, {
      userId: u.id,
      userRole: u.role,
      meta: buildAuditMeta(req),
    });

    res.json({ ok: true, data: sanitizeInvoiceForRole(data, u.role) });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});

/**
 * PATCH /invoices/:id/note
 */
r.patch("/:id/note", async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const id = req.params.id;
    const note = String(req.body?.note ?? "");

    const inv = await mustGetInvoice(id);
    ensureNoPurchaseReturnAccessOrThrow(inv, u.role);

    // ✅ rule: chỉ trong tháng
    await ensureInvoiceInCurrentMonthOrThrow(inv, "sửa ghi chú hóa đơn");

    if (isStaff(u.role)) {
      ensureStaffOwnInvoiceOrThrow(inv, u.id);
      ensureDraftForStaffOrThrow(inv);
    }

    const data = await updateInvoiceNote(id, note, {
      userId: u.id,
      userRole: u.role,
      meta: buildAuditMeta(req),
    });

    res.json({ ok: true, data });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});

r.patch("/:id/payment", requireRole("accountant", "admin"), async (_req, res) => {
  return res.status(409).json({
    ok: false,
    message:
      "Route /invoices/:id/payment đã deprecated. Vui lòng tạo phiếu thu/chi qua POST /payments (kèm allocations) để thanh toán/thu nợ. Invoice sẽ tự cập nhật paidAmount/paymentStatus theo allocations.",
  });
});

r.post("/:id/submit", async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const id = req.params.id;

    const inv = await mustGetInvoice(id);
    ensureNoPurchaseReturnAccessOrThrow(inv, u.role);

    // ✅ rule: chỉ trong tháng (vì submit cũng là “chốt gửi duyệt”)
    await ensureInvoiceInCurrentMonthOrThrow(inv, "gửi duyệt hóa đơn");

    if (isStaff(u.role)) {
      ensureStaffOwnInvoiceOrThrow(inv, u.id);
      ensureDraftForStaffOrThrow(inv);
    }

    const data = await submitInvoice(
      { invoiceId: id, submittedById: u.id },
      { userId: u.id, userRole: u.role, meta: buildAuditMeta(req) }
    );

    res.json({
      ok: true,
      data: sanitizeInvoiceForRole(data, u.role),
    });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});

r.post("/:id/recall", async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const id = req.params.id;

    const inv = await mustGetInvoice(id);
    ensureNoPurchaseReturnAccessOrThrow(inv, u.role);

    // ✅ rule: chỉ trong tháng (thu hồi gửi duyệt)
    await ensureInvoiceInCurrentMonthOrThrow(inv, "thu hồi gửi duyệt hóa đơn");

    if (isStaff(u.role)) {
      ensureStaffOwnInvoiceOrThrow(inv, u.id);
      ensureSubmittedForRecallOrThrow(inv);
    }

    const data = await recallInvoice(
      { invoiceId: id, actorId: u.id },
      { userId: u.id, userRole: u.role, meta: buildAuditMeta(req) }
    );

    res.json({
      ok: true,
      data: sanitizeInvoiceForRole(data, u.role),
    });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});

r.post("/:id/approve", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const u = getUser(req)!;

    const inv = await mustGetInvoice(req.params.id);
    if (inv.type === "PURCHASE_RETURN" && !isAdmin(u.role)) {
      throw httpError(403, "Xuất trả NCC chỉ ADMIN mới được duyệt.");
    }

    // ✅ rule: chỉ trong tháng (approve là post tồn)
    await ensureInvoiceInCurrentMonthOrThrow(inv, "duyệt hóa đơn");

    const { warehouseId } = req.body || {};

    const data = await approveInvoice(
      {
        invoiceId: req.params.id,
        approvedById: u.id,
        warehouseId: warehouseId ? String(warehouseId) : undefined,
      },
      { userId: u.id, userRole: u.role, meta: buildAuditMeta(req) }
    );

    res.json({
      ok: true,
      data: sanitizeInvoiceForRole(data, u.role),
    });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});

r.post("/:id/reject", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const u = getUser(req)!;

    const inv = await mustGetInvoice(req.params.id);
    if (inv.type === "PURCHASE_RETURN" && !isAdmin(u.role)) {
      throw httpError(403, "Xuất trả NCC chỉ ADMIN mới được từ chối.");
    }

    // ✅ rule: chỉ trong tháng
    await ensureInvoiceInCurrentMonthOrThrow(inv, "từ chối hóa đơn");

    const { reason } = req.body || {};

    const data = await rejectInvoice(
      {
        invoiceId: req.params.id,
        approvedById: u.id,
        reason: reason ? String(reason) : undefined,
      },
      { userId: u.id, userRole: u.role, meta: buildAuditMeta(req) }
    );

    res.json({
      ok: true,
      data: sanitizeInvoiceForRole(data, u.role),
    });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});

r.delete("/:id", async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const id = req.params.id;

    const inv = await mustGetInvoice(id);
    ensureNoPurchaseReturnAccessOrThrow(inv, u.role);

    // ✅ rule: chỉ trong tháng (xoá)
    await ensureInvoiceInCurrentMonthOrThrow(inv, "xóa hóa đơn");

    if (isStaff(u.role)) {
      ensureStaffOwnInvoiceOrThrow(inv, u.id);
      ensureDraftForStaffOrThrow(inv);
    }

    const data = await deleteInvoice(id, { userId: u.id, userRole: u.role, meta: buildAuditMeta(req) });
    res.json({ ok: true, data });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});

r.post("/:id/post", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    // ✅ deprecated function, không truyền auditCtx để khỏi TS error
    const result = await postInvoiceToStock(req.params.id);
    res.json({ ok: true, data: result });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});

r.post("/:id/unpost", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    // ✅ deprecated function, không truyền auditCtx để khỏi TS error
    const result = await unpostInvoiceStock(req.params.id);
    res.json({ ok: true, data: result });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});
/**
 * ✅ NEW: PUT /invoices/:id/admin-edit-approved
 * - ADMIN chỉnh sửa trực tiếp invoice đã APPROVED (in-place)
 * - Cơ chế: rollback movements + update invoice/lines + repost movement mới
 * - Rule: chỉ trong THÁNG TẠO hóa đơn (VN)
 */
r.put("/:id/admin-edit-approved", requireRole("admin"), async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const id = req.params.id;

    const inv = await mustGetInvoice(id);
    ensureNoPurchaseReturnAccessOrThrow(inv, u.role);

    // ✅ chỉ trong tháng tạo
    await ensureInvoiceInCurrentMonthOrThrow(inv, "admin sửa hóa đơn đã duyệt");

    const body = { ...(req.body || {}) };

    const data = await adminEditApprovedInvoiceInPlace(
      { invoiceId: id, actorId: u.id, warehouseId: body.warehouseId ? String(body.warehouseId) : undefined, body },
      { userId: u.id, userRole: u.role, meta: buildAuditMeta(req) }
    );

    res.json({ ok: true, data: sanitizeInvoiceForRole(data, u.role) });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});

/**
 * ✅ NEW: POST /invoices/:id/admin-save-and-post
 * - ADMIN: rollback stock (từ movements cũ) -> update invoice -> approve lại
 * - atomic transaction (1 lần bấm)
 * - Rule: chỉ trong THÁNG HIỆN TẠI (VN)
 */
r.post("/:id/admin-save-and-post", requireRole("admin"), async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const id = req.params.id;

    const inv = await mustGetInvoice(id);
    ensureNoPurchaseReturnAccessOrThrow(inv, u.role);

    // ✅ chỉ trong tháng
    await ensureInvoiceInCurrentMonthOrThrow(inv, "điều chỉnh & post lại hóa đơn");

    const body = { ...(req.body || {}) };

    const data = await adminSaveAndPostInvoice(
      {
        invoiceId: id,
        actorId: u.id,
        updateBody: body,                 // giống payload PUT /invoices/:id
        warehouseId: body.warehouseId,    // nếu FE có truyền
      },
      { userId: u.id, userRole: u.role, meta: buildAuditMeta(req) }
    );

    res.json({ ok: true, data: sanitizeInvoiceForRole(data, u.role) });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});

export default r;
