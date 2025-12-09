// src/routes/invoices.routes.ts
import { Router } from "express";
import { requireAuth, requireRole } from "../middlewares/auth";
import {
  listInvoices,
  getInvoiceById,
  createInvoice,
  updateInvoice,
  deleteInvoice,
  postInvoiceToStock,
  unpostInvoiceStock,
  updateInvoiceNote,

} from "../services/invoices.service";

const r = Router();

// tất cả route hoá đơn yêu cầu đăng nhập
r.use(requireAuth);

/**
 * GET /invoices?q=&page=&pageSize=&type=&saleUserId=&techUserId=&from=&to=
 */
r.get("/", async (req, res, next) => {
  try {
    const {
      q = "",
      page = "1",
      pageSize = "20",
      type = "",
      saleUserId = "",
      techUserId = "",
      from = "",
      to = "",
    } = req.query as any;

    const pageNum = Number(page) || 1;
    const sizeNum = Number(pageSize) || 20;

    const filter: any = {};
    if (type) filter.type = type;
    if (saleUserId) filter.saleUserId = saleUserId;
    if (techUserId) filter.techUserId = techUserId;
    if (from) filter.from = new Date(from);
    if (to) filter.to = new Date(to);

    const result = await listInvoices(
      q ? String(q) : undefined,
      pageNum,
      sizeNum,
      filter
    );

    // FE đang dùng hàm unwrap nên giữ dạng { ok, data }
    res.json({
      ok: true,
      data: result.data,
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
    const data = await getInvoiceById(req.params.id);
    res.json({ ok: true, data });
  } catch (e) {
    next(e);
  }
});

/**
 * POST /invoices
 */
r.post(
  "/",
  requireRole("accountant", "admin"),
  async (req, res, next) => {
    try {
      const data = await createInvoice(req.body);
      res.json({ ok: true, data });
    } catch (e: any) {
      // lỗi business (đã được service gắn statusCode) -> trả JSON luôn, không đẩy lên error middleware
      if (e && (e as any).statusCode) {
        return res
          .status((e as any).statusCode)
          .json({ ok: false, message: e.message });
      }
      next(e);
    }
  }
);

/**
 * PUT /invoices/:id
 *  - update header + CRUD dòng hàng
 */
r.put(
  "/:id",
  requireRole("accountant", "admin"),
  async (req, res, next) => {
    try {
      const data = await updateInvoice(req.params.id, req.body);
      res.json({ ok: true, data });
    } catch (e: any) {
      if (e && (e as any).statusCode) {
        return res
          .status((e as any).statusCode)
          .json({ ok: false, message: e.message });
      }
      next(e);
    }
  }
);

/**
 * DELETE /invoices/:id
 *  - chỉ xóa khi chưa post tồn (không có movement)
 */
r.delete(
  "/:id",
  requireRole("accountant", "admin"),
  async (req, res, next) => {
    try {
      const data = await deleteInvoice(req.params.id);
      res.json({ ok: true, data });
    } catch (e) {
      next(e);
    }
  }
);

/**
 * POST /invoices/:id/post
 *  - ghi tồn kho từ hoá đơn
 */
r.post(
  "/:id/post",
  requireRole("accountant", "admin"),
  async (req, res, next) => {
    try {
      const result = await postInvoiceToStock(req.params.id);
      res.json({ ok: true, data: result });
    } catch (e) {
      next(e);
    }
  }
);

/**
 * POST /invoices/:id/unpost
 *  - HỦY post tồn: rollback tồn kho và xoá movement của hoá đơn
 */
r.post(
  "/:id/unpost",
  requireRole("accountant", "admin"),
  async (req, res, next) => {
    try {
      const result = await unpostInvoiceStock(req.params.id);
      res.json({ ok: true, data: result });
    } catch (e) {
      next(e);
    }
  }
);
r.patch("/:id/note", requireAuth, async (req, res, next) => {
  try {
    const note = (req.body?.note ?? "") as string;
    const invoice = await updateInvoiceNote(req.params.id, note);
    res.json(invoice);
  } catch (err) {
    next(err);
  }
});
export default r;
