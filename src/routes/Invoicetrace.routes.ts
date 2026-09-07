// src/routes/invoiceTrace.routes.ts
import { Router } from "express";
import { requireAuth, getUser } from "../middlewares/auth";
import { traceInvoiceLinesByItemCode } from "../services/Invoicetrace.service";
import type { InvoiceType, InvoiceStatus } from "@prisma/client";

const r = Router();

r.use(requireAuth);

function isAdmin(role: string) {
  return role === "admin";
}
function isStaff(role: string) {
  return role === "staff";
}

const VALID_TYPES: InvoiceType[] = ["PURCHASE", "SALES", "PURCHASE_RETURN", "SALES_RETURN"];

/**
 * GET /api/invoice-trace
 * Query:
 *  - code (bắt buộc): mã sản phẩm (sku) cần tra cứu
 *  - type (bắt buộc): PURCHASE (nhập) | SALES (bán) | PURCHASE_RETURN | SALES_RETURN
 *  - from, to: khoảng ngày (issueDate), ISO string, optional
 *  - status: lọc theo trạng thái hoá đơn, optional
 *  - page, pageSize: phân trang, optional (default 1 / 20)
 *
 * Trả về DANH SÁCH DÒNG sản phẩm khớp mã, kèm thông tin hoá đơn chứa nó
 * (mã hoá đơn, loại, ngày, đối tác, số lượng/đơn giá/thành tiền) — không trả cả hoá đơn.
 */
r.get("/", async (req, res, next) => {
  try {
    const u = getUser(req)!;

    const {
      code = "",
      type = "",
      from = "",
      to = "",
      status = "",
      page = "1",
      pageSize = "20",
    } = req.query as any;

    const typeStr = String(type).toUpperCase();
    if (!VALID_TYPES.includes(typeStr as InvoiceType)) {
      return res.status(400).json({
        ok: false,
        message: "Loại hoá đơn không hợp lệ. Chỉ chấp nhận: PURCHASE, SALES, PURCHASE_RETURN, SALES_RETURN.",
      });
    }

    // Xuất trả NCC (PURCHASE_RETURN) chỉ dành cho admin, đồng bộ với quyền của /invoices
    if (typeStr === "PURCHASE_RETURN" && !isAdmin(u.role)) {
      return res.status(403).json({ ok: false, message: "Xuất trả NCC chỉ dành cho ADMIN." });
    }

    const saleUserId = isStaff(u.role) ? u.id : undefined;
    const excludeTypes = !isAdmin(u.role) ? (["PURCHASE_RETURN"] as InvoiceType[]) : undefined;

    const result = await traceInvoiceLinesByItemCode({
      code: String(code),
      type: typeStr as InvoiceType,
      from: from ? new Date(String(from)) : undefined,
      to: to ? new Date(String(to)) : undefined,
      status: status ? (String(status) as InvoiceStatus) : undefined,
      page: Number(page) || 1,
      pageSize: Number(pageSize) || 20,
      saleUserId,
      excludeTypes,
    });

    // Ẩn giá vốn với role không được xem (giống sanitize của /invoices)
    const canSeeCost = u.role === "admin" || u.role === "accountant";
    const data = result.data.map((row) => {
      if (canSeeCost) return row;
      const { unitCost, costTotal, ...rest } = row;
      return rest;
    });

    res.json({
      ok: true,
      data,
      total: result.total,
      page: result.page,
      pageSize: result.pageSize,
    });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});

export default r;