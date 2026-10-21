// src/routes/items.routes.ts
import { Router } from "express";
import multer from "multer";
import * as XLSX from "xlsx";
import { buildReport, sendXlsx, exportSubtitle, stampName, KIND_VN } from "../lib/excelReport"; // ✅ khuôn Excel dùng chung
import { PrismaClient, Prisma } from "@prisma/client";

import { requireAuth, requireRole, getUser } from "../middlewares/auth";
import {
  listItems,
  createItem,
  updateItem,
  updateItemMaster,
  removeItem,
} from "../services/items.service";
import { importItemsFromBuffer } from "../services/items_import.service";

const r = Router();
const prisma = new PrismaClient();
const upload = multer({ storage: multer.memoryStorage() });

r.use(requireAuth);

function friendlyPrismaError(e: any) {
  if (e instanceof Prisma.PrismaClientKnownRequestError) {
    if (e.code === "P2002") {
      const target = (e.meta as any)?.target;

      // target có thể là string[] hoặc string tuỳ Prisma version
      const targets: string[] = Array.isArray(target)
        ? target
        : typeof target === "string"
          ? [target]
          : [];

      if (targets.includes("sku")) {
        const err: any = new Error("Mã máy (SKU) đã tồn tại");
        err.statusCode = 409;
        return err;
      }

      if (targets.includes("name")) {
        const err: any = new Error("Tên sản phẩm đã tồn tại");
        err.statusCode = 409;
        return err;
      }

      const err: any = new Error("Dữ liệu bị trùng (unique constraint)");
      err.statusCode = 409;
      return err;
    }
  }
  return e;
}

/**
 * GET /api/items?q=&page=&pageSize=
 */
r.get("/", async (req, res, next) => {
  try {
    const { q, page = "1", pageSize = "20" } = req.query as any;

    const pageNum = Number(page) || 1;
    const pageSizeNum = Number(pageSize) || 20;

    // ✅ truyền actor để admin thấy price nếu FE cần
    const actor = getUser(req);
    const { data, total } = await listItems(q, pageNum, pageSizeNum, actor);

    res.json({
      ok: true,
      items: data,
      data,
      total,
      page: pageNum,
      pageSize: pageSizeNum,
    });
  } catch (e) {
    next(e);
  }
});

/**
 * GET /api/items/units
 * - dropdown đơn vị tính
 */
r.get("/units", async (_req, res, next) => {
  try {
    const units = await prisma.unit.findMany({
      orderBy: { name: "asc" },
      select: { id: true, code: true, name: true, note: true },
    });
    res.json({ ok: true, data: units });
  } catch (e) {
    next(e);
  }
});

/** POST /api/items (accountant|admin) */
r.post("/", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const actor = getUser(req);
    const created = await createItem(req.body, actor);
    res.json({ ok: true, data: created });
  } catch (e) {
    next(friendlyPrismaError(e));
  }
});

/**
 * PATCH /api/items/:id/master (admin-only)
 * - dành riêng cho UI admin sửa nhanh sku + name + unit
 */
r.patch("/:id/master", requireRole("admin"), async (req, res, next) => {
  try {
    const updated = await updateItemMaster(req.params.id, req.body);
    res.json({ ok: true, data: updated });
  } catch (e) {
    next(friendlyPrismaError(e));
  }
});

/** PUT /api/items/:id (accountant|admin) */
r.put("/:id", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const actor = getUser(req);
    const updated = await updateItem(req.params.id, req.body, actor);
    res.json({ ok: true, data: updated });
  } catch (e) {
    next(friendlyPrismaError(e));
  }
});

/** DELETE /api/items/:id (admin) */
r.delete("/:id", requireRole("admin"), async (req, res, next) => {
  try {
    const del = await removeItem(req.params.id);
    res.json({ ok: true, data: del });
  } catch (e) {
    next(e);
  }
});

/**
 * IMPORT items (xlsx/csv) (accountant|admin)
 * Postman: form-data key = file
 */
r.post(
  "/import",
  requireRole("accountant", "admin"),
  upload.single("file"),
  async (req, res, next) => {
    try {
      const file = req.file;
      if (!file) throw new Error('Missing file field "file"');

      const result = await importItemsFromBuffer(file.buffer);
      res.json(result);
    } catch (e) {
      next(e);
    }
  }
);

/** EXPORT items (xlsx) (accountant|admin) */
r.get("/export", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const items = await prisma.item.findMany({
      orderBy: { createdAt: "desc" },
      include: { unit: true },
    });

    // ✅ trình bày lại nhưng VẪN NHẬP LẠI ĐƯỢC: tiêu đề cột ở dòng 1, tên cột trình nhập (/items/import) đọc được
    const data = items.map((i) => ({
      sku: i.sku,
      name: i.name,
      kind: KIND_VN[(i as any).kind] ?? (i as any).kind ?? "",
      unit: i.unit?.name || i.unit?.code || "",
      sellPrice: i.sellPrice,
      price: i.price,
      note: i.note ?? "",
      serial: (i as any).isSerialized ? "Có" : "",
    }));
    const buf = await buildReport({
      sheetName: "Danh mục hàng",
      columns: [
        { header: "Mã hàng", key: "sku", width: 14 },
        { header: "Tên hàng", key: "name", width: 48, wrap: true },
        { header: "Loại", key: "kind", width: 11, type: "center" },
        { header: "ĐVT", key: "unit", width: 8, type: "center" },
        { header: "Giá bán", key: "sellPrice", width: 15, type: "money" },
        { header: "Giá vốn (tham khảo)", key: "price", width: 15, type: "money" },
        { header: "Ghi chú", key: "note", width: 30, wrap: true },
        { header: "Theo serial", key: "serial", width: 10, type: "center" },
      ],
      rows: data,
      notes: {
        sheetName: "Hướng dẫn",
        lines: [
          ["Danh mục hàng hoá", exportSubtitle(getUser(req), `${items.length} mã hàng`)],
          ["Nhập lại", "Sửa ở sheet \"Danh mục hàng\" rồi tải lên bằng nút Nhập Excel. GIỮ NGUYÊN tên cột ở dòng 1."],
          ["Được cập nhật", "Mã hàng, Giá bán, Ghi chú, Loại (Máy / Phụ tùng). Hàng được nhận diện theo Tên hàng."],
          ["Không nhập", "Giá vốn, ĐVT, Theo serial chỉ để xem — hệ thống bỏ qua các cột này khi nhập."],
        ],
      },
    });
    sendXlsx(res, buf, stampName("danh-muc-hang"));
  } catch (e) {
    next(e);
  }
});

export default r;
