// src/routes/items.routes.ts
import { Router } from "express";
import multer from "multer";
import * as XLSX from "xlsx";
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
r.get("/export", requireRole("accountant", "admin"), async (_req, res, next) => {
  try {
    const items = await prisma.item.findMany({
      orderBy: { createdAt: "desc" },
      include: { unit: true },
    });

    const data = items.map((i) => ({
      sku: i.sku,
      name: i.name,
      unitCode: i.unit?.code ?? "",
      unitName: i.unit?.name ?? "",
      unitId: i.unitId,
      price: i.price?.toString?.() ?? "0",
      sellPrice: (i.sellPrice as any)?.toString?.() ?? "0",
      note: i.note ?? "",
      kind: (i as any).kind ?? "",
      isSerialized: (i as any).isSerialized ?? false,
    }));

    const ws = XLSX.utils.json_to_sheet(data);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Items");
    const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

    res.setHeader("Content-Disposition", 'attachment; filename="items.xlsx"');
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.send(buf);
  } catch (e) {
    next(e);
  }
});

export default r;
