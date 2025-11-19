// src/routes/items.routes.ts
import { Router } from "express";
import multer from "multer";
import * as XLSX from "xlsx";
import { PrismaClient } from "@prisma/client";

import { requireAuth, requireRole } from "../middlewares/auth";
import {
  listItems,
  createItem,
  updateItem,
  removeItem,
} from "../services/items.service";
import { importItemsFromBuffer } from "../services/import.service";

const r = Router();
const prisma = new PrismaClient();
const upload = multer({ storage: multer.memoryStorage() });

r.use(requireAuth);

/**
 * GET /items?q=&page=&pageSize=
 * => trả về:
 * {
 *   ok: true,
 *   items: Item[],   // dùng cho autocomplete, item tổng...
 *   data: Item[],    // alias, để các chỗ cũ dùng res.data.data vẫn chạy
 *   total,
 *   page,
 *   pageSize
 * }
 *
 * listItems ở service KHÔNG filter theo kind,
 * nên kết quả đã bao gồm cả máy lẫn linh kiện.
 */
r.get("/", async (req, res, next) => {
  try {
    const { q, page = "1", pageSize = "20" } = req.query as any;

    const pageNum = Number(page) || 1;
    const pageSizeNum = Number(pageSize) || 20;

    const { data, total } = await listItems(q, pageNum, pageSizeNum);

    res.json({
      ok: true,
      items: data,       // 👈 FE autocomplete / item tổng dùng cái này
      data,              // 👈 alias cho các màn cũ (nếu có) đang dùng res.data.data
      total,
      page: pageNum,
      pageSize: pageSizeNum,
    });
  } catch (e) {
    next(e);
  }
});

/** POST /items (accountant|admin) */
r.post("/", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const created = await createItem(req.body);
    res.json({ ok: true, data: created });
  } catch (e) {
    next(e);
  }
});

/** PUT /items/:id (accountant|admin) */
r.put("/:id", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const updated = await updateItem(req.params.id, req.body);
    res.json({ ok: true, data: updated });
  } catch (e) {
    next(e);
  }
});

/** DELETE /items/:id (admin) */
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
 * Cột hỗ trợ: sku|skud|mahang|code, name, unit?, price?, sellPrice?, note?, kind?
 */
r.post(
  "/import",
  requireRole("accountant", "admin"),
  upload.single("file"),
  async (req, res, next) => {
    try {
      const file = (req as any).file as { buffer: Buffer } | undefined;
      if (!file) throw new Error('Missing file field "file"');

      const result = await importItemsFromBuffer(file.buffer);
      res.json({ ok: true, ...result });
    } catch (e) {
      next(e);
    }
  }
);

/** EXPORT items (xlsx) (accountant|admin) */
r.get(
  "/export",
  requireRole("accountant", "admin"),
  async (_req, res, next) => {
    try {
      const items = await prisma.item.findMany({
        orderBy: { createdAt: "desc" },
      });

      const data = items.map((i) => ({
        sku: i.sku,
        name: i.name,
        unit: i.unit,
        price: i.price.toString(),
        sellPrice: (i.sellPrice as any)?.toString?.() ?? "0",
        note: i.note ?? "",
        // nếu đã thêm cột kind trong Prisma thì có thể ghi thêm:
        // kind: (i as any).kind ?? '',
      }));

      const ws = XLSX.utils.json_to_sheet(data);
      const wb = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(wb, ws, "Items");
      const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

      res.setHeader(
        "Content-Disposition",
        'attachment; filename="items.xlsx"'
      );
      res.setHeader(
        "Content-Type",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
      );
      res.send(buf);
    } catch (e) {
      next(e);
    }
  }
);

export default r;
