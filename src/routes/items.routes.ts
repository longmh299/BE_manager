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

/** GET /items?q=&page=&pageSize= */
r.get("/", async (req, res, next) => {
  try {
    const { q, page = "1", pageSize = "20" } = req.query as any;
    const data = await listItems(q, Number(page), Number(pageSize));
    res.json({ ok: true, ...data });
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

/** IMPORT items (xlsx/csv) (accountant|admin)
 * Cột hỗ trợ: sku|skud|mahang|code, name, unit?, price?, sellPrice?, note?, kind?
 */
r.post(
  "/import",
  requireRole("accountant", "admin"),
  upload.single("file"),
  async (req, res, next) => {
    try {
      // TS không biết req.file, cast sang any cho gọn
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
        // nếu bạn đã thêm cột kind trong prisma, có thể map thêm:
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
