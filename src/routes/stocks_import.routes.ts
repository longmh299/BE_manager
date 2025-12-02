import { Router, text } from "express";
import multer from "multer";
import * as XLSX from "xlsx";
import { requireAuth, requireAnyRole } from "../middlewares/auth";
import {
  importOpeningStocks,
  importOpeningOneFile,
} from "../services/stocks_import.service";

const router = Router();

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB
});

type ImportMode = "replace" | "add";

/** JSON rows */
router.post(
  "/opening",
  requireAuth,
  requireAnyRole(["admin", "accountant"]),
  async (req, res) => {
    try {
      const result = await importOpeningStocks(undefined as any, req.body);
      return res.json(Object.assign({ ok: true }, result));
    } catch (e: any) {
      return res
        .status(400)
        .json({ ok: false, message: e?.message || "Import failed" });
    }
  },
);

/** ONE FILE – chấp nhận mọi field file (upload.any) */
router.post(
  "/opening-onefile",
  requireAuth,
  requireAnyRole(["admin", "accountant"]),
  upload.any(),
  async (req, res) => {
    try {
      const files = (req as any).files as
        | Express.Multer.File[]
        | undefined;
      const f =
        (files && files.find((ff) => ff.fieldname === "file")) ||
        (files && files[0]);
      if (!f) {
        return res.status(400).json({
          ok: false,
          message: "Missing file — please upload with key 'file'",
        });
      }

      const rawMode = (req.body?.mode ?? "replace")
        .toString()
        .toLowerCase();
      const mode: ImportMode =
        rawMode === "add" || rawMode === "adjust" ? "add" : "replace";

      const result = await importOpeningOneFile(f.buffer, { mode });
      return res.json(Object.assign({ ok: true }, result));
    } catch (e: any) {
      return res
        .status(400)
        .json({ ok: false, message: e?.message || "Import failed" });
    }
  },
);

/** CSV raw (tuỳ chọn) */
router.post(
  "/opening/csv",
  requireAuth,
  requireAnyRole(["admin", "accountant"]),
  text({
    type: ["text/csv", "application/vnd.ms-excel", "text/plain"],
  }),
  async (req, res) => {
    try {
      const csv = req.body || "";
      if (!csv.trim())
        return res
          .status(400)
          .json({ ok: false, message: "Empty CSV body" });

      const rawMode = (req.query.mode || req.body?.mode || "replace")
        .toString()
        .toLowerCase();
      const mode: ImportMode =
        rawMode === "add" || rawMode === "adjust" ? "add" : "replace";

      const buf = Buffer.from(csv, "utf8");
      const result = await importOpeningOneFile(buf, { mode });
      return res.json(Object.assign({ ok: true }, result));
    } catch (e: any) {
      return res
        .status(400)
        .json({ ok: false, message: e?.message || "Import failed" });
    }
  },
);

/**
 * GET /imports/stocks/opening-template
 * Xuất file Excel mẫu để nhập tồn đầu
 */
router.get(
  "/opening-template",

  async (req, res) => {
    try {
      const rows = [
        {
          sku: "MAY001",
          name: "Máy hút chân không DZ-400",
          kind: "MACHINE",
          qty: 10,
          sellPrice: 4500000,
          note: "Ví dụ máy",
        },
        {
          sku: "PART001",
          name: "Dây nhiệt máy hàn",
          kind: "PART",
          qty: 200,
          sellPrice: 15000,
          note: "Ví dụ linh kiện",
        },
      ];

      const ws = XLSX.utils.json_to_sheet(rows, {
        header: ["sku", "name", "kind", "qty", "sellPrice", "note"],
      });
      const wb = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(wb, ws, "OpeningTemplate");
      const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

      res.setHeader(
        "Content-Disposition",
        'attachment; filename="ton_dau_template.xlsx"',
      );
      res.setHeader(
        "Content-Type",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      );
      res.send(buf);
    } catch (e: any) {
      return res
        .status(500)
        .json({ ok: false, message: e?.message || "Export template failed" });
    }
  },
);

export default router;
