// src/routes/stocks_import.routes.ts
import { Router, text } from "express";
import multer from "multer";
import * as XLSX from "xlsx";
import { requireAuth, requireAnyRole } from "../middlewares/auth";
import { importOpeningFromExcelBuffer } from "../services/stocks_import.service";

const router = Router();

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB
});

type ImportMode = "replace" | "add";

function parseMode(raw: any): ImportMode {
  const s = String(raw ?? "replace").toLowerCase().trim();
  return s === "add" || s === "adjust" ? "add" : "replace";
}

/**
 * Trả về JSON ok chuẩn mà không bị TS2783 (ok bị set 2 lần).
 * - Nếu result đã có ok -> giữ nguyên ok đó
 * - Nếu chưa có ok -> set ok: true
 */
function sendOk(res: any, result: any) {
  if (result && typeof result === "object" && "ok" in result) return res.json(result);
  return res.json({ ok: true, ...result });
}

/**
 * POST /imports/stocks/opening-onefile
 * Postman form-data:
 *  - file: (xlsx)
 *  - mode: replace | add
 */
router.post(
  "/opening-onefile",
  requireAuth,
  requireAnyRole(["admin", "accountant"]),
  upload.single("file"),
  async (req, res, next) => {
    try {
      const f = req.file;
      if (!f) {
        return res.status(400).json({
          ok: false,
          message: "Missing file — please upload with key 'file'",
        });
      }

      const mode: ImportMode = parseMode(req.body?.mode);
      const result = await importOpeningFromExcelBuffer(f.buffer, { mode });

      return sendOk(res, result);
    } catch (e) {
      return next(e);
    }
  }
);

/**
 * POST /imports/stocks/opening  (JSON rows)
 * Body:
 * {
 *   "mode": "replace"|"add",
 *   "rows": [...]
 * }
 * Convert JSON -> XLSX buffer để reuse importer
 */
router.post(
  "/opening",
  requireAuth,
  requireAnyRole(["admin", "accountant"]),
  async (req, res, next) => {
    try {
      const mode: ImportMode = parseMode(req.body?.mode);

      const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
      if (!rows.length) {
        return res.status(400).json({
          ok: false,
          message: "rows must be a non-empty array",
        });
      }

      const ws = XLSX.utils.json_to_sheet(rows);
      const wb = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(wb, ws, "OPENING");
      const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

      const result = await importOpeningFromExcelBuffer(buf, { mode });
      return sendOk(res, result);
    } catch (e) {
      return next(e);
    }
  }
);

/**
 * POST /imports/stocks/opening/csv
 * Raw CSV body -> convert to XLSX buffer -> reuse importer
 */
router.post(
  "/opening/csv",
  requireAuth,
  requireAnyRole(["admin", "accountant"]),
  text({
    type: ["text/csv", "application/vnd.ms-excel", "text/plain"],
  }),
  async (req, res, next) => {
    try {
      const csv = req.body || "";
      if (!csv.trim()) {
        return res.status(400).json({ ok: false, message: "Empty CSV body" });
      }

      const mode: ImportMode = parseMode(req.query.mode ?? req.body?.mode);

      const wb = XLSX.read(csv, { type: "string" });
      const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

      const result = await importOpeningFromExcelBuffer(buf, { mode });
      return sendOk(res, result);
    } catch (e) {
      return next(e);
    }
  }
);

/**
 * GET /imports/stocks/opening-template
 * Excel template đúng format bạn đang dùng
 */
router.get(
  "/opening-template",
  requireAuth,
  requireAnyRole(["admin", "accountant"]),
  async (_req, res, next) => {
    try {
      const rows = [
        {
          name: "Bếp chiên vn [ID-1]",
          skud: "ID-1",
          ton_dau: 1,
          nhap: 0,
          xuat: 1,
          ton_cuoi: 0,
          note: "Ví dụ",
          location: "wh-01",
          kind: "MACHINE",
          ten_goc: "Bếp chiên vn",
          gia_goc: 70000,
        },
        {
          name: "Bếp nướng điện [ID-2]",
          skud: "ID-2",
          ton_dau: 1,
          nhap: 0,
          xuat: 0,
          ton_cuoi: 1,
          note: "",
          location: "wh-01",
          kind: "MACHINE",
          ten_goc: "Bếp nướng điện",
          gia_goc: 22222,
        },
      ];

      const ws = XLSX.utils.json_to_sheet(rows, {
        header: [
          "name",
          "skud",
          "ton_dau",
          "nhap",
          "xuat",
          "ton_cuoi",
          "note",
          "location",
          "kind",
          "ten_goc",
          "gia_goc",
        ],
      });

      const wb = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(wb, ws, "OpeningTemplate");
      const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

      res.setHeader(
        "Content-Disposition",
        'attachment; filename="ton_dau_template.xlsx"'
      );
      res.setHeader(
        "Content-Type",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
      );

      return res.send(buf);
    } catch (e) {
      return next(e);
    }
  }
);

export default router;
