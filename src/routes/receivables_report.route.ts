// src/routes/receivables_report.route.ts
import { Router } from "express";
import { requireAuth } from "../middlewares/auth";
import { getReceivablesReport } from "../services/receivables_report.service";

const router = Router();

router.use(requireAuth);

function isValidDateString(s: any) {
  if (!s) return true; // optional
  const d = new Date(String(s));
  return !isNaN(d.getTime());
}

function parseBoolish(v: any, defaultValue: boolean) {
  if (v == null) return defaultValue;
  const s = String(v).trim().toLowerCase();
  if (s === "1" || s === "true" || s === "yes") return true;
  if (s === "0" || s === "false" || s === "no") return false;
  return defaultValue;
}

/**
 * GET /api/receivables_report?asOf=2025-12-19&includeRows=1
 * - asOf: ngày chốt công nợ
 * - includeRows: 0/1 (default 1) trả thêm list hóa đơn chi tiết
 */
router.get("/", async (req, res, next) => {
  try {
    const asOf = typeof req.query.asOf === "string" ? req.query.asOf : undefined;
    const includeRows = parseBoolish(req.query.includeRows, true);

    if (!isValidDateString(asOf)) {
      return res.status(400).json({ ok: false, message: "asOf không hợp lệ." });
    }

    const result = await getReceivablesReport({ asOf, includeRows });
    return res.json(result);
  } catch (e) {
    next(e);
  }
});

router.get("/ping", (_req, res) => {
  res.json({ ok: true, route: "receivables_report", message: "receivables_report router is mounted" });
});

export default router;
