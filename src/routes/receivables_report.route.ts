import { Router } from "express";
import { getReceivablesReport } from "../services/receivables_report.service";

const router = Router();

/**
 * GET /api/receivables_report?asOf=2025-12-19&includeRows=1
 * - asOf: ngày chốt công nợ
 * - includeRows: 0/1 (default 1) trả thêm list hóa đơn chi tiết
 */
router.get("/", async (req, res, next) => {
  try {
    const asOf = typeof req.query.asOf === "string" ? req.query.asOf : undefined;
    const includeRows = req.query.includeRows == null ? true : String(req.query.includeRows) !== "0";

    const result = await getReceivablesReport({ asOf, includeRows });
    res.json(result);
  } catch (e) {
    next(e);
  }
});

export default router;
