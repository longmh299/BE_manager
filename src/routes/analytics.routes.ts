// src/routes/analytics.routes.ts
import { Router } from "express";
import { requireAuth, requireRole } from "../middlewares/auth";
import { getAnalyticsOverview } from "../services/analytics.service";

const r = Router();

// ✅ chỉ admin xem được thống kê truy cập web
r.use(requireAuth, requireRole("admin"));

/**
 * GET /api/analytics/overview
 * Query:
 *  - from: "YYYY-MM-DD" hoặc từ khoá GA4 ("7daysAgo", "30daysAgo", "today"...) — mặc định "28daysAgo"
 *  - to:   "YYYY-MM-DD" hoặc "today" — mặc định "today"
 *  - compare: "1" để so sánh % với kỳ trước đó (chỉ hoạt động khi from/to là ngày cụ thể)
 */
r.get("/overview", async (req, res, next) => {
  try {
    const from = (req.query.from as string) || "28daysAgo";
    const to = (req.query.to as string) || "today";
    const compare = String(req.query.compare || "") === "1";

    const data = await getAnalyticsOverview({ from, to, compare });
    res.json({ ok: true, data });
  } catch (e) {
    next(e);
  }
});

export default r;