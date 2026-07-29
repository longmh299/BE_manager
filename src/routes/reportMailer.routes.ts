// src/routes/reportMailer.routes.ts
import { Router } from "express";
import { requireAuth, requireRole } from "../middlewares/auth";
import { sendMonthlyReportNow, sendQuarterlyReportNow } from "../jobs/reportMailer.job";

const router = Router();

// POST /api/report-mail/monthly/send-now  (chỉ admin, để test gửi ngay)
router.post("/monthly/send-now", requireAuth, requireRole("admin"), async (_req, res, next) => {
  try {
    const r = await sendMonthlyReportNow();
    res.json({ ok: true, ...r });
  } catch (e) {
    next(e);
  }
});

// POST /api/report-mail/quarterly/send-now
router.post("/quarterly/send-now", requireAuth, requireRole("admin"), async (_req, res, next) => {
  try {
    const r = await sendQuarterlyReportNow();
    res.json({ ok: true, ...r });
  } catch (e) {
    next(e);
  }
});

export default router;