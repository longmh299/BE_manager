import { Router } from "express";
import { prisma } from "../tool/prisma";
import { buildLowStockAlerts } from "../services/assistant/alerts/lowStock.service";

const router = Router();

router.get("/low-stock", async (_req, res, next) => {
  try {
    const rows = await buildLowStockAlerts(prisma);

    res.json({
      ok: true,
      count: rows.length,
      rows,
    });
  } catch (e) {
    next(e);
  }
});

export default router;
