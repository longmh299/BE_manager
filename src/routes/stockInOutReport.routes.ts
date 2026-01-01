// src/routes/stockInOutReport.routes.ts
import { Router } from "express";
import { requireAuth } from "../middlewares/auth";
import { getStockInOutReport } from "../services/stockInOutReport.service";

const r = Router();
r.use(requireAuth);

// GET /api/reports/stock-inout
r.get("/stock-inout", async (req, res) => {
  try {
    const { from, to, q, warehouseId } = req.query as any;

    const data = await getStockInOutReport({
      from: from ? String(from) : undefined,
      to: to ? String(to) : undefined,
      q: q ? String(q) : undefined,
      warehouseId: warehouseId ? String(warehouseId) : undefined,
    });

    res.json(data);
  } catch (e: any) {
    res.status(400).json({ ok: false, message: e?.message || "Failed to build report" });
  }
});

export default r;
