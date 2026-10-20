// src/routes/stockInOutReport.routes.ts
import { Router } from "express";
import { requireAuth, requireRole } from "../middlewares/auth";
import { getStockInOutReport } from "../services/stockInOutReport.service";

const r = Router();
r.use(requireAuth);

// GET /api/reports/stock-inout
// ✅ bảo mật: báo cáo xuất nhập tồn (có giá trị tồn) chỉ admin + kế toán — menu cũng chỉ 2 quyền này thấy
r.get("/stock-inout", requireRole("admin", "accountant"), async (req, res) => {
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
