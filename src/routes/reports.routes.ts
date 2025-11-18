// src/routes/reports.routes.ts
import { Router } from "express";
import { requireAuth } from "../middlewares/auth";
import { getRevenueSummary } from "../services/reports.service";

export const reportsRouter = Router();

// tất cả report yêu cầu đăng nhập
reportsRouter.use(requireAuth);

/**
 * GET /api/reports/revenue?from=yyyy-mm-dd&to=yyyy-mm-dd
 *
 * - admin, accountant  => xem toàn bộ
 * - các role khác (vd staff) => chỉ xem hóa đơn do chính mình phụ trách
 */
reportsRouter.get("/revenue", async (req, res, next) => {
  try {
    const { from, to } = req.query as { from?: string; to?: string };

    const user: any = (req as any).user; // đã được set trong requireAuth
    const role: string | undefined = user?.role;
    const userId: string | undefined = user?.id;

    // admin + accountant: xem full
    // còn lại (staff, ...) => chỉ xem của mình
    let userIdFilter: string | undefined = undefined;
    if (role !== "admin" && role !== "accountant") {
      userIdFilter = userId;
    }

    const summary = await getRevenueSummary({
      from,
      to,
      userId: userIdFilter,
    });

    res.json({
      ok: true,
      data: summary,
    });
  } catch (e) {
    next(e);
  }
});
