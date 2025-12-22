// src/routes/revenue.routes.ts
import { Router } from "express";
import { getRevenueDashboard } from "../services/revenue.service";
import { requireAuth, getUser } from "../middlewares/auth";

const router = Router();

// GET /api/revenue/dashboard?from=...&to=...&groupBy=day|week|month&staffRole=SALE|TECH&staffUserId=...&receiveAccountId=...
router.get("/dashboard", requireAuth, async (req, res, next) => {
  try {
    const me = getUser(req);
    if (!me) {
      return res.status(401).json({ code: "UNAUTHORIZED", message: "Unauthorized" });
    }

    const isStaff = me.role === "staff";
    const staffRole = (req.query.staffRole as any) || undefined;
    const staffUserIdQuery = (req.query.staffUserId as string) || undefined;

    const data = await getRevenueDashboard({
      from: req.query.from as string | undefined,
      to: req.query.to as string | undefined,
      groupBy: (req.query.groupBy as any) || undefined,

      // ✅ PRIVATE: staff chỉ xem được đúng "của tôi"
      staffRole: staffRole,
      staffUserId: isStaff ? me.id : staffUserIdQuery,

      receiveAccountId: (req.query.receiveAccountId as string) || undefined,
    });

    res.json(data);
  } catch (e) {
    next(e);
  }
});

export default router;
