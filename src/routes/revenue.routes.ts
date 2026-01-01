import { Router } from "express";
import { getRevenueDashboard } from "../services/revenue.service";
import { requireAuth, getUser } from "../middlewares/auth";

const router = Router();

// GET /api/revenue/dashboard?from=...&to=...&staffRole=SALE|TECH&staffUserId=...&receiveAccountId=...&includeStaffInvoices=1
router.get("/dashboard", requireAuth, async (req, res, next) => {
  try {
    const me = getUser(req);
    if (!me) {
      return res.status(401).json({ code: "UNAUTHORIZED", message: "Unauthorized" });
    }

    const isStaff = me.role === "staff";

    const staffRole = (req.query.staffRole as any) || undefined;
    const staffUserIdQuery = (req.query.staffUserId as string) || undefined;

    const includeStaffInvoices =
      String(req.query.includeStaffInvoices || "") === "1" ||
      String(req.query.includeStaffInvoices || "").toLowerCase() === "true";

    // ✅ staff chỉ xem được đúng của mình
    const finalStaffUserId = isStaff ? me.id : staffUserIdQuery;

    const data = await getRevenueDashboard({
      from: req.query.from as string | undefined,
      to: req.query.to as string | undefined,

      staffRole,
      staffUserId: finalStaffUserId || undefined,

      receiveAccountId: (req.query.receiveAccountId as string) || undefined,
      includeStaffInvoices,
    });

    res.json(data);
  } catch (e) {
    next(e);
  }
});

export default router;
