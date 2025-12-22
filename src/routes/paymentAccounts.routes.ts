import { Router } from "express";
import * as svc from "../services/paymentAccounts.service";
import { requireAuth } from "../middlewares/auth"; // tùy project bạn đặt tên

const r = Router();

// GET /payment-accounts?active=1
r.get("/", requireAuth, async (req: any, res, next) => {
  try {
    const activeOnly = String(req.query.active ?? "1") !== "0";
    const rows = await svc.listPaymentAccounts(req.user, activeOnly);
    res.json(rows);
  } catch (e) {
    next(e);
  }
});

// POST /payment-accounts (admin)
r.post("/", requireAuth, async (req: any, res, next) => {
  try {
    const row = await svc.createPaymentAccount(req.user, req.body);
    res.json(row);
  } catch (e) {
    next(e);
  }
});

// PATCH /payment-accounts/:id (admin)
r.patch("/:id", requireAuth, async (req: any, res, next) => {
  try {
    const row = await svc.updatePaymentAccount(req.user, req.params.id, req.body);
    res.json(row);
  } catch (e) {
    next(e);
  }
});

// DELETE /payment-accounts/:id (admin) => deactivate
r.delete("/:id", requireAuth, async (req: any, res, next) => {
  try {
    const row = await svc.deactivatePaymentAccount(req.user, req.params.id);
    res.json(row);
  } catch (e) {
    next(e);
  }
});

export default r;
