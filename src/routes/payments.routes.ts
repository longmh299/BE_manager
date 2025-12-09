// src/routes/payments.routes.ts
import { Router } from "express";
import { requireAuth } from "../middlewares/auth";
import {
  createPaymentWithAllocations,
  getPaymentById,
  listPayments,
} from "../services/payments.service";

const r = Router();

r.use(requireAuth);

function getUserId(req: any): string | undefined {
  return req.user?.id || req.userId;
}

r.post("/", async (req, res, next) => {
  try {
    const userId = getUserId(req);
    const payment = await createPaymentWithAllocations({
      ...(req.body || {}),
      createdById: userId,
    });
    res.json(payment);
  } catch (err: any) {
    console.error(err);
    res
      .status(400)
      .json({ message: err.message || "Không tạo được phiếu thanh toán" });
  }
});

r.get("/", async (req, res, next) => {
  try {
    const data = await listPayments({
      partnerId: req.query.partnerId as string | undefined,
      type: req.query.type as any,
      from: req.query.from as string | undefined,
      to: req.query.to as string | undefined,
    });
    res.json(data);
  } catch (err) {
    next(err);
  }
});

r.get("/:id", async (req, res, next) => {
  try {
    const payment = await getPaymentById(req.params.id);
    if (!payment) {
      return res.status(404).json({ message: "Không tìm thấy phiếu" });
    }
    res.json(payment);
  } catch (err) {
    next(err);
  }
});

export default r;
