// src/routes/locks.routes.ts
import { Router } from "express";
import { requireAuth, requireRole } from "../middlewares/auth";
import { PrismaClient } from "@prisma/client";
import { getClosedUntil } from "../services/periodLock.service";

const prisma = new PrismaClient();
const r = Router();

r.use(requireAuth);

/**
 * GET /locks/period
 * -> Xem ngày đã khoá đến đâu
 */
r.get("/period", async (_req, res, next) => {
  try {
    const closedUntil = await getClosedUntil();
    res.json({
      ok: true,
      closedUntil,
    });
  } catch (e) {
    next(e);
  }
});

/**
 * POST /locks/period  (accountant|admin)
 * Body: { closedUntil: "2025-11-30", note? }
 * -> Khoá kỳ tới hết ngày đó (tính theo createdAt của Movement)
 */
r.post(
  "/period",
  requireRole("accountant", "admin"),
  async (req, res, next) => {
    try {
      const { closedUntil, note } = req.body as {
        closedUntil: string;
        note?: string;
      };

      if (!closedUntil) {
        return res
          .status(400)
          .json({ ok: false, message: "Thiếu closedUntil (yyyy-mm-dd)" });
      }

      // parse yyyy-mm-dd -> Date (cuối ngày)
      const d = new Date(closedUntil + "T23:59:59.999Z");
      if (Number.isNaN(d.getTime())) {
        return res
          .status(400)
          .json({ ok: false, message: "closedUntil không hợp lệ" });
      }

      const row = await prisma.periodLock.create({
        data: {
          closedUntil: d,
          note: note ?? null,
        },
      });

      res.json({ ok: true, data: row });
    } catch (e) {
      next(e);
    }
  }
);

export default r;
