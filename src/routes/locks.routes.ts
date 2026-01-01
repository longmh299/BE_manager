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

function parseYMDToLocalEOD(ymd: string) {
  // ymd: "YYYY-MM-DD"
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(ymd).trim());
  if (!m) return null;
  const y = Number(m[1]);
  const mo = Number(m[2]) - 1;
  const d = Number(m[3]);
  const dt = new Date(y, mo, d, 23, 59, 59, 999); // ✅ local end of day
  if (Number.isNaN(dt.getTime())) return null;
  return dt;
}

/**
 * POST /locks/period  (accountant|admin)
 * Body: { closedUntil: "2025-11-30", note? }
 * -> Khoá kỳ tới hết ngày đó (Rule lock trong service: occurredAt ưu tiên, fallback createdAt)
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

      const d = parseYMDToLocalEOD(closedUntil);
      if (!d) {
        return res
          .status(400)
          .json({ ok: false, message: "closedUntil không hợp lệ (yyyy-mm-dd)" });
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
