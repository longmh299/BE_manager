// src/routes/movements.routes.ts
import { Router } from "express";
import { MovementType, PrismaClient } from "@prisma/client";
import { requireAuth, requireRole } from "../middlewares/auth";
import {
  createDraft,
  addLine,
  updateLine,
  deleteLine,
  postMovement,
} from "../services/movements.service";
import {
  ensureMovementNotLocked,
  ensureMovementLineNotLocked,
} from "../services/periodLock.service";

const prisma = new PrismaClient();
const r = Router();

r.use(requireAuth);

/** GET /movements?type=&posted=&page=&pageSize= */
r.get("/", async (req, res, next) => {
  try {
    const { type, posted, page = "1", pageSize = "20" } = req.query as any;
    const where: any = {};
    if (type) where.type = type;
    if (posted !== undefined) where.posted = posted === "true";

    const [total, data] = await Promise.all([
      prisma.movement.count({ where }),
      prisma.movement.findMany({
        where,
        include: { lines: { include: { item: true } } },
        orderBy: { createdAt: "desc" },
        skip: (Number(page) - 1) * Number(pageSize),
        take: Number(pageSize),
      }),
    ]);

    res.json({
      ok: true,
      total,
      page: Number(page),
      pageSize: Number(pageSize),
      data,
    });
  } catch (e) {
    next(e);
  }
});

/** GET /movements/:id */
r.get("/:id", async (req, res, next) => {
  try {
    const data = await prisma.movement.findUnique({
      where: { id: req.params.id },
      include: { lines: { include: { item: true } }, invoice: true },
    });
    res.json({ ok: true, data });
  } catch (e) {
    next(e);
  }
});

/** POST /movements  (accountant|admin) — tạo phiếu nháp (IN/OUT/TRANSFER) */
r.post("/", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const { type, refNo, note } = req.body as {
      type: MovementType;
      refNo?: string;
      note?: string;
    };

    // createDraft nhận (type, { refNo?, note? })
    const data = await createDraft(type, { refNo, note });

    res.json({ ok: true, data });
  } catch (e) {
    next(e);
  }
});

/** POST /movements/:id/lines (accountant|admin) — thêm dòng */
r.post(
  "/:id/lines",
  requireRole("accountant", "admin"),
  async (req, res, next) => {
    try {
      const movementId = req.params.id;
      // 🔒 kiểm tra phiếu có thuộc kỳ đã khoá không
      await ensureMovementNotLocked(movementId);

      const { itemId, fromLocationId, toLocationId, qty, note } = req.body;
      const data = await addLine(movementId, {
        itemId,
        fromLocationId,
        toLocationId,
        qty,
        note,
      });
      res.json({ ok: true, data });
    } catch (e) {
      next(e);
    }
  }
);

/** PUT /movements/lines/:lineId (accountant|admin) — sửa dòng */
r.put(
  "/lines/:lineId",
  requireRole("accountant", "admin"),
  async (req, res, next) => {
    try {
      const lineId = req.params.lineId;
      // 🔒 kiểm tra dòng thuộc kỳ đã khoá không
      await ensureMovementLineNotLocked(lineId);

      const data = await updateLine(lineId, req.body);
      res.json({ ok: true, data });
    } catch (e) {
      next(e);
    }
  }
);

/** DELETE /movements/lines/:lineId (accountant|admin) — xoá dòng */
r.delete(
  "/lines/:lineId",
  requireRole("accountant", "admin"),
  async (req, res, next) => {
    try {
      const lineId = req.params.lineId;
      // 🔒 kiểm tra dòng thuộc kỳ đã khoá không
      await ensureMovementLineNotLocked(lineId);

      const data = await deleteLine(lineId);
      res.json({ ok: true, data });
    } catch (e) {
      next(e);
    }
  }
);

/** POST /movements/:id/post (accountant|admin) — ghi sổ */
r.post(
  "/:id/post",
  requireRole("accountant", "admin"),
  async (req, res, next) => {
    try {
      const movementId = req.params.id;
      // 🔒 kiểm tra phiếu thuộc kỳ đã khoá không
      await ensureMovementNotLocked(movementId);

      const data = await postMovement(movementId);
      res.json({ ok: true, data });
    } catch (e) {
      next(e);
    }
  }
);

export default r;
