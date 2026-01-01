// src/routes/movements.routes.ts
import { Router } from "express";
import { MovementType, PrismaClient } from "@prisma/client";
import { requireAuth, requireRole } from "../middlewares/auth";
import { createDraft, addLine, updateLine, deleteLine, postMovement } from "../services/movements.service";
import {
  ensureMovementNotLocked,
  ensureMovementLineNotLocked,
  ensureDateNotLocked,
} from "../services/periodLock.service";

const prisma = new PrismaClient();
const r = Router();

r.use(requireAuth);

function getUserId(req: any): string | undefined {
  return req.user?.id || req.userId;
}
function getUserRole(req: any): string | undefined {
  return req.user?.role || req.userRole;
}
function buildAuditMeta(req: any) {
  return {
    ip: req.ip,
    userAgent: req.headers?.["user-agent"],
    path: req.originalUrl || req.url,
    method: req.method,
    params: req.params,
    query: req.query,
  };
}
function requireUserOr401(req: any, res: any) {
  const userId = getUserId(req);
  if (!userId) {
    res.status(401).json({ ok: false, message: "Chưa đăng nhập." });
    return null;
  }
  return {
    userId,
    userRole: getUserRole(req),
    meta: buildAuditMeta(req),
  };
}

function parseYmdToLocalStart(ymd: string): Date | null {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(ymd || "").trim());
  if (!m) return null;
  const y = Number(m[1]);
  const mo = Number(m[2]);
  const d = Number(m[3]);
  const dt = new Date(y, mo - 1, d, 0, 0, 0, 0);
  if (Number.isNaN(dt.getTime())) return null;
  return dt;
}

/** GET /movements?type=&posted=&page=&pageSize= */
r.get("/", async (req, res, next) => {
  try {
    const { type, posted, page = "1", pageSize = "20" } = req.query as any;
    const where: any = {};
    if (type) where.type = type;
    if (posted !== undefined) where.posted = posted === "true";

    const pageNum = Number(page) || 1;
    const pageSizeNum = Number(pageSize) || 20;

    const [total, data] = await Promise.all([
      prisma.movement.count({ where }),
      prisma.movement.findMany({
        where,
        include: { lines: { include: { item: true, fromLoc: true, toLoc: true } } },
        orderBy: { createdAt: "desc" },
        skip: (pageNum - 1) * pageSizeNum,
        take: pageSizeNum,
      }),
    ]);

    res.json({ ok: true, total, page: pageNum, pageSize: pageSizeNum, data });
  } catch (e) {
    next(e);
  }
});

/** GET /movements/:id */
r.get("/:id", async (req, res, next) => {
  try {
    const data = await prisma.movement.findUnique({
      where: { id: req.params.id },
      include: { lines: { include: { item: true, fromLoc: true, toLoc: true } }, invoice: true },
    });
    res.json({ ok: true, data });
  } catch (e) {
    next(e);
  }
});

/** POST /movements  (accountant|admin) — tạo phiếu nháp */
r.post("/", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const { type, refNo, note, occurredAt } = req.body as {
      type: MovementType;
      refNo?: string;
      note?: string;
      occurredAt?: string; // "yyyy-mm-dd"
    };

    const audit = requireUserOr401(req, res);
    if (!audit) return;

    let occurredAtDate: Date | undefined = undefined;
    if (occurredAt) {
      const d = parseYmdToLocalStart(occurredAt);
      if (!d) {
        return res.status(400).json({ ok: false, message: "occurredAt không hợp lệ (yyyy-mm-dd)" });
      }
      // ✅ chặn tạo movement backdate vào kỳ đã khoá
      await ensureDateNotLocked(d, "tạo chứng từ");
      occurredAtDate = d;
    }

    const data = await createDraft(type, { refNo, note, occurredAt: occurredAtDate }, audit);
    res.json({ ok: true, data });
  } catch (e) {
    next(e);
  }
});

/** POST /movements/:id/lines (accountant|admin) — thêm dòng */
r.post("/:id/lines", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const movementId = req.params.id;
    await ensureMovementNotLocked(movementId);

    const { itemId, fromLocationId, toLocationId, qty, note, unitCost } = req.body;

    const audit = requireUserOr401(req, res);
    if (!audit) return;

    const data = await addLine(
      movementId,
      { itemId, fromLocationId, toLocationId, qty, note, unitCost },
      audit
    );

    res.json({ ok: true, data });
  } catch (e) {
    next(e);
  }
});

/** PUT /movements/lines/:lineId (accountant|admin) — sửa dòng */
r.put("/lines/:lineId", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const lineId = req.params.lineId;
    await ensureMovementLineNotLocked(lineId);

    const audit = requireUserOr401(req, res);
    if (!audit) return;

    const data = await updateLine(lineId, req.body, audit);
    res.json({ ok: true, data });
  } catch (e) {
    next(e);
  }
});

/** DELETE /movements/lines/:lineId (accountant|admin) — xoá dòng */
r.delete("/lines/:lineId", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const lineId = req.params.lineId;
    await ensureMovementLineNotLocked(lineId);

    const audit = requireUserOr401(req, res);
    if (!audit) return;

    const data = await deleteLine(lineId, audit);
    res.json({ ok: true, data });
  } catch (e) {
    next(e);
  }
});

/** POST /movements/:id/post (accountant|admin) — ghi sổ */
r.post("/:id/post", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const movementId = req.params.id;
    await ensureMovementNotLocked(movementId);

    const audit = requireUserOr401(req, res);
    if (!audit) return;

    const data = await postMovement(movementId, audit);
    res.json({ ok: true, data });
  } catch (e) {
    next(e);
  }
});

export default r;
