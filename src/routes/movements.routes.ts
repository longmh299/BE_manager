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
  ensureDateNotLocked,
} from "../services/periodLock.service";

const prisma = new PrismaClient();
const r = Router();

r.use(requireAuth);

// ✅ movements module chỉ cho ADJUST / REVALUE
function assertMovementsTypeOr400(type: any) {
  if (!type) return;
  if (type !== "ADJUST" && type !== "REVALUE") {
    const err: any = new Error("Movements chỉ hỗ trợ loại: ADJUST và REVALUE.");
    err.statusCode = 400;
    throw err;
  }
}

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

/** GET /movements?type=&posted=&warehouseId=&page=&pageSize=&q=
 * ✅ trả shape ổn định: { ok, rows, total, page, pageSize }
 * ✅ type chỉ ADJUST/REVALUE (nếu có truyền)
 */
r.get("/", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const {
      type,
      posted,
      warehouseId,
      q = "",
      page = "1",
      pageSize = "20",
    } = req.query as any;

    if (type) assertMovementsTypeOr400(type);

    const where: any = {};
    if (q) {
      where.OR = [
        { refNo: { contains: String(q), mode: "insensitive" } },
        { note: { contains: String(q), mode: "insensitive" } },
      ];
    }
    if (type) where.type = type;
    if (posted !== undefined) where.posted = posted === "true";
    if (warehouseId) where.warehouseId = String(warehouseId);

    // ✅ mặc định list movements module chỉ show ADJUST/REVALUE
    if (!type) where.type = { in: ["ADJUST", "REVALUE"] };

    const pageNum = Math.max(1, Number(page) || 1);
    const pageSizeNum = Math.min(200, Math.max(1, Number(pageSize) || 20));

    const [total, rows] = await Promise.all([
      prisma.movement.count({ where }),
      prisma.movement.findMany({
        where,
        include: {
          lines: { include: { item: true, fromLoc: true, toLoc: true } },
          invoice: true,
        },
        orderBy: [{ occurredAt: "desc" }, { createdAt: "desc" }],
        skip: (pageNum - 1) * pageSizeNum,
        take: pageSizeNum,
      }),
    ]);

    res.json({ ok: true, rows, total, page: pageNum, pageSize: pageSizeNum });
  } catch (e) {
    next(e);
  }
});

/** GET /movements/:id
 * ✅ vẫn cho xem mọi movement theo id (nhưng UI/menu movements chỉ mở ADJUST/REVALUE)
 */
r.get("/:id", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const data = await prisma.movement.findUnique({
      where: { id: req.params.id },
      include: {
        lines: { include: { item: true, fromLoc: true, toLoc: true } },
        invoice: true,
      },
    });
    res.json({ ok: true, data });
  } catch (e) {
    next(e);
  }
});

/** POST /movements  (accountant|admin) — tạo phiếu nháp
 * ✅ chỉ cho ADJUST/REVALUE
 * ✅ truyền warehouseId vào service luôn (đỡ update lại)
 */
r.post("/", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const { type, refNo, note, occurredAt, warehouseId } = req.body as {
      type: MovementType;
      refNo?: string;
      note?: string;
      occurredAt?: string; // "yyyy-mm-dd"
      warehouseId?: string | null;
    };

    assertMovementsTypeOr400(type);

    const audit = requireUserOr401(req, res);
    if (!audit) return;

    let occurredAtDate: Date | undefined = undefined;
    if (occurredAt) {
      const d = parseYmdToLocalStart(occurredAt);
      if (!d) {
        return res
          .status(400)
          .json({ ok: false, message: "occurredAt không hợp lệ (yyyy-mm-dd)" });
      }
      await ensureDateNotLocked(d, "tạo chứng từ");
      occurredAtDate = d;
    }

    const created = await createDraft(
      type,
      {
        refNo,
        note,
        occurredAt: occurredAtDate,
        warehouseId: warehouseId ? String(warehouseId) : null,
      },
      audit
    );

    res.json({ ok: true, data: created });
  } catch (e) {
    next(e);
  }
});

/** POST /movements/:id/lines (accountant|admin) — thêm dòng
 * ✅ service sẽ chặn from/to (1 kho) và validate theo type
 */
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
