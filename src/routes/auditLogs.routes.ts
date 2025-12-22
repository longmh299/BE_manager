// src/routes/auditLogs.routes.ts
import { Router } from "express";
import { requireAuth, getUser } from "../middlewares/auth";
import { listAuditLogs, getAuditLogById } from "../services/auditLogs.service";

const r = Router();
r.use(requireAuth);

function isAdminOrAccountant(role: string) {
  return role === "admin" || role === "accountant";
}

/** staff chỉ thấy log của chính mình + ẩn before/after (tuỳ bạn) */
function maskForStaff(row: any) {
  if (!row) return row;
  const cloned = JSON.parse(JSON.stringify(row));
  delete cloned.before;
  delete cloned.after;

  // giữ meta "nhẹ" thôi
  if (cloned.meta && typeof cloned.meta === "object") {
    const { ip, path, method, userAgent } = cloned.meta;
    cloned.meta = { ip, path, method, userAgent };
  }
  return cloned;
}

/**
 * GET /audit-logs
 * Query: q, entity, entityId, action, userId, from, to, page, pageSize
 */
r.get("/", async (req, res, next) => {
  try {
    const u = getUser(req)!;

    const {
      q = "",
      entity = "",
      entityId = "",
      action = "",
      userId = "",
      from = "",
      to = "",
      page = "1",
      pageSize = "30",
    } = req.query as any;

    // ✅ RBAC: staff chỉ được xem log của mình
    const safeUserId = isAdminOrAccountant(u.role) ? (userId || undefined) : u.id;

    const result = await listAuditLogs({
      q: q ? String(q) : undefined,
      entity: entity ? String(entity) : undefined,
      entityId: entityId ? String(entityId) : undefined,
      action: action ? String(action) : undefined,
      userId: safeUserId ? String(safeUserId) : undefined,
      from: from ? String(from) : undefined,
      to: to ? String(to) : undefined,
      page: Number(page) || 1,
      pageSize: Number(pageSize) || 30,
    });

    const rows = isAdminOrAccountant(u.role)
      ? result.rows
      : (result.rows || []).map((x: any) => maskForStaff(x));

    res.json({
      ok: true,
      total: result.total,
      page: result.page,
      pageSize: result.pageSize,
      rows,
    });
  } catch (e) {
    next(e);
  }
});

/**
 * GET /audit-logs/:id
 */
r.get("/:id", async (req, res, next) => {
  try {
    const u = getUser(req)!;

    const row = await getAuditLogById(req.params.id);
    if (!row) return res.status(404).json({ ok: false, message: "Audit log not found" });

    if (!isAdminOrAccountant(u.role)) {
      // staff chỉ xem log của họ
      if (String(row.userId || "") !== String(u.id)) {
        return res.status(403).json({ ok: false, message: "Forbidden" });
      }
      return res.json({ ok: true, data: maskForStaff(row) });
    }

    res.json({ ok: true, data: row });
  } catch (e) {
    next(e);
  }
});

export default r;
