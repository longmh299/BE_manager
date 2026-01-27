import { Router } from "express";
import { prisma } from "../tool/prisma";
import { requireAuth, requireRole, getUser } from "../middlewares/auth";

const r = Router();

function httpError(statusCode: number, message: string) {
  const err: any = new Error(message);
  err.statusCode = statusCode;
  return err;
}

function buildAuditMeta(req: any) {
  return {
    ip: req.ip,
    userAgent: req.headers?.["user-agent"],
    path: req.originalUrl || req.url,
    method: req.method,
  };
}

/**
 * PATCH /api/invoices/:id/status
 * Body: { toStatus: "DRAFT" | "APPROVED", reason?: string }
 *
 * Rule:
 * - chỉ admin
 * - APPROVED -> DRAFT: allocations = 0, bắt buộc reason
 * - DRAFT -> APPROVED: ok
 * - KHÔNG tự chỉnh paidAmount/paymentStatus (NV tự cân)
 */
r.use(requireAuth);

r.patch("/:id/status", requireRole("admin"), async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const id = String(req.params.id || "");
    const { toStatus, reason } = (req.body ?? {}) as {
      toStatus?: "DRAFT" | "APPROVED";
      reason?: string;
    };

    if (!id) throw httpError(400, "Missing invoice id");
    if (toStatus !== "DRAFT" && toStatus !== "APPROVED") {
      throw httpError(400, "Invalid toStatus (must be DRAFT or APPROVED)");
    }

    const updated = await prisma.$transaction(async (tx) => {
      const inv = await tx.invoice.findUnique({
        where: { id },
        include: {
          allocations: { select: { id: true } },
        },
      });

      if (!inv) throw httpError(404, "Invoice not found");
      if (inv.cancelledAt) throw httpError(409, "Invoice is CANCELLED");

      // APPROVED -> DRAFT
      if (toStatus === "DRAFT") {
        if (inv.status !== "APPROVED") {
          throw httpError(409, "Chỉ được chuyển APPROVED → DRAFT.");
        }
        // if (!reason || !reason.trim()) {
        //   throw httpError(400, "Bắt buộc nhập lý do khi chuyển về DRAFT.");
        // }
        if ((inv.allocations?.length ?? 0) > 0) {
          throw httpError(
            409,
            "Hóa đơn đã phát sinh thanh toán (allocations), không thể chuyển về DRAFT."
          );
        }

        const after = await tx.invoice.update({
          where: { id },
          data: {
            status: "DRAFT",
            approvedAt: null,
            approvedById: null,
            // ✅ không đụng paidAmount/paymentStatus
          },
        });

        await tx.auditLog.create({
          data: {
            userId: u.id,
            userRole: u.role,
            action: "INVOICE_STATUS_CHANGE",
            entity: "Invoice",
            entityId: id,
            before: {
              status: inv.status,
              approvedAt: inv.approvedAt,
              approvedById: inv.approvedById,
            },
            after: {
              status: after.status,
              approvedAt: after.approvedAt,
              approvedById: after.approvedById,
            },
            meta: {
              // reason: reason.trim(),
              toStatus: "DRAFT",
              ...buildAuditMeta(req),
            },
          },
        });

        return after;
      }

      // DRAFT -> APPROVED
      if (toStatus === "APPROVED") {
        if (inv.status !== "DRAFT") {
          throw httpError(409, "Chỉ được duyệt khi hóa đơn đang DRAFT.");
        }

        const after = await tx.invoice.update({
          where: { id },
          data: {
            status: "APPROVED",
            approvedAt: new Date(),
            approvedById: u.id,
          },
        });

        await tx.auditLog.create({
          data: {
            userId: u.id,
            userRole: u.role,
            action: "INVOICE_STATUS_CHANGE",
            entity: "Invoice",
            entityId: id,
            before: {
              status: inv.status,
              approvedAt: inv.approvedAt,
              approvedById: inv.approvedById,
            },
            after: {
              status: after.status,
              approvedAt: after.approvedAt,
              approvedById: after.approvedById,
            },
            meta: {
              reason: (reason ?? "").trim() || null,
              toStatus: "APPROVED",
              ...buildAuditMeta(req),
            },
          },
        });

        return after;
      }

      throw httpError(400, "Unhandled");
    });

    return res.json({ ok: true, data: updated });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ ok: false, message: e.message });
    next(e);
  }
});

export default r;
