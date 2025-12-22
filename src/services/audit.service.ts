// src/services/audit.service.ts
import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

/** Context truyền từ route xuống */
export type AuditCtx =
  | {
      userId?: string;
      userRole?: string;
      meta?: any; // ip, userAgent, path, method...
    }
  | undefined;

export type AuditInput = {
  userId?: string;
  userRole?: string;

  action: string;
  entity: string;
  entityId?: string;

  before?: any;
  after?: any;
  meta?: any;
};

/**
 * Ghi audit log
 * ❗ Không throw lỗi – audit không được làm gãy nghiệp vụ
 */
export async function auditLog(txOrPrisma: PrismaClient | any, input: AuditInput) {
  try {
    if (!input.userId) return;

    await txOrPrisma.auditLog.create({
      data: {
        userId: input.userId,
        userRole: input.userRole ?? null,

        action: input.action,
        entity: input.entity,
        entityId: input.entityId ?? null,

        before: input.before ?? undefined,
        after: input.after ?? undefined,
        meta: input.meta ?? undefined,
      },
    });
  } catch (err) {
    console.error("[AUDIT_LOG_FAILED]", err);
  }
}
