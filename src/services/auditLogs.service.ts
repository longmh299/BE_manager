// src/services/auditLogs.service.ts
import { PrismaClient, Prisma } from "@prisma/client";

const prisma = new PrismaClient();

export type ListAuditLogsParams = {
  q?: string;
  entity?: string;
  entityId?: string;
  action?: string;
  userId?: string;
  from?: string; // YYYY-MM-DD hoặc ISO
  to?: string; // YYYY-MM-DD hoặc ISO
  page?: number;
  pageSize?: number;
};

function toEndOfDay(d: Date) {
  const x = new Date(d);
  x.setHours(23, 59, 59, 999);
  return x;
}

export async function listAuditLogs(params: ListAuditLogsParams) {
  const page = Number(params.page || 1);
  const pageSize = Number(params.pageSize || 30);

  const where: Prisma.AuditLogWhereInput = {};

  if (params.entity) where.entity = String(params.entity);
  if (params.entityId) where.entityId = String(params.entityId);
  if (params.action) where.action = String(params.action);
  if (params.userId) where.userId = String(params.userId);

  if (params.from || params.to) {
    where.createdAt = {};
    if (params.from) (where.createdAt as any).gte = new Date(params.from);
    if (params.to) (where.createdAt as any).lte = toEndOfDay(new Date(params.to));
  }

  if (params.q && String(params.q).trim()) {
    const q = String(params.q).trim();
    where.OR = [
      { action: { contains: q, mode: "insensitive" } },
      { entity: { contains: q, mode: "insensitive" } },
      { entityId: { contains: q, mode: "insensitive" } },
      // meta/before/after là Json => không search ổn định bằng contains
    ];
  }

  const skip = (page - 1) * pageSize;

  const [total, rows] = await Promise.all([
    prisma.auditLog.count({ where }),
    prisma.auditLog.findMany({
      where,
      orderBy: { createdAt: "desc" },
      skip,
      take: pageSize,
      include: {
        user: {
          select: {
            id: true,
            username: true,
            role: true,
          },
        },
      },
    }),
  ]);

  return { total, page, pageSize, rows };
}

export async function getAuditLogById(id: string) {
  return prisma.auditLog.findUnique({
    where: { id },
    include: {
      user: {
        select: {
          id: true,
          username: true,
          role: true,
        },
      },
    },
  });
}
