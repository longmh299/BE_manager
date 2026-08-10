// src/services/machineImages.service.ts
import { PrismaClient, Prisma, MachineImageStatus } from "@prisma/client";
import { auditLog, type AuditCtx } from "./audit.service";
import { getUploadUrl, getDownloadUrl, getPreviewUrl, deleteObject, objectExists } from "../lib/r2Client";
import { randomUUID } from "crypto";

const prisma = new PrismaClient();

function httpError(status: number, message: string) {
  const err: any = new Error(message);
  err.statusCode = status;
  return err;
}

const LIST_SELECT = {
  id: true,
  title: true,
  machineCode: true,
  category: true,
  note: true,
  fileName: true,
  mimeType: true,
  fileSize: true,
  status: true,
  r2Key: true,
  createdAt: true,
  updatedAt: true,
  uploadedBy: { select: { id: true, username: true } },
} as const;

export async function listMachineImages(params: {
  q?: string;
  category?: string;
  page?: number;
  pageSize?: number;
}) {
  const { q, category } = params;
  const page = Math.max(1, params.page ?? 1);
  const pageSize = Math.min(200, Math.max(1, params.pageSize ?? 24));

  const where: Prisma.MachineImageWhereInput = {
    status: "READY",
    ...(category ? { category } : {}),
    ...(q
      ? {
          OR: [
            { title: { contains: q, mode: "insensitive" } },
            { machineCode: { contains: q, mode: "insensitive" } },
            { fileName: { contains: q, mode: "insensitive" } },
          ],
        }
      : {}),
  };

  const [items, total] = await Promise.all([
    prisma.machineImage.findMany({
      where,
      select: LIST_SELECT,
      orderBy: [{ createdAt: "desc" }],
      skip: (page - 1) * pageSize,
      take: pageSize,
    }),
    prisma.machineImage.count({ where }),
  ]);

  const itemsWithUrl = await Promise.all(
    items.map(async ({ r2Key, ...doc }) => ({
      ...doc,
      previewUrl: await getPreviewUrl(r2Key),
    }))
  );

  return { items: itemsWithUrl, total, page, pageSize };
}

export async function initMachineImageUpload(
  input: {
    title: string;
    machineCode?: string | null;
    category?: string | null;
    note?: string | null;
    fileName: string;
    mimeType: string;
    fileSize: number;
  },
  ctx: AuditCtx
) {
  if (!input.title?.trim()) throw httpError(400, "Thiếu tên ảnh");
  if (!input.fileName) throw httpError(400, "Thiếu tên file");

  const allowedMime = ["image/jpeg", "image/png", "image/webp", "image/gif"];
  if (input.mimeType && !allowedMime.includes(input.mimeType)) {
    throw httpError(400, "Chỉ chấp nhận ảnh JPEG, PNG, WEBP, GIF");
  }

  const safeName = input.fileName.replace(/[^\w.\-]+/g, "_");
  const r2Key = `images/${randomUUID()}/${safeName}`;

  const created = await prisma.machineImage.create({
    data: {
      title: input.title.trim(),
      machineCode: input.machineCode?.trim() || null,
      category: input.category?.trim() || null,
      note: input.note?.trim() || null,
      fileName: input.fileName,
      mimeType: input.mimeType || "application/octet-stream",
      fileSize: input.fileSize || 0,
      r2Key,
      status: MachineImageStatus.PENDING,
      uploadedById: ctx?.userId ?? null,
    },
    select: LIST_SELECT,
  });

  const uploadUrl = await getUploadUrl(r2Key, input.mimeType || "application/octet-stream");

  return { id: created.id, uploadUrl };
}

export async function completeMachineImageUpload(id: string, ctx: AuditCtx) {
  const existing = await prisma.machineImage.findUnique({ where: { id } });
  if (!existing) throw httpError(404, "Không tìm thấy ảnh");

  const ok = await objectExists(existing.r2Key);
  if (!ok) {
    await prisma.machineImage.update({ where: { id }, data: { status: MachineImageStatus.FAILED } });
    throw httpError(400, "Chưa thấy file trên storage — upload có thể chưa xong hoặc bị lỗi, thử lại.");
  }

  const updated = await prisma.machineImage.update({
    where: { id },
    data: { status: MachineImageStatus.READY },
    select: LIST_SELECT,
  });

  await auditLog(prisma, {
    userId: ctx?.userId,
    userRole: ctx?.userRole,
    action: "UPLOAD",
    entity: "MachineImage",
    entityId: id,
    after: updated,
    meta: ctx?.meta,
  });

  return updated;
}

export async function getMachineImageDownloadUrl(id: string) {
  const doc = await prisma.machineImage.findUnique({ where: { id } });
  if (!doc) throw httpError(404, "Không tìm thấy ảnh");
  if (doc.status !== "READY") throw httpError(400, "Ảnh chưa upload xong.");

  const url = await getDownloadUrl(doc.r2Key, doc.fileName);
  return { url, fileName: doc.fileName };
}

export async function getMachineImagePreviewUrl(id: string) {
  const doc = await prisma.machineImage.findUnique({ where: { id } });
  if (!doc) throw httpError(404, "Không tìm thấy ảnh");
  if (doc.status !== "READY") throw httpError(400, "Ảnh chưa upload xong.");

  const url = await getPreviewUrl(doc.r2Key);
  return { url, mimeType: doc.mimeType };
}

export async function updateMachineImageMeta(
  id: string,
  input: { title?: string; machineCode?: string | null; category?: string | null; note?: string | null },
  ctx: AuditCtx
) {
  const existing = await prisma.machineImage.findUnique({ where: { id } });
  if (!existing) throw httpError(404, "Không tìm thấy ảnh");

  const updated = await prisma.machineImage.update({
    where: { id },
    data: {
      ...(input.title !== undefined ? { title: input.title.trim() } : {}),
      ...(input.machineCode !== undefined ? { machineCode: input.machineCode?.trim() || null } : {}),
      ...(input.category !== undefined ? { category: input.category?.trim() || null } : {}),
      ...(input.note !== undefined ? { note: input.note?.trim() || null } : {}),
    },
    select: LIST_SELECT,
  });

  await auditLog(prisma, {
    userId: ctx?.userId,
    userRole: ctx?.userRole,
    action: "UPDATE",
    entity: "MachineImage",
    entityId: id,
    before: existing,
    after: updated,
    meta: ctx?.meta,
  });

  return updated;
}

export async function deleteMachineImage(id: string, ctx: AuditCtx) {
  const existing = await prisma.machineImage.findUnique({ where: { id } });
  if (!existing) throw httpError(404, "Không tìm thấy ảnh");

  await deleteObject(existing.r2Key);
  await prisma.machineImage.delete({ where: { id } });

  await auditLog(prisma, {
    userId: ctx?.userId,
    userRole: ctx?.userRole,
    action: "DELETE",
    entity: "MachineImage",
    entityId: id,
    before: { title: existing.title, fileName: existing.fileName },
    meta: ctx?.meta,
  });

  return { ok: true };
}