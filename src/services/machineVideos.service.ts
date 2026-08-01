// src/services/machineVideos.service.ts
import { PrismaClient, Prisma, MachineVideoStatus } from "@prisma/client";
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
  createdAt: true,
  updatedAt: true,
  uploadedBy: { select: { id: true, username: true } },
} as const;

export async function listMachineVideos(params: {
  q?: string;
  category?: string;
  page?: number;
  pageSize?: number;
}) {
  const { q, category } = params;
  const page = Math.max(1, params.page ?? 1);
  const pageSize = Math.min(200, Math.max(1, params.pageSize ?? 20));

  const where: Prisma.MachineVideoWhereInput = {
    status: "READY", // ✅ chưa upload xong thì không hiện cho người khác thấy
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
    prisma.machineVideo.findMany({
      where,
      select: LIST_SELECT,
      orderBy: [{ createdAt: "desc" }],
      skip: (page - 1) * pageSize,
      take: pageSize,
    }),
    prisma.machineVideo.count({ where }),
  ]);

  return { items, total, page, pageSize };
}

/** BƯỚC 1 của upload: tạo record + trả URL để FE PUT thẳng file lên R2 */
export async function initMachineVideoUpload(
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
  if (!input.title?.trim()) throw httpError(400, "Thiếu tên video");
  if (!input.fileName) throw httpError(400, "Thiếu tên file");

  const safeName = input.fileName.replace(/[^\w.\-]+/g, "_");
  const r2Key = `videos/${randomUUID()}/${safeName}`;

  const created = await prisma.machineVideo.create({
    data: {
      title: input.title.trim(),
      machineCode: input.machineCode?.trim() || null,
      category: input.category?.trim() || null,
      note: input.note?.trim() || null,
      fileName: input.fileName,
      mimeType: input.mimeType || "application/octet-stream",
      fileSize: input.fileSize || 0,
      r2Key,
      status: MachineVideoStatus.PENDING,
      uploadedById: ctx?.userId ?? null,
    },
    select: LIST_SELECT,
  });

  const uploadUrl = await getUploadUrl(r2Key, input.mimeType || "application/octet-stream");

  return { id: created.id, uploadUrl };
}

/** BƯỚC 2 của upload: FE báo đã PUT xong lên R2 -> xác nhận & đổi trạng thái READY */
export async function completeMachineVideoUpload(id: string, ctx: AuditCtx) {
  const existing = await prisma.machineVideo.findUnique({ where: { id } });
  if (!existing) throw httpError(404, "Không tìm thấy video");

  const ok = await objectExists(existing.r2Key);
  if (!ok) {
    await prisma.machineVideo.update({ where: { id }, data: { status: MachineVideoStatus.FAILED } });
    throw httpError(400, "Chưa thấy file trên storage — upload có thể chưa xong hoặc bị lỗi, thử lại.");
  }

  const updated = await prisma.machineVideo.update({
    where: { id },
    data: { status: MachineVideoStatus.READY },
    select: LIST_SELECT,
  });

  await auditLog(prisma, {
    userId: ctx?.userId,
    userRole: ctx?.userRole,
    action: "UPLOAD",
    entity: "MachineVideo",
    entityId: id,
    after: updated,
    meta: ctx?.meta,
  });

  return updated;
}

export async function getMachineVideoDownloadUrl(id: string) {
  const doc = await prisma.machineVideo.findUnique({ where: { id } });
  if (!doc) throw httpError(404, "Không tìm thấy video");
  if (doc.status !== "READY") throw httpError(400, "Video chưa upload xong.");

  const url = await getDownloadUrl(doc.r2Key, doc.fileName);
  return { url, fileName: doc.fileName };
}

/** ✅ URL để xem trước (stream) video ngay trong trang, không ép tải xuống */
export async function getMachineVideoPreviewUrl(id: string) {
  const doc = await prisma.machineVideo.findUnique({ where: { id } });
  if (!doc) throw httpError(404, "Không tìm thấy video");
  if (doc.status !== "READY") throw httpError(400, "Video chưa upload xong.");

  const url = await getPreviewUrl(doc.r2Key);
  return { url, mimeType: doc.mimeType };
}

export async function updateMachineVideoMeta(
  id: string,
  input: { title?: string; machineCode?: string | null; category?: string | null; note?: string | null },
  ctx: AuditCtx
) {
  const existing = await prisma.machineVideo.findUnique({ where: { id } });
  if (!existing) throw httpError(404, "Không tìm thấy video");

  const updated = await prisma.machineVideo.update({
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
    entity: "MachineVideo",
    entityId: id,
    before: existing,
    after: updated,
    meta: ctx?.meta,
  });

  return updated;
}

export async function deleteMachineVideo(id: string, ctx: AuditCtx) {
  const existing = await prisma.machineVideo.findUnique({ where: { id } });
  if (!existing) throw httpError(404, "Không tìm thấy video");

  await deleteObject(existing.r2Key); // xoá file thật trên R2 trước
  await prisma.machineVideo.delete({ where: { id } });

  await auditLog(prisma, {
    userId: ctx?.userId,
    userRole: ctx?.userRole,
    action: "DELETE",
    entity: "MachineVideo",
    entityId: id,
    before: { title: existing.title, fileName: existing.fileName },
    meta: ctx?.meta,
  });

  return { ok: true };
}