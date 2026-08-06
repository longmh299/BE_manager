// src/services/quoteDocuments.service.ts
import { PrismaClient, Prisma } from "@prisma/client";
import { auditLog, type AuditCtx } from "./audit.service";
import { isDocx, patchDocxContact } from "../lib/docxContactPatch";

const prisma = new PrismaClient();

function httpError(status: number, message: string) {
  const err: any = new Error(message);
  err.statusCode = status;
  return err;
}

// Danh sách: KHÔNG select fileData để tránh tải nặng khi liệt kê nhiều file
const LIST_SELECT = {
  id: true,
  title: true,
  machineCode: true,
  category: true,
  note: true,
  fileName: true,
  mimeType: true,
  fileSize: true,
  createdAt: true,
  updatedAt: true,
  uploadedBy: { select: { id: true, username: true } },
} as const;

export async function listQuoteDocuments(params: {
  q?: string;
  category?: string;
  page?: number;
  pageSize?: number;
}) {
  const { q, category } = params;
  const page = Math.max(1, params.page ?? 1);
  const pageSize = Math.min(200, Math.max(1, params.pageSize ?? 20));

  const where: Prisma.QuoteDocumentWhereInput = {
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
    prisma.quoteDocument.findMany({
      where,
      select: LIST_SELECT,
      orderBy: [{ createdAt: "desc" }],
      skip: (page - 1) * pageSize,
      take: pageSize,
    }),
    prisma.quoteDocument.count({ where }),
  ]);

  return { items, total, page, pageSize };
}

export async function getQuoteDocumentMeta(id: string) {
  const doc = await prisma.quoteDocument.findUnique({ where: { id }, select: LIST_SELECT });
  if (!doc) throw httpError(404, "Không tìm thấy file báo giá");
  return doc;
}

/** Lấy cả nội dung file để stream download */
export async function getQuoteDocumentFile(id: string) {
  const doc = await prisma.quoteDocument.findUnique({ where: { id } });
  if (!doc) throw httpError(404, "Không tìm thấy file báo giá");
  return doc;
}

/**
 * ✅ Lấy file để tải xuống, TỰ ĐỘNG thay "Email:"/"Mobile:" trong file .docx
 * bằng email/SĐT của người đang tải (nếu người đó đã lưu trong hồ sơ cá nhân).
 * File gốc lưu trong kho KHÔNG bị đổi — chỉ tạo bản vá tạm trong bộ nhớ để trả
 * về ngay lúc này. Nếu không phải .docx, hoặc user chưa lưu email/phone, hoặc
 * file không khớp mẫu "Email:"/"Mobile:" -> trả nguyên file gốc, không lỗi.
 */
export async function getQuoteDocumentFileForDownload(id: string, userId?: string) {
  const doc = await prisma.quoteDocument.findUnique({ where: { id } });
  if (!doc) throw httpError(404, "Không tìm thấy file báo giá");

  if (!userId || !isDocx(doc.mimeType, doc.fileName)) {
    return { ...doc, fileData: Buffer.from(doc.fileData) };
  }

  const user = await prisma.user.findUnique({
    where: { id: userId },
    select: { email: true, phone: true },
  });
  if (!user || (!user.email && !user.phone)) {
    return { ...doc, fileData: Buffer.from(doc.fileData) };
  }

  const { buffer, replacedEmail, replacedPhone } = await patchDocxContact(
    Buffer.from(doc.fileData),
    { email: user.email, phone: user.phone }
  );

  return { ...doc, fileData: buffer, patchedEmail: replacedEmail, patchedPhone: replacedPhone };
}

export type CreateQuoteDocumentInput = {
  title: string;
  machineCode?: string | null;
  category?: string | null;
  note?: string | null;
  file: {
    originalname: string;
    mimetype: string;
    size: number;
    buffer: Buffer;
  };
};

export async function createQuoteDocument(input: CreateQuoteDocumentInput, ctx: AuditCtx) {
  if (!input.title?.trim()) throw httpError(400, "Thiếu tên báo giá");
  if (!input.file) throw httpError(400, "Thiếu file upload");

  const created = await prisma.quoteDocument.create({
    data: {
      title: input.title.trim(),
      machineCode: input.machineCode?.trim() || null,
      category: input.category?.trim() || null,
      note: input.note?.trim() || null,
      fileName: input.file.originalname,
      mimeType: input.file.mimetype,
      fileSize: input.file.size,
      fileData: new Uint8Array(input.file.buffer),
      uploadedById: ctx?.userId ?? null,
    },
    select: LIST_SELECT,
  });

  await auditLog(prisma, {
    userId: ctx?.userId,
    userRole: ctx?.userRole,
    action: "UPLOAD",
    entity: "QuoteDocument",
    entityId: created.id,
    after: created,
    meta: ctx?.meta,
  });

  return created;
}

export async function updateQuoteDocument(
  id: string,
  input: {
    title?: string;
    machineCode?: string | null;
    category?: string | null;
    note?: string | null;
    file?: { originalname: string; mimetype: string; size: number; buffer: Buffer } | null;
  },
  ctx: AuditCtx
) {
  const existing = await prisma.quoteDocument.findUnique({ where: { id } });
  if (!existing) throw httpError(404, "Không tìm thấy file báo giá");

  const updated = await prisma.quoteDocument.update({
    where: { id },
    data: {
      ...(input.title !== undefined ? { title: input.title.trim() } : {}),
      ...(input.machineCode !== undefined ? { machineCode: input.machineCode?.trim() || null } : {}),
      ...(input.category !== undefined ? { category: input.category?.trim() || null } : {}),
      ...(input.note !== undefined ? { note: input.note?.trim() || null } : {}),
      // ✅ chỉ thay nội dung file khi có file mới gửi lên (giá đổi -> báo giá mới)
      ...(input.file
        ? {
            fileName: input.file.originalname,
            mimeType: input.file.mimetype,
            fileSize: input.file.size,
            fileData: new Uint8Array(input.file.buffer),
          }
        : {}),
    },
    select: LIST_SELECT,
  });

  await auditLog(prisma, {
    userId: ctx?.userId,
    userRole: ctx?.userRole,
    action: input.file ? "UPDATE_FILE" : "UPDATE",
    entity: "QuoteDocument",
    entityId: id,
    before: { title: existing.title, fileName: existing.fileName, fileSize: existing.fileSize },
    after: updated,
    meta: ctx?.meta,
  });

  return updated;
}

export async function deleteQuoteDocument(id: string, ctx: AuditCtx) {
  const existing = await prisma.quoteDocument.findUnique({ where: { id } });
  if (!existing) throw httpError(404, "Không tìm thấy file báo giá");

  await prisma.quoteDocument.delete({ where: { id } });

  await auditLog(prisma, {
    userId: ctx?.userId,
    userRole: ctx?.userRole,
    action: "DELETE",
    entity: "QuoteDocument",
    entityId: id,
    before: { title: existing.title, fileName: existing.fileName },
    meta: ctx?.meta,
  });

  return { ok: true };
}