// src/services/translateHistory.service.ts
// ✅ LỊCH SỬ TÀI LIỆU ĐÃ DỊCH
//   - Dịch xong FE tự lưu: bản dịch vào Neon (bảng TranslatedDoc), file gốc lên R2
//     (thư mục tai-lieu-dich/<năm-tháng>/<id>/ — trình duyệt PUT thẳng lên R2, không qua backend).
//   - Mở lại: xem / Hỏi AI / làm Báo giá mà KHÔNG phải dịch lại (không tốn tiền AI).
//   - Quyền: người dịch chỉ thấy tài liệu của mình, admin thấy hết.
import { PrismaClient, Prisma } from "@prisma/client";
import { getUploadUrl, getDownloadUrl, deleteObject, objectExists } from "../lib/r2Client";
import { keySafe, vnStamp } from "../utils/machineMatch";
import { httpError } from "./translate.service";

const prisma = new PrismaClient();

type Who = { id: string; role: string };
type Source = { key: string; name: string; type: string; size: number; uploaded: boolean };

const KINDS = ["pdf", "images", "docx", "xlsx"] as const;
const MAX_FILES = 60;
const MAX_FILE_SIZE = 200 * 1024 * 1024; // 200MB / file

const isAdmin = (u: Who) => u.role === "admin";
const str = (v: unknown, max = 300) => (typeof v === "string" ? v.trim().slice(0, max) : "");
const json = (v: unknown) => (v === undefined ? undefined : v === null ? Prisma.JsonNull : (v as Prisma.InputJsonValue));

function pageCountOf(pages: unknown, sheets: unknown) {
  if (Array.isArray(pages)) return pages.length;
  if (Array.isArray(sheets)) return sheets.length;
  return 0;
}

/** Lấy tài liệu + kiểm tra quyền (chủ tài liệu hoặc admin) */
async function own(id: string, u: Who) {
  const doc = await prisma.translatedDoc.findUnique({ where: { id } });
  if (!doc) throw httpError(404, "Không tìm thấy tài liệu");
  if (doc.createdById !== u.id && !isAdmin(u)) throw httpError(403, "Bạn không có quyền với tài liệu này");
  return doc;
}

/**
 * Tạo bản lưu khi dịch xong.
 * Body: { title, kind, context?, quality?, pages?|sheets?, usage?, files: [{ name, type, size }] }
 * -> { id, uploads: [{ index, url }] }  (FE PUT từng file gốc lên url rồi gọi markUploaded)
 */
export async function createDoc(u: Who, body: any) {
  const kind = String(body?.kind || "");
  if (!KINDS.includes(kind as any)) throw httpError(400, "Loại tài liệu không hợp lệ");
  const files: any[] = Array.isArray(body?.files) ? body.files.slice(0, MAX_FILES) : [];
  for (const f of files) {
    if (Number(f?.size) > MAX_FILE_SIZE) throw httpError(400, `File "${str(f?.name, 80)}" quá lớn (tối đa 200MB)`);
  }
  const usage = body?.usage ?? null;

  const doc = await prisma.translatedDoc.create({
    data: {
      title: str(body?.title, 200) || "Tài liệu",
      kind,
      context: str(body?.context, 300) || null,
      quality: body?.quality === "deep" ? "deep" : "fast",
      pages: json(body?.pages ?? null),
      sheets: json(body?.sheets ?? null),
      pageCount: pageCountOf(body?.pages, body?.sheets),
      usage: json(usage),
      cost: Number(usage?.cost) || 0,
      createdById: u.id,
    },
  });

  const { ym } = vnStamp();
  const sources: Source[] = files.map((f, i) => {
    const name = str(f?.name, 150) || `file-${i + 1}`;
    const ext = /\.([A-Za-z0-9]{2,5})$/.exec(name)?.[1]?.toLowerCase() || "bin";
    return {
      key: `tai-lieu-dich/${ym}/${doc.id}/${String(i + 1).padStart(2, "0")}-${keySafe(name.replace(/\.[^.]+$/, "")).slice(0, 60)}.${ext}`,
      name,
      type: str(f?.type, 100) || "application/octet-stream",
      size: Number(f?.size) || 0,
      uploaded: false,
    };
  });
  if (sources.length) {
    await prisma.translatedDoc.update({ where: { id: doc.id }, data: { sources: sources as any } });
  }
  const uploads = await Promise.all(sources.map(async (s, index) => ({ index, url: await getUploadUrl(s.key, s.type) })));
  return { id: doc.id, uploads };
}

/** FE báo đã PUT xong file gốc -> kiểm tra thật trên R2 rồi đánh dấu */
export async function markUploaded(u: Who, id: string, indexes: unknown) {
  const doc = await own(id, u);
  const sources = (doc.sources as unknown as Source[]) || [];
  const want = new Set((Array.isArray(indexes) ? indexes : []).map(Number));
  for (const [i, s] of sources.entries()) {
    if (want.has(i) && !s.uploaded) s.uploaded = await objectExists(s.key);
  }
  await prisma.translatedDoc.update({ where: { id }, data: { sources: sources as any } });
  return { uploaded: sources.filter((s) => s.uploaded).length, total: sources.length };
}

/**
 * Danh sách (nhẹ, không kèm nội dung dịch). q: tìm theo tên file / máy / mã máy.
 * Người thường chỉ thấy của mình, admin thấy hết. Phân trang bằng cursor (id cuối trang trước).
 */
export async function listDocs(u: Who, query: any) {
  const q = str(query?.q, 100);
  const take = Math.min(Math.max(Number(query?.take) || 30, 1), 100);
  const where: Prisma.TranslatedDocWhereInput = {
    ...(isAdmin(u) ? {} : { createdById: u.id }),
    ...(q
      ? {
          OR: [
            { title: { contains: q, mode: "insensitive" } },
            { context: { contains: q, mode: "insensitive" } },
            { machineCode: { contains: q, mode: "insensitive" } },
          ],
        }
      : {}),
  };
  const rows = await prisma.translatedDoc.findMany({
    where,
    orderBy: [{ createdAt: "desc" }, { id: "desc" }],
    take: take + 1,
    ...(query?.cursor ? { cursor: { id: String(query.cursor) }, skip: 1 } : {}),
    select: {
      id: true,
      title: true,
      kind: true,
      context: true,
      quality: true,
      pageCount: true,
      cost: true,
      machineCode: true,
      createdAt: true,
      quoteDraft: true,
      createdBy: { select: { username: true } },
    },
  });
  const items = rows.slice(0, take).map(({ quoteDraft, createdBy, ...r }) => ({
    ...r,
    hasQuote: !!quoteDraft,
    createdBy: createdBy?.username ?? null,
  }));
  return { items, nextCursor: rows.length > take ? items[items.length - 1].id : null };
}

/** Mở lại 1 tài liệu (đầy đủ bản dịch + nháp báo giá) */
export async function getDoc(u: Who, id: string) {
  const doc = await own(id, u);
  const sources = ((doc.sources as unknown as Source[]) || []).map(({ name, type, size, uploaded }) => ({ name, type, size, uploaded }));
  return { ...doc, sources };
}

/**
 * Cập nhật sau khi: dịch lại đoạn sót / phần lỗi, hỏi AI (chi phí), sửa nháp báo giá.
 * Body (đều không bắt buộc): { pages, sheets, usage, quoteDraft }
 */
export async function updateDoc(u: Who, id: string, body: any) {
  await own(id, u);
  const data: Prisma.TranslatedDocUpdateInput = {};
  if (body?.pages !== undefined) data.pages = json(body.pages);
  if (body?.sheets !== undefined) data.sheets = json(body.sheets);
  if (body?.pages !== undefined || body?.sheets !== undefined) data.pageCount = pageCountOf(body?.pages, body?.sheets);
  if (body?.usage !== undefined) {
    data.usage = json(body.usage);
    data.cost = Number(body.usage?.cost) || 0;
  }
  if (body?.quoteDraft !== undefined) {
    data.quoteDraft = json(body.quoteDraft);
    data.machineCode = str(body.quoteDraft?.q?.maHang, 60) || null;
  }
  const doc = await prisma.translatedDoc.update({ where: { id }, data, select: { id: true, updatedAt: true } });
  return doc;
}

/** Link tải file gốc (hết hạn sau 10 phút) */
export async function sourceUrls(u: Who, id: string) {
  const doc = await own(id, u);
  const sources = ((doc.sources as unknown as Source[]) || []).filter((s) => s.uploaded);
  return {
    files: await Promise.all(sources.map(async (s) => ({ name: s.name, size: s.size, url: await getDownloadUrl(s.key, s.name) }))),
  };
}

/** Xoá tài liệu + file gốc trên R2 */
export async function deleteDoc(u: Who, id: string) {
  const doc = await own(id, u);
  for (const s of (doc.sources as unknown as Source[]) || []) {
    await deleteObject(s.key).catch(() => undefined); // file chưa upload xong thì bỏ qua
  }
  await prisma.translatedDoc.delete({ where: { id } });
  return { ok: true };
}