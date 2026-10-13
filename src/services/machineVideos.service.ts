// src/services/machineVideos.service.ts
import { PrismaClient, Prisma, MachineVideoStatus } from "@prisma/client";
import { auditLog, type AuditCtx } from "./audit.service";
import { getUploadUrl, getDownloadUrl, getPreviewUrl, deleteObject, objectExists, moveObject } from "../lib/r2Client";
import { randomBytes } from "crypto";
import { resolveMachine, keySafe, vnStamp, fileExt, modelFromName } from "../utils/machineMatch";

const prisma = new PrismaClient();

// ✅ Tách "video hướng dẫn vận hành" khỏi "video chạy máy" mà KHÔNG cần thêm
// cột mới / chạy migration: dùng field "category" có sẵn làm nhãn ẩn cố định.
// Video nào có category = giá trị này thì được xem là video hướng dẫn, và bị
// loại khỏi danh sách mặc định (tab "Kho video vận hành máy") để 2 tab không
// lẫn vào nhau. Giá trị cố tình đặt lạ để không trùng với "nhóm máy" người
// dùng tự gõ (vd "CNC", "Laser"...).
export const OPERATION_MANUAL_CATEGORY = "__operation_manual__";

// ✅ Ghi tường minh cho "video chạy máy" thay vì gửi chuỗi rỗng ("") — vì
// updateMachineVideoMeta phía dưới coi chuỗi rỗng là "xoá field" và tự động
// lưu thành NULL, gây khó hiểu khi xem trực tiếp trong DB (NULL trông như
// "chưa phân loại" dù thực chất là "video chạy máy"). Video cũ có category
// NULL hoặc bất kỳ giá trị tự do nào khác (từ trước khi có tính năng này)
// vẫn được tính là "video chạy máy" như bình thường — xem listMachineVideos.
export const OPERATION_DEMO_CATEGORY = "__operation_demo__";

function httpError(status: number, message: string, data?: unknown) {
  const err: any = new Error(message);
  err.statusCode = status;
  if (data !== undefined) err.data = data;
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
  const { q } = params;
  const page = Math.max(1, params.page ?? 1);
  const pageSize = Math.min(200, Math.max(1, params.pageSize ?? 20));

  // ✅ Nếu FE truyền category cụ thể (vd tab "Video hướng dẫn vận hành" luôn
  // truyền category = OPERATION_MANUAL_CATEGORY) -> lọc đúng category đó.
  // Nếu không truyền gì (tab "Kho video vận hành máy" mặc định) -> loại video
  // hướng dẫn ra, để 2 tab không lẫn vào nhau.
  //
  // ⚠️ CHÚ Ý: KHÔNG dùng { category: { not: OPERATION_MANUAL_CATEGORY } } ở
  // đây — về mặt SQL, so sánh NULL != 'x' trả về NULL (không phải true), nên
  // các video cũ có category = NULL (chưa từng điền "Nhóm máy") sẽ bị loại
  // nhầm khỏi kết quả. Phải liệt kê rõ "khác giá trị này HOẶC null" để chắc
  // chắn không ẩn nhầm video cũ.
  const categoryFilter: Prisma.MachineVideoWhereInput = params.category
    ? { category: params.category }
    : {
        OR: [
          { category: { not: OPERATION_MANUAL_CATEGORY } },
          { category: null },
        ],
      };

  const where: Prisma.MachineVideoWhereInput = {
    status: "READY", // ✅ chưa upload xong thì không hiện cho người khác thấy
    ...categoryFilter,
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

/**
 * ✅ Tra "tên máy người dùng gõ" -> máy chuẩn trong danh mục.
 * GET /api/machine-videos/resolve-machine?q=... (FE dùng để gợi ý khi đang gõ)
 */
export async function resolveMachineForUpload(q: string) {
  return resolveMachine(q);
}

// ============================================================
// ✅ TÁCH BẠCH 2 LOẠI VIDEO — KHÔNG ĐƯỢC LẪN
//   - Video chạy máy   = video THÔ  -> videos/raw/...       (AI được phép đọc để dựng nội dung)
//   - Video hướng dẫn  = NGƯỜI DỰNG -> videos/huong-dan/... (AI KHÔNG BAO GIỜ đọc / sửa / dùng)
// Tách ở 2 tầng: thư mục R2 riêng + cột category trong DB.
// Mọi công cụ AI chỉ được lấy video qua AI_ELIGIBLE_WHERE / isAiEligible bên dưới,
// và chỉ được quét prefix VIDEO_PREFIX_RAW.
// ============================================================
export const VIDEO_PREFIX_RAW = "videos/raw/";
export const VIDEO_PREFIX_MANUAL = "videos/huong-dan/";

/** Khu chứa video của máy mới chưa có trong danh mục Item */
export const MACHINE_NEW_FOLDER = "_may-moi";

export function isManual(category?: string | null) {
  return category === OPERATION_MANUAL_CATEGORY;
}

/** Điều kiện Prisma: chỉ video thô, đã upload xong — dùng cho MỌI chỗ AI lấy video */
export const AI_ELIGIBLE_WHERE: Prisma.MachineVideoWhereInput = {
  status: "READY",
  OR: [{ category: { not: OPERATION_MANUAL_CATEGORY } }, { category: null }],
  r2Key: { startsWith: VIDEO_PREFIX_RAW },
};

/** Kiểm tra 2 lớp: đúng loại + đúng thư mục. Thiếu 1 trong 2 là AI không được đụng. */
export function isAiEligible(v: { category?: string | null; r2Key: string; status?: string }) {
  return !isManual(v.category) && v.r2Key.startsWith(VIDEO_PREFIX_RAW) && (v.status ?? "READY") === "READY";
}

/**
 * Đặt đường dẫn R2 theo mã máy chuẩn:
 *   video thô:      videos/raw/JL-1000/JL-1000_20261013_095012_a1b2.mp4
 *   video hướng dẫn: videos/huong-dan/JL-1000/JL-1000_huong-dan_20261013_095012_a1b2.mp4
 * - có giờ + 4 ký tự ngẫu nhiên -> nhiều video cùng máy không bao giờ trùng tên
 * - không dấu, không khoảng trắng -> link không lỗi ở bất kỳ công cụ nào
 */
export function buildVideoKey(
  sku: string | null,
  category: string | null | undefined,
  fileName: string,
  mimeType: string,
  at: Date = new Date(),
  /** máy mới chưa có trong danh mục -> để trong khu riêng _may-moi/ cho dễ gắn mã sau */
  newMachine = false
) {
  const { stamp } = vnStamp(at);
  const rand = randomBytes(2).toString("hex");
  const ext = fileExt(fileName, mimeType);
  const manual = isManual(category);
  const prefix = manual ? VIDEO_PREFIX_MANUAL : VIDEO_PREFIX_RAW;
  const tag = manual ? "_huong-dan" : "";
  const code = sku ? keySafe(sku) : "_chua-ro"; // chưa xác định máy -> gom 1 chỗ, vẫn đúng loại
  const nice = sku ? code : "chua-ro";
  const folder = sku && newMachine ? `${MACHINE_NEW_FOLDER}/${code}` : code;
  return {
    r2Key: `${prefix}${folder}/${nice}${tag}_${stamp}_${rand}.${ext}`,
    niceName: `${nice}${tag}_${stamp}.${ext}`,
  };
}

/** BƯỚC 1 của upload: tạo record + trả URL để FE PUT thẳng file lên R2 */
export async function initMachineVideoUpload(
  input: {
    title?: string;
    machineCode?: string | null; // người dùng gõ tự do — server sẽ dò ra mã chuẩn
    category?: string | null;
    note?: string | null;
    fileName: string;
    mimeType: string;
    fileSize: number;
    /** true = bắt buộc khớp máy trong danh mục, không khớp thì trả 422 + gợi ý (dùng cho trang upload điện thoại) */
    strictMachine?: boolean;
    /** true = người dùng xác nhận đây là MÁY MỚI chưa có trong danh mục -> vẫn cho upload */
    newMachine?: boolean;
  },
  ctx: AuditCtx
) {
  if (!input.fileName) throw httpError(400, "Thiếu tên file");

  // Dò máy: ưu tiên ô mã máy, không có thì thử từ tên video
  let { match, suggestions } = await resolveMachine(input.machineCode);
  if (!match && input.title) {
    const byTitle = await resolveMachine(input.title);
    match = byTitle.match;
    if (!suggestions.length) suggestions = byTitle.suggestions;
  }

  // Máy mới chưa có trong danh mục: lấy mã model trong tên nếu có, không thì dùng chính tên gõ
  const typed = input.machineCode?.trim() || "";
  const isNewMachine = !match && !!input.newMachine && typed.length >= 2;
  const newCode = isNewMachine ? modelFromName(typed) || typed : null;

  if (!match && !isNewMachine && input.strictMachine) {
    throw httpError(
      422,
      "Không tìm thấy máy khớp với tên đã nhập. Chọn 1 máy trong gợi ý, hoặc bấm \"Dùng làm máy mới\".",
      { suggestions }
    );
  }
  if (!input.title?.trim() && !match && !isNewMachine) throw httpError(400, "Thiếu tên video");

  const mimeType = input.mimeType || "application/octet-stream";
  const { r2Key, niceName } = buildVideoKey(
    match?.code ?? newCode,
    input.category,
    input.fileName,
    mimeType,
    new Date(),
    isNewMachine
  );

  const created = await prisma.machineVideo.create({
    data: {
      title: input.title?.trim() || match?.name || typed,
      // Khớp được thì lưu MÃ CHUẨN, không lưu nguyên chữ người gõ
      machineCode: match?.code ?? (typed || null), // máy mới: lưu đúng tên/mã đã gõ để sau gắn mã chuẩn
      category: input.category?.trim() || null,
      note: input.note?.trim() || null,
      fileName: niceName, // tên khi tải về cũng theo mã máy
      mimeType,
      fileSize: input.fileSize || 0,
      r2Key,
      status: MachineVideoStatus.PENDING,
      uploadedById: ctx?.userId ?? null,
    },
    select: LIST_SELECT,
  });

  const uploadUrl = await getUploadUrl(r2Key, mimeType);

  return {
    id: created.id,
    uploadUrl,
    r2Key,
    machine: match, // null nếu chưa xác định -> FE có thể hiện "chưa rõ máy" + suggestions
    suggestions: match ? [] : suggestions,
  };
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

  const nextCategory = input.category !== undefined ? input.category?.trim() || null : existing.category;
  let nextMachineCode =
    input.machineCode !== undefined ? input.machineCode?.trim() || null : existing.machineCode;

  // Mã máy gõ tay -> đổi về mã chuẩn nếu khớp danh mục (chỉ khi người dùng có sửa ô mã máy)
  const { match } = input.machineCode !== undefined ? await resolveMachine(nextMachineCode) : { match: null };
  if (match) nextMachineCode = match.code;

  // ✅ Chỉ áp dụng cho video MỚI (đã nằm trong videos/raw/ hoặc videos/huong-dan/):
  // đổi loại video (thô <-> hướng dẫn) hoặc đổi máy -> chuyển file sang ĐÚNG thư mục,
  // để video hướng dẫn không bao giờ nằm lẫn trong khu video thô mà AI được đọc.
  // ⛔ Video CŨ (thư mục UUID, file có tên sẵn...) KHÔNG BAO GIỜ bị di chuyển.
  const isNewLayout =
    existing.r2Key.startsWith(VIDEO_PREFIX_RAW) || existing.r2Key.startsWith(VIDEO_PREFIX_MANUAL);
  let moved: { r2Key: string; fileName: string } | null = null;
  if (existing.status === "READY" && isNewLayout) {
    const prefix = isManual(nextCategory) ? VIDEO_PREFIX_MANUAL : VIDEO_PREFIX_RAW;
    const curPrefix = existing.r2Key.startsWith(VIDEO_PREFIX_RAW) ? VIDEO_PREFIX_RAW : VIDEO_PREFIX_MANUAL;
    // Thư mục hiện tại: "JL-1000"  |  "_chua-ro"  |  "_may-moi/May-dong-goi-tu-dong"
    const parts = existing.r2Key.slice(curPrefix.length).split("/");
    const curIsNew = parts[0] === MACHINE_NEW_FOLDER;
    const currentFolder = curIsNew ? `${parts[0]}/${parts[1]}` : parts[0];
    // Có khớp máy trong danh mục -> về thư mục máy chuẩn (máy mới đã được thêm vào danh mục cũng đi đường này).
    // Không đổi máy -> giữ nguyên thư mục máy hiện tại, chỉ đổi khu raw/huong-dan nếu đổi loại.
    const folder = `${prefix}${match ? keySafe(match.code) : currentFolder}/`;
    if (!existing.r2Key.startsWith(folder)) {
      const keepCode = curIsNew ? parts[1] : currentFolder !== "_chua-ro" ? currentFolder : null;
      const { r2Key, niceName } = buildVideoKey(
        match?.code ?? keepCode,
        nextCategory,
        existing.fileName,
        existing.mimeType,
        existing.createdAt,
        !match && curIsNew
      );
      await moveObject(existing.r2Key, r2Key, existing.mimeType);
      moved = { r2Key, fileName: niceName };
    }
  }

  const updated = await prisma.machineVideo.update({
    where: { id },
    data: {
      ...(input.title !== undefined ? { title: input.title.trim() } : {}),
      ...(input.machineCode !== undefined || match ? { machineCode: nextMachineCode } : {}),
      ...(input.category !== undefined ? { category: nextCategory } : {}),
      ...(input.note !== undefined ? { note: input.note?.trim() || null } : {}),
      ...(moved ?? {}),
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

// ============================================================
// ✅ SHARE LINK — chia sẻ video cho khách xem, không cần đăng nhập,
// không cho tải xuống (chặn ở mức UI/HTTP, không phải DRM tuyệt đối).
// ============================================================

const SHARE_SELECT = {
  id: true,
  token: true,
  expiresAt: true,
  revokedAt: true,
  createdAt: true,
  createdBy: { select: { id: true, username: true } },
} as const;

/** Tạo link chia sẻ mới cho 1 video. Ai đăng nhập cũng tạo được (mọi role). */
export async function createMachineVideoShare(
  videoId: string,
  input: { expiresInDays?: number | null },
  ctx: AuditCtx
) {
  const video = await prisma.machineVideo.findUnique({ where: { id: videoId } });
  if (!video) throw httpError(404, "Không tìm thấy video");
  if (video.status !== "READY") throw httpError(400, "Video chưa upload xong, chưa chia sẻ được.");

  // token ngẫu nhiên, an toàn cho URL (base64url), không đoán được
  const token = randomBytes(16).toString("base64url");

  const days = input.expiresInDays;
  const expiresAt =
    typeof days === "number" && days > 0
      ? new Date(Date.now() + days * 24 * 60 * 60 * 1000)
      : null;

  const share = await prisma.machineVideoShare.create({
    data: {
      videoId,
      token,
      expiresAt,
      createdById: ctx?.userId ?? null,
    },
    select: SHARE_SELECT,
  });

  await auditLog(prisma, {
    userId: ctx?.userId,
    userRole: ctx?.userRole,
    action: "CREATE_SHARE",
    entity: "MachineVideoShare",
    entityId: share.id,
    after: { videoId, expiresAt },
    meta: ctx?.meta,
  });

  return share;
}

/** Liệt kê các link chia sẻ (còn hạn/hết hạn/đã thu hồi) của 1 video, mới nhất trước. */
export async function listMachineVideoShares(videoId: string) {
  const items = await prisma.machineVideoShare.findMany({
    where: { videoId },
    select: SHARE_SELECT,
    orderBy: [{ createdAt: "desc" }],
  });
  return { items };
}

/** Thu hồi 1 link chia sẻ — sau đó khách bấm vào link sẽ báo lỗi ngay. */
export async function revokeMachineVideoShare(shareId: string, ctx: AuditCtx) {
  const existing = await prisma.machineVideoShare.findUnique({ where: { id: shareId } });
  if (!existing) throw httpError(404, "Không tìm thấy link chia sẻ");

  if (existing.revokedAt) {
    return { id: existing.id, revokedAt: existing.revokedAt };
  }

  const updated = await prisma.machineVideoShare.update({
    where: { id: shareId },
    data: { revokedAt: new Date() },
    select: { id: true, revokedAt: true },
  });

  await auditLog(prisma, {
    userId: ctx?.userId,
    userRole: ctx?.userRole,
    action: "REVOKE_SHARE",
    entity: "MachineVideoShare",
    entityId: shareId,
    meta: ctx?.meta,
  });

  return updated;
}

/**
 * ✅ Dùng cho route CÔNG KHAI (không đăng nhập).
 * Kiểm tra token còn hợp lệ không, trả về thông tin video + URL xem trực tiếp
 * (tạo mới mỗi lần gọi, không lưu URL cố định — URL R2 có hạn dùng ngắn).
 */
export async function getPublicMachineVideoShare(token: string) {
  const share = await prisma.machineVideoShare.findUnique({
    where: { token },
    include: { video: true },
  });

  if (!share) throw httpError(404, "Link chia sẻ không tồn tại.");
  if (share.revokedAt) throw httpError(410, "Link chia sẻ này đã bị thu hồi.");
  if (share.expiresAt && share.expiresAt.getTime() < Date.now()) {
    throw httpError(410, "Link chia sẻ này đã hết hạn.");
  }
  if (share.video.status !== "READY") throw httpError(400, "Video chưa sẵn sàng để xem.");

  const url = await getPreviewUrl(share.video.r2Key);

  return {
    title: share.video.title,
    machineCode: share.video.machineCode,
    note: share.video.note,
    mimeType: share.video.mimeType,
    url,
  };
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