// src/services/mkt.service.ts
// ✅ MKT TỰ ĐỘNG — phần chạy trên server (Render)
//
//  Máy công ty (mcbrother-mkt-worker)                Server (file này)
//  ─────────────────────────────────                 ─────────────────
//  1. claimSource ───────────────────────────────▶   chọn 1 video thô chưa xử lý (CHỈ videos/raw/, AI_ELIGIBLE_WHERE)
//  2. tải video, AI xem + cắt clip + viết bài
//  3. uploadUrls ────────────────────────────────▶   link PUT lên R2 (mkt/clips/...)
//  4. finishSource ──────────────────────────────▶   tạo MktClip "chờ duyệt" + gửi Telegram có nút ✅ / ❌
//                                                    người duyệt: Telegram hoặc trang "Hàng chờ MKT" trên CRM
//                                                    duyệt -> xếp lịch 1 clip/ngày, 19h–21h giờ VN
//  5. claimDue (mỗi vài phút) ───────────────────▶   clip tới giờ đăng
//  6. đăng Facebook Reels, báo postResult ───────▶   lưu kết quả; TikTok / YouTube: gửi sẵn clip + caption qua Telegram để đăng tay
//
// KHÔNG bao giờ đụng video hướng dẫn (videos/huong-dan/) — mọi chỗ lấy video đều qua AI_ELIGIBLE_WHERE + isAiEligible.
import { PrismaClient, Prisma } from "@prisma/client";
import { getUploadUrl, getDownloadUrl, getPreviewUrl, objectExists, deleteObject } from "../lib/r2Client";
import { AI_ELIGIBLE_WHERE, isAiEligible } from "./machineVideos.service";
import { resolveMachine, vnStamp, keySafe } from "../utils/machineMatch";
import { tg, esc, cut, keyboard, telegramReady, MKT_CHAT_ID } from "../lib/telegram";

const prisma = new PrismaClient();

export const CHANNELS = ["facebook", "tiktok", "youtube"] as const;
export type Channel = (typeof CHANNELS)[number];
/** Kênh máy công ty tự đăng được (còn lại: AI chuẩn bị sẵn, người đăng tay) */
export const AUTO_CHANNELS: Channel[] = ["facebook"];
export const CHANNEL_LABEL: Record<Channel, string> = { facebook: "Facebook Reels", tiktok: "TikTok", youtube: "YouTube Shorts" };

const CRM_URL = () => (process.env.MKT_CRM_URL || "https://apibrother.id.vn").replace(/\/+$/, "");
/** Nút mở CRM trên Telegram — Telegram KHÔNG nhận link localhost / http thường, khi đó bỏ nút */
const crmButton = (id: string, text: string) =>
  /^https:\/\/(?!localhost|127\.|192\.168\.|10\.)/i.test(CRM_URL()) ? [{ text, url: `${CRM_URL()}/mkt/${id}` }] : null;
const SLOT_START_MIN = 19 * 60; // 19:00
const SLOT_END_MIN = 21 * 60; // 21:00
const VN_OFFSET = 7 * 3600 * 1000;
const MAX_ATTEMPTS = 3;
/**
 * CHẾ ĐỘ: tự động (video thô mới tự làm) | bán tự động (chỉ làm video được bấm "Cho AI làm").
 * Cả 2 chế độ clip vẫn chờ duyệt rồi mới đăng. Admin gạt trên trang Hàng chờ MKT (lưu bảng MktSetting);
 * chưa gạt lần nào thì theo biến MKT_AUTO (mặc định: tự động).
 */
let autoMode: boolean | null = null;
const AUTO = () => (autoMode ?? process.env.MKT_AUTO !== "0");
async function loadSettings() {
  if (autoMode !== null) return;
  const row = await prisma.mktSetting.findUnique({ where: { key: "auto" } }).catch(() => null);
  autoMode = row ? row.value === "1" : process.env.MKT_AUTO !== "0";
}
export async function setAutoMode(on: boolean, by: string) {
  await prisma.mktSetting.upsert({
    where: { key: "auto" },
    create: { key: "auto", value: on ? "1" : "0", updatedBy: by },
    update: { value: on ? "1" : "0", updatedBy: by },
  });
  autoMode = on;
  if (on) mktPoke(); // vừa bật tự động -> máy công ty kiểm tra video thô đang chờ
  else hint.checkedAt = 0; // tắt -> lần hỏi tới tính lại có việc hay không
  return { auto: on };
}
const STALE_CLAIM_MS = 3 * 3600 * 1000; // máy công ty nhận việc mà 3 tiếng không báo -> cho nhận lại
const STALE_POST_MS = 2 * 3600 * 1000;

function httpError(status: number, message: string) {
  const err: any = new Error(message);
  err.statusCode = status;
  return err;
}

// ------------------------------------------------------------ TIẾT KIỆM NEON
// Máy công ty hỏi /worker/ping vài phút 1 lần — trả lời từ BỘ NHỚ, KHÔNG đụng database, để Neon được ngủ
// (Neon tính tiền theo giờ database thức; cứ < 5 phút hỏi DB 1 lần thì Neon thức 24/7).
// Chỉ hỏi DB khi: có việc thật (video mới / bấm Cho AI làm / tới giờ đăng) hoặc 3 tiếng kiểm tra lại 1 lần.
const HINT_REFRESH_MS = 3 * 3600 * 1000;
const hint = { checkedAt: 0, pending: true, nextDueAt: null as Date | null };
/** Báo có việc mới cho máy công ty (video thô mới upload, Cho AI làm, Làm lại từ đầu) */
export const mktPoke = () => {
  hint.pending = true;
};
const noteDue = (d: Date | null | undefined) => {
  if (d && (!hint.nextDueAt || d < hint.nextDueAt)) hint.nextDueAt = d;
};
async function refreshHint() {
  await loadSettings();
  const vids = await prisma.machineVideo.findMany({ where: AI_ELIGIBLE_WHERE, select: { id: true } });
  const jobs = await prisma.mktSourceJob.findMany({ where: { videoId: { in: vids.map((v) => v.id) } } });
  const byVideo = new Map(jobs.map((j) => [j.videoId, j]));
  hint.pending = vids.some((v) => isPending(byVideo.get(v.id)));
  const next = await prisma.mktClip.findFirst({ where: { status: "da_len_lich" }, orderBy: { scheduledAt: "asc" }, select: { scheduledAt: true } });
  hint.nextDueAt = next?.scheduledAt ?? null;
  hint.checkedAt = Date.now();
}
/** Máy công ty hỏi "có việc không?" — thường KHÔNG chạm database */
export async function ping() {
  touchWorker();
  if (Date.now() - hint.checkedAt > HINT_REFRESH_MS) await refreshHint();
  const now = new Date();
  return { pending: hint.pending, due: !!hint.nextDueAt && hint.nextDueAt <= now, nextDueAt: hint.nextDueAt };
}

// Lần cuối máy công ty gọi lên (để CRM hiện "máy công ty đang chạy / đã tắt")
let workerSeenAt: Date | null = null;
export const touchWorker = () => {
  workerSeenAt = new Date();
};

// ------------------------------------------------------------ giờ Việt Nam
const vnParts = (d: Date) => {
  const v = new Date(d.getTime() + VN_OFFSET);
  return { y: v.getUTCFullYear(), m: v.getUTCMonth(), d: v.getUTCDate(), min: v.getUTCHours() * 60 + v.getUTCMinutes() };
};
const vnDate = (y: number, m: number, d: number, min: number) => new Date(Date.UTC(y, m, d, 0, min) - VN_OFFSET);
const vnDayKey = (d: Date) => {
  const p = vnParts(d);
  return `${p.y}-${p.m}-${p.d}`;
};
const pad = (n: number) => String(n).padStart(2, "0");
/** "19:45 13/10" (giờ VN) */
export const fmtVN = (d: Date) => {
  const v = new Date(d.getTime() + VN_OFFSET);
  return `${pad(v.getUTCHours())}:${pad(v.getUTCMinutes())} ${pad(v.getUTCDate())}/${pad(v.getUTCMonth() + 1)}`;
};

/** Khung đăng kế tiếp còn trống: 1 clip / ngày, 19h–21h giờ VN */
export async function nextSlot(excludeId?: string): Promise<Date> {
  const now = new Date();
  const taken = await prisma.mktClip.findMany({
    where: {
      status: { in: ["da_len_lich", "dang_dang", "da_dang"] },
      scheduledAt: { gte: new Date(now.getTime() - 24 * 3600 * 1000) },
      ...(excludeId ? { id: { not: excludeId } } : {}),
    },
    select: { scheduledAt: true },
  });
  const used = new Set(taken.map((t) => vnDayKey(t.scheduledAt!)));
  const p = vnParts(now);
  for (let i = 0; i < 365; i++) {
    const day = vnDate(p.y, p.m, p.d + i, 0);
    if (used.has(vnDayKey(day))) continue;
    const dp = vnParts(day);
    let min = SLOT_START_MIN + Math.floor(Math.random() * 90); // 19:00–20:30, lệch ngẫu nhiên cho tự nhiên
    if (i === 0) {
      if (p.min > SLOT_END_MIN - 15) continue; // hôm nay đã quá khung giờ
      min = Math.max(min, p.min + 10);
    }
    return vnDate(dp.y, dp.m, dp.d, min);
  }
  throw httpError(500, "Không tìm được khung giờ trống");
}

// ------------------------------------------------------------ thông tin máy cho AI viết bài
async function machineInfo(code: string | null, title: string) {
  const m = await resolveMachine(code || title).catch(() => ({ match: null }));
  const match = (m as any).match as { id: string; sku: string; name: string; code: string } | null;
  const finalCode = match?.code || code || null;
  const item = match ? await prisma.item.findUnique({ where: { id: match.id }, select: { note: true } }) : null;
  // Thông số / đặc điểm đã có trong nháp báo giá của tài liệu dịch (nếu có) -> AI viết đúng số liệu, không bịa
  let quote: any = null;
  if (finalCode) {
    const doc = await prisma.translatedDoc.findFirst({
      where: {
        quoteDraft: { not: Prisma.DbNull },
        OR: [{ machineCode: { equals: finalCode, mode: "insensitive" } }, { context: { contains: finalCode, mode: "insensitive" } }],
      },
      orderBy: { updatedAt: "desc" },
      select: { quoteDraft: true },
    });
    quote = (doc?.quoteDraft as any)?.q ?? null;
  }
  return {
    code: finalCode,
    name: match?.name || title || null,
    note: item?.note || null,
    process: quote?.quyTrinh || null,
    features: Array.isArray(quote?.dacDiem) ? quote.dacDiem.slice(0, 10) : [],
    specs: Array.isArray(quote?.thongSo) ? quote.thongSo.slice(0, 15) : [],
  };
}

/** Video thô này có đang chờ máy công ty xử lý không */
function isPending(j?: { status: string; attempts: number; claimedAt: Date } | null) {
  if (!j) return AUTO();
  if (j.status === "uu_tien") return true;
  if (j.status === "loi") return j.attempts < MAX_ATTEMPTS;
  if (j.status === "dang_xu_ly") return Date.now() - j.claimedAt.getTime() > STALE_CLAIM_MS;
  return false;
}

// ================================================================ API cho máy công ty
/** Nhận 1 video thô chưa xử lý (cũ nhất trước). Không còn -> { job: null } */
export async function claimSource() {
  touchWorker();
  await loadSettings();
  const SELECT = { id: true, title: true, note: true, machineCode: true, category: true, fileName: true, fileSize: true, r2Key: true, status: true, createdAt: true } as const;
  // 1) Video được yêu cầu làm (ưu tiên trước), cũ nhất trước
  const queued = await prisma.mktSourceJob.findMany({ where: { status: "uu_tien" }, orderBy: { updatedAt: "asc" }, take: 20 });
  let video: any = null;
  const byVideo = new Map<string, any>();
  if (queued.length) {
    const vids = await prisma.machineVideo.findMany({ where: { AND: [AI_ELIGIBLE_WHERE, { id: { in: queued.map((q) => q.videoId) } }] }, select: SELECT });
    for (const q of queued) byVideo.set(q.videoId, q);
    video = queued.map((q) => vids.find((v) => v.id === q.videoId)).find((v) => v && isAiEligible(v)) || null;
  }
  // 2) Tự động: video thô chưa xử lý (nếu bật MKT_AUTO), video lỗi còn lượt thử, việc bị bỏ dở quá lâu
  if (!video) {
    const candidates = await prisma.machineVideo.findMany({ where: AI_ELIGIBLE_WHERE, orderBy: { createdAt: "asc" }, take: 300, select: SELECT });
    if (!candidates.length) {
      hint.pending = false;
      return { job: null };
    }
    const jobs = await prisma.mktSourceJob.findMany({ where: { videoId: { in: candidates.map((c) => c.id) } } });
    for (const j of jobs) byVideo.set(j.videoId, j);
    video = candidates.find((v) => isAiEligible(v) && isPending(byVideo.get(v.id))) || null;
  }
  if (!video) {
    hint.pending = false;
    return { job: null };
  }

  const prev = byVideo.get(video.id);
  const job = prev
    ? await prisma.mktSourceJob.update({
        where: { id: prev.id },
        data: { status: "dang_xu_ly", attempts: { increment: 1 }, claimedAt: new Date(), note: null },
      })
    : await prisma.mktSourceJob.create({ data: { videoId: video.id, r2Key: video.r2Key, machineCode: video.machineCode } });

  return {
    job: { id: job.id, attempt: job.attempts },
    video: {
      id: video.id,
      title: video.title,
      note: video.note,
      fileName: video.fileName,
      fileSize: Number(video.fileSize),
      createdAt: video.createdAt,
    },
    downloadUrl: await getDownloadUrl(video.r2Key, video.fileName, 6 * 3600),
    machine: await machineInfo(video.machineCode, video.title),
  };
}

/** Link PUT lên R2 cho clip / bản xem trước / ảnh bìa */
export async function uploadUrls(jobId: string, files: { name: string; type: string }[]) {
  touchWorker();
  const job = await prisma.mktSourceJob.findUnique({ where: { id: jobId } });
  if (!job) throw httpError(404, "Không có job này");
  const { ym } = vnStamp();
  return {
    files: await Promise.all(
      (files || []).slice(0, 20).map(async (f) => {
        const name = keySafe(String(f.name || "file").replace(/\.[^.]+$/, "")) + (/\.([a-z0-9]{2,4})$/i.exec(f.name)?.[0] || "");
        const key = `mkt/clips/${ym}/${job.id}/${name}`;
        return { name: f.name, key, url: await getUploadUrl(key, f.type || "application/octet-stream", 6 * 3600) };
      })
    ),
  };
}

type ClipIn = {
  clipKey: string;
  previewKey?: string;
  thumbKey?: string;
  durationSec: number;
  startSec: number;
  endSec: number;
  hook: string;
  captions: any;
  aiNote?: string;
  aiCost?: number;
};

const str = (v: unknown, n: number) => (typeof v === "string" ? v.trim().slice(0, n) : "");
function cleanCaptions(c: any) {
  return {
    facebook: { text: str(c?.facebook?.text, 5000) },
    tiktok: { text: str(c?.tiktok?.text, 2200) },
    youtube: { title: str(c?.youtube?.title, 100), description: str(c?.youtube?.description, 5000) },
  };
}

/** Máy công ty xử lý xong 1 video thô */
export async function finishSource(jobId: string, body: { clips?: ClipIn[]; aiCost?: number; skipped?: boolean; note?: string; machine?: { code?: string; name?: string } }) {
  touchWorker();
  const job = await prisma.mktSourceJob.findUnique({ where: { id: jobId } });
  if (!job) throw httpError(404, "Không có job này");
  const clips = Array.isArray(body.clips) ? body.clips.slice(0, 5) : [];

  if (body.skipped || !clips.length) {
    await prisma.mktSourceJob.update({
      where: { id: jobId },
      data: { status: "bo_qua", note: str(body.note, 500) || "AI thấy video không dùng được", aiCost: Number(body.aiCost) || 0 },
    });
    return { clips: [] };
  }
  for (const c of clips) {
    if (!(await objectExists(c.clipKey))) throw httpError(400, `Chưa thấy file clip trên R2: ${c.clipKey}`);
  }
  const created = [];
  for (const c of clips) {
    created.push(
      await prisma.mktClip.create({
        data: {
          jobId,
          videoId: job.videoId,
          machineCode: str(body.machine?.code, 60) || job.machineCode,
          machineName: str(body.machine?.name, 200) || null,
          clipKey: c.clipKey,
          previewKey: c.previewKey || null,
          thumbKey: c.thumbKey || null,
          durationSec: Number(c.durationSec) || 0,
          startSec: Number(c.startSec) || 0,
          endSec: Number(c.endSec) || 0,
          hook: str(c.hook, 120),
          captions: cleanCaptions(c.captions),
          aiNote: str(c.aiNote, 1000) || null,
          aiCost: Number(c.aiCost) || 0,
        },
      })
    );
  }
  await prisma.mktSourceJob.update({
    where: { id: jobId },
    data: { status: "xong", clipCount: created.length, aiCost: Number(body.aiCost) || 0, note: str(body.note, 500) || null },
  });
  for (const c of created) await sendReview(c.id).catch((e) => console.warn("[mkt] gửi Telegram lỗi:", e?.message));
  return { clips: created.map((c) => c.id) };
}

export async function failSource(jobId: string, error: string) {
  touchWorker();
  await prisma.mktSourceJob.update({ where: { id: jobId }, data: { status: "loi", note: str(error, 1000) } });
  return { ok: true };
}

/** Clip đã tới giờ đăng -> giao cho máy công ty (đánh dấu "đang đăng" để không đăng 2 lần) */
export async function claimDue() {
  touchWorker();
  const now = new Date();
  // Máy công ty nhận đăng mà 2 tiếng không báo kết quả -> báo lỗi để người kiểm tra (KHÔNG tự đăng lại, tránh đăng trùng)
  const stale = await prisma.mktClip.findMany({ where: { status: "dang_dang", updatedAt: { lt: new Date(now.getTime() - STALE_POST_MS) } } });
  for (const s of stale) {
    await prisma.mktClip.update({ where: { id: s.id }, data: { status: "loi", aiNote: `${s.aiNote || ""}\n[Máy công ty không báo kết quả đăng — kiểm tra trên Page trước khi đăng lại]`.trim() } });
  }
  const due = await prisma.mktClip.findMany({ where: { status: "da_len_lich", scheduledAt: { lte: now } }, orderBy: { scheduledAt: "asc" }, take: 5 });
  const out = [];
  for (const c of due) {
    const r = await prisma.mktClip.updateMany({ where: { id: c.id, status: "da_len_lich" }, data: { status: "dang_dang" } });
    if (!r.count) continue;
    out.push({
      id: c.id,
      channels: c.channels,
      autoChannels: c.channels.filter((ch) => (AUTO_CHANNELS as string[]).includes(ch)),
      captions: c.captions,
      machineCode: c.machineCode,
      clipUrl: await getDownloadUrl(c.clipKey, `${keySafe(c.machineCode || "clip")}_${c.id}.mp4`, 6 * 3600),
    });
  }
  const next = await prisma.mktClip.findFirst({ where: { status: "da_len_lich" }, orderBy: { scheduledAt: "asc" }, select: { scheduledAt: true } });
  hint.nextDueAt = next?.scheduledAt ?? null;
  return { posts: out };
}

/** Máy công ty báo kết quả đăng */
export async function postResult(id: string, results: Record<string, { ok: boolean; id?: string; url?: string; error?: string }>) {
  touchWorker();
  const c = await prisma.mktClip.findUnique({ where: { id } });
  if (!c) throw httpError(404, "Không có clip này");
  const merged: Record<string, any> = { ...((c.results as any) || {}) };
  for (const ch of CHANNELS) if (results?.[ch]) merged[ch] = { ...results[ch], at: new Date().toISOString() };
  const manual = c.channels.filter((ch) => !(AUTO_CHANNELS as string[]).includes(ch) || !results?.[ch]);
  for (const ch of manual) if (!merged[ch]) merged[ch] = { manual: true, at: new Date().toISOString() };
  const failed = Object.entries(merged).filter(([, v]) => v && v.ok === false);
  const updated = await prisma.mktClip.update({
    where: { id },
    data: { results: merged, status: failed.length ? "loi" : "da_dang", postedAt: new Date() },
  });
  await notifyPosted(updated.id, manual as Channel[]).catch((e) => console.warn("[mkt] Telegram:", e?.message));
  return { status: updated.status };
}

// ================================================================ API cho CRM
const LIST_SELECT = {
  id: true,
  status: true,
  machineCode: true,
  machineName: true,
  hook: true,
  durationSec: true,
  thumbKey: true,
  channels: true,
  scheduledAt: true,
  postedAt: true,
  results: true,
  reviewedBy: true,
  createdAt: true,
} as const;

export async function listClips(q: any) {
  const status = str(q?.status, 20);
  const where: Prisma.MktClipWhereInput = {
    ...(status ? { status: { in: status.split(",") } } : {}),
    ...(q?.q
      ? {
          OR: [
            { machineCode: { contains: String(q.q), mode: "insensitive" } },
            { machineName: { contains: String(q.q), mode: "insensitive" } },
            { hook: { contains: String(q.q), mode: "insensitive" } },
          ],
        }
      : {}),
  };
  const sortScheduled = status === "da_len_lich";
  const rows = await prisma.mktClip.findMany({
    where,
    orderBy: sortScheduled ? [{ scheduledAt: "asc" }] : [{ createdAt: "desc" }],
    take: Math.min(Number(q?.take) || 50, 200),
    select: LIST_SELECT,
  });
  return {
    items: await Promise.all(rows.map(async ({ thumbKey, ...r }) => ({ ...r, thumbUrl: thumbKey ? await getPreviewUrl(thumbKey, 3600) : null }))),
  };
}

export async function summary() {
  await loadSettings();
  const groups = await prisma.mktClip.groupBy({ by: ["status"], _count: { _all: true } });
  const counts = Object.fromEntries(groups.map((g) => [g.status, g._count._all]));
  const processing = await prisma.mktSourceJob.count({ where: { status: "dang_xu_ly" } });
  const vids = await prisma.machineVideo.findMany({ where: AI_ELIGIBLE_WHERE, select: { id: true } });
  const jobs = await prisma.mktSourceJob.findMany({ where: { videoId: { in: vids.map((v) => v.id) } } });
  const byVideo = new Map(jobs.map((j) => [j.videoId, j]));
  const waiting = vids.filter((v) => byVideo.get(v.id)?.status !== "dang_xu_ly" && isPending(byVideo.get(v.id))).length;
  return { counts, processing, waitingVideos: waiting, totalRaw: vids.length, auto: AUTO(), workerSeenAt, telegram: telegramReady() };
}

export async function getClip(id: string) {
  const c = await prisma.mktClip.findUnique({ where: { id } });
  if (!c) throw httpError(404, "Không tìm thấy clip");
  const video = await prisma.machineVideo.findUnique({ where: { id: c.videoId }, select: { title: true, fileName: true } });
  return {
    ...c,
    sourceTitle: video?.title ?? null,
    previewUrl: await getPreviewUrl(c.previewKey || c.clipKey, 3600),
    clipUrl: await getDownloadUrl(c.clipKey, `${keySafe(c.machineCode || "clip")}_${c.id}.mp4`, 3600),
    thumbUrl: c.thumbKey ? await getPreviewUrl(c.thumbKey, 3600) : null,
  };
}

const EDITABLE = ["cho_duyet", "da_len_lich", "loi"];

export async function updateClip(id: string, body: any) {
  const c = await prisma.mktClip.findUnique({ where: { id } });
  if (!c) throw httpError(404, "Không tìm thấy clip");
  if (!EDITABLE.includes(c.status)) throw httpError(400, "Clip này không sửa được nữa (đã đăng / đang đăng / đã bỏ)");
  const data: Prisma.MktClipUpdateInput = {};
  if (body?.captions) data.captions = cleanCaptions({ ...(c.captions as any), ...body.captions });
  if (Array.isArray(body?.channels)) {
    const ch = body.channels.filter((x: string) => (CHANNELS as readonly string[]).includes(x));
    if (!ch.length) throw httpError(400, "Chọn ít nhất 1 kênh");
    data.channels = ch;
  }
  if (body?.scheduledAt !== undefined && c.status === "da_len_lich") {
    const d = new Date(body.scheduledAt);
    if (isNaN(d.getTime())) throw httpError(400, "Giờ đăng không hợp lệ");
    data.scheduledAt = d;
    noteDue(d);
  }
  return prisma.mktClip.update({ where: { id }, data });
}

/** Duyệt -> xếp lịch. at: giờ đăng tuỳ chọn (không có = khung trống kế tiếp 19h–21h) */
export async function approveClip(id: string, by: string, at?: string | Date | null) {
  const c = await prisma.mktClip.findUnique({ where: { id } });
  if (!c) throw httpError(404, "Không tìm thấy clip");
  if (!["cho_duyet", "loi", "da_len_lich"].includes(c.status)) throw httpError(400, "Clip này đã xử lý rồi");
  let when = at ? new Date(at) : await nextSlot(id);
  if (isNaN(when.getTime())) when = await nextSlot(id);
  const updated = await prisma.mktClip.update({
    where: { id },
    data: { status: "da_len_lich", scheduledAt: when, reviewedBy: by, reviewedAt: new Date() },
  });
  noteDue(when);
  await markReviewed(updated.id, `✅ ${esc(by)} đã duyệt · đăng lúc <b>${fmtVN(when)}</b>`).catch(() => undefined);
  return updated;
}

export async function rejectClip(id: string, by: string) {
  const c = await prisma.mktClip.findUnique({ where: { id } });
  if (!c) throw httpError(404, "Không tìm thấy clip");
  if (["dang_dang", "da_dang"].includes(c.status)) throw httpError(400, "Clip đã đăng / đang đăng, không bỏ được");
  const updated = await prisma.mktClip.update({ where: { id }, data: { status: "bo", reviewedBy: by, reviewedAt: new Date() } });
  await markReviewed(updated.id, `❌ ${esc(by)} đã bỏ clip này`).catch(() => undefined);
  return updated;
}

/** Huỷ lịch -> về chờ duyệt */
export async function unscheduleClip(id: string) {
  const c = await prisma.mktClip.findUnique({ where: { id } });
  if (!c || c.status !== "da_len_lich") throw httpError(400, "Clip không ở trạng thái đã lên lịch");
  return prisma.mktClip.update({ where: { id }, data: { status: "cho_duyet", scheduledAt: null } });
}

/** Xoá hẳn clip đã bỏ (xoá cả file trên R2) */
export async function deleteClip(id: string) {
  const c = await prisma.mktClip.findUnique({ where: { id } });
  if (!c) throw httpError(404, "Không tìm thấy clip");
  if (c.status !== "bo") throw httpError(400, "Chỉ xoá được clip đã bỏ");
  for (const k of [c.clipKey, c.previewKey, c.thumbKey]) if (k) await deleteObject(k).catch(() => undefined);
  await prisma.mktClip.delete({ where: { id } });
  return { ok: true };
}

/**
 * Danh sách TẤT CẢ video thô (videos/raw/) kèm trạng thái AI. q: tìm theo tên máy / mã máy / tên video.
 * trangThai: cho_xu_ly (tự động sẽ làm) · chua_xu_ly (tắt tự động, chưa ai yêu cầu) · uu_tien (đã bấm Cho AI làm)
 *            · dang_xu_ly · xong · bo_qua · loi
 */
export async function listSources(q?: string) {
  await loadSettings();
  const query = str(q, 80);
  const vids = await prisma.machineVideo.findMany({
    where: {
      AND: [
        AI_ELIGIBLE_WHERE,
        query
          ? {
              OR: [
                { title: { contains: query, mode: "insensitive" } },
                { machineCode: { contains: query, mode: "insensitive" } },
                { fileName: { contains: query, mode: "insensitive" } },
              ],
            }
          : {},
      ],
    },
    orderBy: { createdAt: "desc" },
    take: 100,
    select: { id: true, title: true, machineCode: true, fileName: true, fileSize: true, createdAt: true },
  });
  const jobs = await prisma.mktSourceJob.findMany({ where: { videoId: { in: vids.map((v) => v.id) } } });
  const byVideo = new Map(jobs.map((j) => [j.videoId, j]));
  return {
    auto: AUTO(),
    items: vids.map((v) => {
      const j = byVideo.get(v.id);
      const status = !j ? (AUTO() ? "cho_xu_ly" : "chua_xu_ly") : j.status === "loi" && isPending(j) ? "cho_xu_ly" : j.status;
      return {
        videoId: v.id,
        title: v.title || v.fileName,
        machineCode: v.machineCode,
        fileSize: Number(v.fileSize),
        createdAt: v.createdAt,
        status,
        clipCount: j?.clipCount ?? 0,
        note: j?.note ?? null,
        aiCost: j?.aiCost ?? 0,
        updatedAt: j?.updatedAt ?? v.createdAt,
      };
    }),
  };
}

/** "Cho AI làm": đưa video thô lên đầu hàng chờ (kể cả khi đã tắt tự động) */
export async function queueSources(videoIds: string[]) {
  const ids = (Array.isArray(videoIds) ? videoIds : []).map(String).slice(0, 50);
  let queued = 0;
  for (const id of ids) {
    const v = await prisma.machineVideo.findUnique({ where: { id } });
    if (!v || !isAiEligible(v)) continue;
    const j = await prisma.mktSourceJob.findUnique({ where: { videoId: id } });
    if (j?.status === "dang_xu_ly" || j?.status === "uu_tien") continue;
    if (j) await prisma.mktSourceJob.update({ where: { id: j.id }, data: { status: "uu_tien", attempts: 0, note: "Chờ AI làm theo yêu cầu" } });
    else await prisma.mktSourceJob.create({ data: { videoId: id, r2Key: v.r2Key, machineCode: v.machineCode, status: "uu_tien", attempts: 0, note: "Chờ AI làm theo yêu cầu" } });
    queued++;
  }
  if (queued) mktPoke();
  return { queued };
}

/** Cho AI xử lý lại 1 video thô (lần sau máy công ty sẽ nhận lại) */
export async function retrySource(videoId: string) {
  const v = await prisma.machineVideo.findUnique({ where: { id: videoId } });
  if (!v || !isAiEligible(v)) throw httpError(400, "Video này không phải video thô AI được dùng");
  await prisma.mktSourceJob.updateMany({ where: { videoId }, data: { status: "loi", attempts: 0, note: "Xử lý lại theo yêu cầu" } });
  mktPoke();
  return { ok: true };
}

/**
 * LÀM LẠI TỪ ĐẦU 1 video thô: xoá mọi clip CHƯA ĐĂNG của video đó (cả file trên R2),
 * rồi đưa video về hàng chờ để máy công ty dựng lại. Clip đã đăng / đang đăng giữ nguyên làm lịch sử.
 * Video thô gốc trên R2 KHÔNG bị đụng tới.
 */
export async function resetSource(videoId: string) {
  const v = await prisma.machineVideo.findUnique({ where: { id: videoId } });
  if (!v || !isAiEligible(v)) throw httpError(400, "Video này không phải video thô AI được dùng");
  const job = await prisma.mktSourceJob.findUnique({ where: { videoId } });
  if (job?.status === "dang_xu_ly") throw httpError(400, "Máy công ty đang xử lý video này, đợi xong rồi làm lại");
  const clips = await prisma.mktClip.findMany({ where: { videoId, status: { notIn: ["da_dang", "dang_dang"] } } });
  for (const c of clips) {
    for (const k of [c.clipKey, c.previewKey, c.thumbKey]) if (k) await deleteObject(k).catch(() => undefined);
    await prisma.mktClip.delete({ where: { id: c.id } });
  }
  const kept = await prisma.mktClip.count({ where: { videoId } });
  if (job) await prisma.mktSourceJob.update({ where: { id: job.id }, data: { status: "uu_tien", attempts: 0, clipCount: kept, note: "Làm lại từ đầu theo yêu cầu" } });
  mktPoke();
  return { deleted: clips.length, kept };
}

// ================================================================ Telegram
const STATUS_LABEL: Record<string, string> = {
  cho_duyet: "Chờ duyệt",
  da_len_lich: "Đã lên lịch",
  dang_dang: "Đang đăng",
  da_dang: "Đã đăng",
  bo: "Đã bỏ",
  loi: "Lỗi",
};

/** Gửi clip mới vào nhóm Telegram để duyệt */
export async function sendReview(id: string, manual = false) {
  if (!telegramReady()) {
    if (manual) throw httpError(400, "Chưa cấu hình Telegram (TELEGRAM_BOT_TOKEN / TELEGRAM_MKT_CHAT_ID)");
    return;
  }
  const c = await prisma.mktClip.findUnique({ where: { id } });
  if (!c) return;
  const cap = c.captions as any;
  const text =
    `🎬 <b>${esc(c.machineName || c.machineCode || "Clip mới")}</b>${c.machineCode ? ` (${esc(c.machineCode)})` : ""}\n` +
    `<i>${esc(c.hook)}</i>\n` +
    `⏱ ${Math.round(c.durationSec)}s · kênh: ${c.channels.map((x) => CHANNEL_LABEL[x as Channel] || x).join(", ")}\n\n` +
    esc(cut(cap?.facebook?.text || "", 650));
  const markup = keyboard([
    [
      { text: "✅ Duyệt & xếp lịch", callback_data: `mkt:a:${c.id}` },
      { text: "❌ Bỏ", callback_data: `mkt:r:${c.id}` },
    ],
    ...[crmButton(c.id, "✏️ Sửa caption / giờ đăng trên CRM")].filter(Boolean),
  ] as any);
  // Gửi video xem trước; Telegram không tải được video -> ảnh bìa -> chỉ chữ (vẫn có nút duyệt)
  const tries: Array<() => Promise<any>> = [
    async () =>
      tg("sendVideo", {
        chat_id: MKT_CHAT_ID(),
        video: await getPreviewUrl(c.previewKey || c.clipKey, 24 * 3600),
        caption: cut(text, 1020),
        parse_mode: "HTML",
        supports_streaming: true,
        reply_markup: markup,
      }),
    async () =>
      c.thumbKey
        ? tg("sendPhoto", { chat_id: MKT_CHAT_ID(), photo: await getPreviewUrl(c.thumbKey, 24 * 3600), caption: cut(text, 1020), parse_mode: "HTML", reply_markup: markup })
        : Promise.reject(new Error("không có ảnh bìa")),
    async () => tg("sendMessage", { chat_id: MKT_CHAT_ID(), text: cut(text, 4000), parse_mode: "HTML", reply_markup: markup }),
  ];
  let msg: any = null;
  let lastErr: any = null;
  for (const t of tries) {
    try {
      msg = await t();
      break;
    } catch (e) {
      lastErr = e;
      console.warn("[mkt] Telegram:", (e as any)?.message);
    }
  }
  if (!msg) throw lastErr;
  await prisma.mktClip.update({ where: { id }, data: { telegram: { chatId: msg.chat.id, messageId: msg.message_id } } });
}

/** Sau khi duyệt / bỏ: gỡ nút trên tin nhắn duyệt + trả lời trạng thái */
async function markReviewed(id: string, html: string) {
  if (!telegramReady()) return;
  const c = await prisma.mktClip.findUnique({ where: { id } });
  const t = c?.telegram as any;
  if (!t?.messageId) return;
  await tg("editMessageReplyMarkup", {
    chat_id: t.chatId,
    message_id: t.messageId,
    reply_markup: (() => {
      const btn = crmButton(id, `${STATUS_LABEL[c!.status] || c!.status} · mở trên CRM`);
      return keyboard(btn ? [btn] : []); // không có link CRM hợp lệ -> gỡ hết nút
    })(),
  }).catch(() => undefined);
  await tg("sendMessage", { chat_id: t.chatId, text: html, parse_mode: "HTML", reply_to_message_id: t.messageId }).catch(() => undefined);
}

/** Đã tới giờ đăng: báo kết quả kênh tự động + gửi sẵn clip & caption cho kênh đăng tay */
async function notifyPosted(id: string, manual: Channel[]) {
  if (!telegramReady()) return;
  const c = await prisma.mktClip.findUnique({ where: { id } });
  if (!c) return;
  const res = (c.results as any) || {};
  const t = c.telegram as any;
  const lines = [`📣 <b>${esc(c.machineName || c.machineCode || "Clip")}</b> — tới giờ đăng`];
  for (const ch of CHANNELS) {
    const r = res[ch];
    if (!r || !c.channels.includes(ch)) continue;
    if (r.ok) lines.push(`✅ ${CHANNEL_LABEL[ch]}: đã đăng${r.url ? ` — ${esc(r.url)}` : ""}`);
    else if (r.ok === false) lines.push(`⚠️ ${CHANNEL_LABEL[ch]}: lỗi — ${esc(cut(r.error || "", 300))}`);
  }
  if (manual.length) lines.push(`👉 Cần đăng tay: <b>${manual.map((m) => CHANNEL_LABEL[m]).join(", ")}</b> — clip + caption ngay bên dưới`);
  await tg("sendMessage", {
    chat_id: MKT_CHAT_ID(),
    text: lines.join("\n"),
    parse_mode: "HTML",
    disable_web_page_preview: true,
    ...(t?.messageId ? { reply_to_message_id: t.messageId } : {}),
  });
  if (!manual.length) return;

  const link = await getDownloadUrl(c.clipKey, `${keySafe(c.machineCode || "clip")}_${c.id}.mp4`, 7 * 24 * 3600);
  await tg("sendMessage", {
    chat_id: MKT_CHAT_ID(),
    text: `⬇️ <a href="${esc(link)}">Tải clip gốc 1080×1920 để đăng</a> (link dùng được 7 ngày)`,
    parse_mode: "HTML",
  });
  const cap = c.captions as any;
  if (manual.includes("tiktok") && cap?.tiktok?.text) {
    await tg("sendMessage", { chat_id: MKT_CHAT_ID(), text: `🎵 <b>Caption TikTok</b> (chạm để copy)\n<pre>${esc(cut(cap.tiktok.text, 3500))}</pre>`, parse_mode: "HTML" });
  }
  if (manual.includes("youtube") && (cap?.youtube?.title || cap?.youtube?.description)) {
    await tg("sendMessage", { chat_id: MKT_CHAT_ID(), text: `▶️ <b>Tiêu đề YouTube Shorts</b>\n<pre>${esc(cap.youtube.title || "")}</pre>`, parse_mode: "HTML" });
    await tg("sendMessage", { chat_id: MKT_CHAT_ID(), text: `▶️ <b>Mô tả YouTube</b>\n<pre>${esc(cut(cap.youtube.description || "", 3500))}</pre>`, parse_mode: "HTML" });
  }
  if (manual.includes("facebook") && cap?.facebook?.text) {
    await tg("sendMessage", { chat_id: MKT_CHAT_ID(), text: `📘 <b>Caption Facebook</b>\n<pre>${esc(cut(cap.facebook.text, 3500))}</pre>`, parse_mode: "HTML" });
  }
}

/** Webhook: người trong nhóm bấm nút ✅ / ❌ */
export async function handleTelegramUpdate(update: any) {
  const cb = update?.callback_query;
  if (!cb?.data || !String(cb.data).startsWith("mkt:")) return;
  const answer = (text: string) => tg("answerCallbackQuery", { callback_query_id: cb.id, text, show_alert: false }).catch(() => undefined);
  if (String(cb.message?.chat?.id) !== String(MKT_CHAT_ID())) return answer("Nhóm này không được duyệt bài");
  const [, action, id] = String(cb.data).split(":");
  const who = cb.from?.username ? `@${cb.from.username}` : [cb.from?.first_name, cb.from?.last_name].filter(Boolean).join(" ") || "Telegram";
  try {
    if (action === "a") {
      const c = await approveClip(id, who);
      return answer(`Đã duyệt · đăng lúc ${fmtVN(c.scheduledAt!)}`);
    }
    if (action === "r") {
      await rejectClip(id, who);
      return answer("Đã bỏ clip");
    }
  } catch (e: any) {
    return answer(e?.message || "Lỗi");
  }
}