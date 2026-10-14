// src/routes/mkt.routes.ts
// ✅ MKT tự động: video thô -> clip dọc + bài viết -> duyệt (Telegram / CRM) -> đăng
//   /api/mkt/worker/*            máy công ty gọi (header x-worker-key = MKT_WORKER_KEY)
//   /api/mkt/telegram/webhook    Telegram gọi khi bấm nút ✅ / ❌ (header bí mật TELEGRAM_WEBHOOK_SECRET)
//   /api/mkt/*                   trang "Hàng chờ MKT" trên CRM (đăng nhập)
import "dotenv/config"; // đọc .env sớm (chế độ TELEGRAM_POLLING khởi động ngay khi nạp file)
import express, { Router, type Request, type Response, type NextFunction } from "express";
import { timingSafeEqual } from "crypto";
import { requireAuth, requireRole, getUser } from "../middlewares/auth";
import { tg, MKT_CHAT_ID, WEBHOOK_SECRET, telegramReady } from "../lib/telegram";
import {
  claimSource,
  uploadUrls,
  finishSource,
  failSource,
  claimDue,
  postResult,
  listClips,
  summary,
  getClip,
  updateClip,
  approveClip,
  rejectClip,
  unscheduleClip,
  deleteClip,
  listSources,
  retrySource,
  resetSource,
  queueSources,
  nextSlot,
  handleTelegramUpdate,
  sendReview,
  ping,
  setAutoMode,
} from "../services/mkt.service";

const r = Router();
r.use(express.json({ limit: "2mb" }));

type H = (req: Request, res: Response) => Promise<unknown>;
const run = (fn: H) => (req: Request, res: Response, next: NextFunction) => {
  fn(req, res)
    .then((out) => {
      if (!res.headersSent) res.json(out);
    })
    .catch(next);
};
const pid = (req: Request) => String(req.params.id);

// ================================================================ máy công ty
function workerAuth(req: Request, res: Response, next: NextFunction) {
  const key = process.env.MKT_WORKER_KEY || "";
  if (!key) return res.status(503).json({ message: "Server chưa cấu hình MKT_WORKER_KEY" });
  const got = Buffer.from(String(req.headers["x-worker-key"] || ""));
  const want = Buffer.from(key);
  if (got.length !== want.length || !timingSafeEqual(got, want)) return res.status(401).json({ message: "Sai x-worker-key" });
  next();
}
const worker = Router();
worker.use(workerAuth);
worker.post("/ping", run(() => ping()));
worker.post("/claim", run(() => claimSource()));
worker.post("/upload-urls", run((req) => uploadUrls(String(req.body?.jobId), req.body?.files)));
worker.post("/jobs/:id/done", run((req) => finishSource(pid(req), req.body ?? {})));
worker.post("/jobs/:id/fail", run((req) => failSource(pid(req), String(req.body?.error || "Lỗi không rõ"))));
worker.post("/due", run(() => claimDue()));
worker.post("/clips/:id/result", run((req) => postResult(pid(req), req.body?.results ?? {})));
r.use("/worker", worker);

// ================================================================ Telegram webhook
r.post("/telegram/webhook", async (req, res) => {
  if (!WEBHOOK_SECRET() || req.headers["x-telegram-bot-api-secret-token"] !== WEBHOOK_SECRET()) return res.sendStatus(401);
  res.sendStatus(200); // trả lời Telegram ngay, xử lý sau
  handleTelegramUpdate(req.body).catch((e) => console.warn("[mkt] webhook:", e?.message));
});

// ================================================================ CRM (đăng nhập)
r.use(requireAuth);
r.use(requireRole("admin")); // ✅ trang Hàng chờ MKT: chỉ admin (duyệt trên Telegram vẫn dành cho người trong nhóm)
const who = (req: Request) => getUser(req)?.username || "CRM";

r.get("/summary", run(() => summary()));
r.get("/clips", run((req) => listClips(req.query)));
r.get("/clips/:id", run((req) => getClip(pid(req))));
r.patch("/clips/:id", run((req) => updateClip(pid(req), req.body ?? {})));
r.post("/clips/:id/approve", run((req) => approveClip(pid(req), who(req), req.body?.scheduledAt)));
r.post("/clips/:id/reject", run((req) => rejectClip(pid(req), who(req))));
r.post("/clips/:id/post-now", run((req) => approveClip(pid(req), who(req), new Date())));
r.post("/clips/:id/unschedule", run((req) => unscheduleClip(pid(req))));
r.post("/clips/:id/telegram", run(async (req) => (await sendReview(pid(req), true), { ok: true })));
r.delete("/clips/:id", run((req) => deleteClip(pid(req))));
r.get("/next-slot", run(async () => ({ at: await nextSlot() })));
/** Admin: bật / tắt chế độ TỰ ĐỘNG (tắt = bán tự động: chỉ làm video được bấm "Cho AI làm") */
r.post("/settings/auto", requireRole("admin"), run((req) => setAutoMode(!!req.body?.auto, who(req))));
r.get("/sources", run((req) => listSources(String(req.query.q || ""))));
r.post("/sources/queue", run((req) => queueSources(req.body?.videoIds)));
r.post("/sources/:id/retry", run((req) => retrySource(pid(req))));
r.post("/sources/:id/reset", run((req) => resetSource(pid(req))));

/** Admin: nối bot Telegram vào server (chạy 1 lần sau khi đặt biến môi trường) */
r.post(
  "/telegram/setup",
  requireRole("admin"),
  run(async (req) => {
    if (!telegramReady()) throw Object.assign(new Error("Thiếu TELEGRAM_BOT_TOKEN hoặc TELEGRAM_MKT_CHAT_ID"), { statusCode: 400 });
    // Chạy trên máy (localhost): Telegram không gọi vào được -> server tự hỏi Telegram (TELEGRAM_POLLING=1)
    if (pollingMode()) {
      await tg("deleteWebhook", {});
      startTelegramPolling();
      await tg("sendMessage", { chat_id: MKT_CHAT_ID(), text: "✅ Bot MKT McBrother đã kết nối (chế độ chạy trên máy). Clip mới sẽ được gửi vào nhóm này để duyệt." });
      return { ok: true, mode: "polling" };
    }
    if (!WEBHOOK_SECRET()) throw Object.assign(new Error("Thiếu TELEGRAM_WEBHOOK_SECRET"), { statusCode: 400 });
    const base =
      (process.env.MKT_PUBLIC_API_URL || "").replace(/\/+$/, "") ||
      `${String(req.headers["x-forwarded-proto"] || req.protocol).split(",")[0]}://${req.get("host")}`;
    if (/localhost|127\.0\.0\.1/.test(base))
      throw Object.assign(new Error("Server đang chạy trên máy (localhost), Telegram không gọi vào được. Thêm TELEGRAM_POLLING=1 vào .env rồi chạy lại server"), { statusCode: 400 });
    const url = `${base}/api/mkt/telegram/webhook`;
    await tg("setWebhook", { url, secret_token: WEBHOOK_SECRET(), allowed_updates: ["callback_query"] });
    await tg("sendMessage", { chat_id: MKT_CHAT_ID(), text: "✅ Bot MKT McBrother đã kết nối. Clip mới sẽ được gửi vào nhóm này để duyệt." });
    return { ok: true, mode: "webhook", webhook: url };
  })
);

// ================================================================ chế độ chạy trên máy (TELEGRAM_POLLING=1)
// Server tự hỏi Telegram có ai bấm nút ✅ / ❌ không (không cần địa chỉ công khai). Trên Render KHÔNG bật — dùng webhook.
const pollingMode = () => process.env.TELEGRAM_POLLING === "1";
let polling = false;
function startTelegramPolling() {
  if (polling || !pollingMode() || !telegramReady()) return;
  polling = true;
  let offset = 0;
  const loop = async () => {
    try {
      const updates: any[] = await tg("getUpdates", { offset, timeout: 25, allowed_updates: ["callback_query"] });
      for (const u of updates) {
        offset = u.update_id + 1;
        await handleTelegramUpdate(u).catch((e) => console.warn("[mkt] telegram:", e?.message));
      }
    } catch (e: any) {
      console.warn("[mkt] telegram polling:", e?.message);
      await new Promise((r) => setTimeout(r, 10_000));
    }
    setImmediate(loop);
  };
  tg("deleteWebhook", {})
    .catch(() => undefined)
    .finally(() => {
      console.log("[mkt] Telegram: chế độ chạy trên máy (polling)");
      loop();
    });
}
startTelegramPolling();

export default r;