// src/lib/telegram.ts
// ✅ Gọi Telegram Bot API tối giản (không cần thư viện).
// Biến môi trường (Render -> Environment):
//   TELEGRAM_BOT_TOKEN       token bot lấy từ @BotFather
//   TELEGRAM_MKT_CHAT_ID     id nhóm duyệt bài MKT (số âm, vd -1001234567890)
//   TELEGRAM_WEBHOOK_SECRET  chuỗi bí mật tự đặt (chống người lạ giả Telegram gọi vào webhook)
const TOKEN = () => process.env.TELEGRAM_BOT_TOKEN || "";
export const MKT_CHAT_ID = () => process.env.TELEGRAM_MKT_CHAT_ID || "";
export const WEBHOOK_SECRET = () => process.env.TELEGRAM_WEBHOOK_SECRET || "";

export const telegramReady = () => !!TOKEN() && !!MKT_CHAT_ID();

export type InlineButton = { text: string; callback_data?: string; url?: string };

export async function tg<T = any>(method: string, body: Record<string, unknown>): Promise<T> {
  if (!TOKEN()) throw new Error("Chưa cấu hình TELEGRAM_BOT_TOKEN");
  const r = await fetch(`https://api.telegram.org/bot${TOKEN()}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const j: any = await r.json().catch(() => ({}));
  if (!j.ok) {
    const mig = j.parameters?.migrate_to_chat_id;
    throw new Error(
      mig
        ? `Nhóm Telegram đã chuyển thành siêu nhóm, ID mới là ${mig} — sửa TELEGRAM_MKT_CHAT_ID=${mig} rồi chạy lại server`
        : `Telegram ${method}: ${j.description || r.status}`
    );
  }
  return j.result as T;
}

/** Chữ thường -> an toàn cho parse_mode HTML */
export const esc = (s: unknown) =>
  String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

export const cut = (s: string, n: number) => (s.length > n ? s.slice(0, n - 1) + "…" : s);

export const keyboard = (rows: InlineButton[][]) => ({ inline_keyboard: rows });