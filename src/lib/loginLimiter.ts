// src/lib/loginLimiter.ts — ✅ MỚI (bảo mật): chặn dò mật khẩu
// Sai quá MAX_FAILS lần trong WINDOW_MS (tính theo tài khoản VÀ theo IP) -> khoá LOCK_MS.
// Lưu trong bộ nhớ: đủ cho 1 server Render. Server khởi động lại thì bộ đếm về 0.
const MAX_FAILS_USER = 5; // mỗi tài khoản
const MAX_FAILS_IP = 20; // mỗi IP (thử nhiều tài khoản khác nhau)
const WINDOW_MS = 15 * 60 * 1000;
const LOCK_MS = 15 * 60 * 1000;

type Rec = { count: number; first: number; lockedUntil: number };
const recs = new Map<string, Rec>();

function get(key: string, now: number) {
  const r = recs.get(key);
  if (!r) return null;
  if (r.lockedUntil && now >= r.lockedUntil) {
    recs.delete(key);
    return null;
  }
  if (!r.lockedUntil && now - r.first > WINDOW_MS) {
    recs.delete(key);
    return null;
  }
  return r;
}

const keys = (username: string, ip: string) => [`u:${String(username || "").trim().toLowerCase()}`, `ip:${ip || "?"}`];

/** Còn bị khoá bao nhiêu giây (0 = không khoá) */
export function lockedSeconds(username: string, ip: string) {
  const now = Date.now();
  let max = 0;
  for (const k of keys(username, ip)) {
    const r = get(k, now);
    if (r?.lockedUntil) max = Math.max(max, Math.ceil((r.lockedUntil - now) / 1000));
  }
  return max;
}

export function recordFail(username: string, ip: string) {
  const now = Date.now();
  keys(username, ip).forEach((k, i) => {
    const limit = i === 0 ? MAX_FAILS_USER : MAX_FAILS_IP;
    const r = get(k, now) ?? { count: 0, first: now, lockedUntil: 0 };
    r.count++;
    if (r.count >= limit) r.lockedUntil = now + LOCK_MS;
    recs.set(k, r);
  });
  // dọn bớt cho đỡ tốn bộ nhớ
  if (recs.size > 5000) for (const [k] of recs) if (!get(k, now)) recs.delete(k);
}

export function recordSuccess(username: string, ip: string) {
  recs.delete(keys(username, ip)[0]);
}

/** IP thật của người gọi (Render đứng sau proxy -> lấy x-forwarded-for đầu tiên) */
export function clientIp(req: { headers: any; socket?: any; ip?: string }) {
  const xf = String(req.headers?.["x-forwarded-for"] || "").split(",")[0].trim();
  return xf || req.ip || req.socket?.remoteAddress || "?";
}
