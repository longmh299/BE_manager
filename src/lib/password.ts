// src/lib/password.ts — ✅ MỚI (bảo mật): mật khẩu mã hoá bcrypt
// Database cũ đang lưu mật khẩu dạng chữ thường -> vẫn đăng nhập được như cũ, và NGAY LẦN ĐĂNG NHẬP ĐÚNG đó
// hệ thống tự đổi sang bản mã hoá (needsUpgrade). Không ai phải đặt lại mật khẩu.
import bcrypt from "bcrypt";
import { timingSafeEqual, createHash } from "crypto";

const ROUNDS = 10;

export const isBcryptHash = (s: string | null | undefined) => /^\$2[aby]\$\d{2}\$/.test(String(s || ""));

export function hashPassword(plain: string) {
  return bcrypt.hash(String(plain), ROUNDS);
}

/** So sánh chữ thường an toàn (không lộ thời gian so sánh) */
function safeEqual(a: string, b: string) {
  const ha = createHash("sha256").update(a).digest();
  const hb = createHash("sha256").update(b).digest();
  return timingSafeEqual(ha, hb);
}

/** ok: đúng mật khẩu. needsUpgrade: mật khẩu cũ còn dạng chữ thường -> nên lưu lại bản mã hoá */
export async function verifyPassword(stored: string | null | undefined, input: string): Promise<{ ok: boolean; needsUpgrade: boolean }> {
  const s = String(stored ?? "");
  const p = String(input ?? "");
  if (!s || !p) return { ok: false, needsUpgrade: false };
  if (isBcryptHash(s)) return { ok: await bcrypt.compare(p, s), needsUpgrade: false };
  const ok = safeEqual(s, p);
  return { ok, needsUpgrade: ok };
}
