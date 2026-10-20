// src/middlewares/stripSecrets.ts — ✅ MỚI (bảo mật)
// Lưới an toàn: KHÔNG BAO GIỜ trả trường mật khẩu ra ngoài, dù route nào lỡ include nguyên bản ghi User
// (vd hoá đơn include saleUser / approvedBy, phiếu thu include createdBy -> trước đây lộ cả mật khẩu khi bấm F12).
import type { Request, Response, NextFunction } from "express";

const SECRET_KEYS = new Set(["password", "passwordHash", "resetToken"]);

function isPlain(v: any) {
  if (v === null || typeof v !== "object") return false;
  const p = Object.getPrototypeOf(v);
  return p === Object.prototype || p === null;
}

/** Xoá các khoá bí mật trong object / mảng thường (không đụng Date, Decimal, Buffer...) */
export function stripSecrets<T>(body: T, seen = new WeakSet<object>()): T {
  if (Array.isArray(body)) {
    if (seen.has(body)) return body;
    seen.add(body);
    for (const x of body) stripSecrets(x, seen);
    return body;
  }
  if (!isPlain(body)) return body;
  const o = body as any;
  if (seen.has(o)) return body;
  seen.add(o);
  for (const k of Object.keys(o)) {
    if (SECRET_KEYS.has(k)) delete o[k];
    else stripSecrets(o[k], seen);
  }
  return body;
}

export function stripSecretsMiddleware(_req: Request, res: Response, next: NextFunction) {
  const json = res.json.bind(res);
  res.json = ((body: any) => json(stripSecrets(body))) as any;
  next();
}
