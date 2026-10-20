import { Router } from 'express';
import { login, register } from '../services/auth.service';
import { requireAuth, requireRole } from '../middlewares/auth';
import { lockedSeconds, recordFail, recordSuccess, clientIp } from '../lib/loginLimiter';

const r = Router();

/** Đăng ký — ✅ bảo mật: CHỈ ADMIN đang đăng nhập (trước đây ai cũng tự tạo được tài khoản admin) */
r.post('/register', requireAuth, requireRole('admin'), async (req, res, next) => {
  try {
    const { username, password, role } = req.body ?? {};
    if (typeof username !== 'string' || typeof password !== 'string' || !username || password.length < 6) {
      return res.status(400).json({ ok: false, message: 'Username / mật khẩu (ít nhất 6 ký tự) không hợp lệ' });
    }
    const safeRole = ['staff', 'accountant', 'admin'].includes(role) ? role : 'staff';
    const data = await register(username, password, safeRole);
    res.json({ ok: true, data });
  } catch (e) { next(e); }
});

/** Đăng nhập → trả token + user */
r.post('/login', async (req, res, next) => {
  try {
    const { username, password } = req.body ?? {};
    // ✅ sai quá 5 lần -> khoá 15 phút (dùng chung bộ đếm với /api/auth/login)
    const ip = clientIp(req);
    const wait = lockedSeconds(String(username || ''), ip);
    if (wait > 0) return res.status(429).json({ ok: false, message: `Đăng nhập sai quá nhiều lần. Thử lại sau ${Math.ceil(wait / 60)} phút.` });
    let data;
    try {
      data = await login(String(username || ''), String(password || ''));
    } catch (e) {
      recordFail(String(username || ''), ip);
      throw e;
    }
    recordSuccess(String(username || ''), ip);
    // data nên có: { token, user }
    res.json({ ok: true, ...data });
  } catch (e: any) {
    e.status = e.status || 401;
    next(e);
  }
});

export default r;
