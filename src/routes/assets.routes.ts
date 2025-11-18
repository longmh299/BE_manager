import { Router } from 'express';
import { login, register } from '../services/auth.service';

const r = Router();

/** Đăng ký (có thể tắt ở prod) */
r.post('/register', async (req, res, next) => {
  try {
    const { username, password, role } = req.body;
    const data = await register(username, password, role);
    res.json({ ok: true, data });
  } catch (e) { next(e); }
});

/** Đăng nhập → trả token + user */
r.post('/login', async (req, res, next) => {
  try {
    const { username, password } = req.body;
    const data = await login(username, password);
    // data nên có: { token, user }
    res.json({ ok: true, ...data });
  } catch (e: any) {
    e.status = e.status || 401;
    next(e);
  }
});

export default r;
