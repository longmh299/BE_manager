import { Router } from 'express';
import { PrismaClient } from '@prisma/client';
import { signJwt, requireAuth, meHandler, requireRole } from '../middlewares/auth';

const prisma = new PrismaClient();
export const authRouter = Router();

/** Đăng ký tài khoản mới (tùy bạn: có thể mở tạm thời, hoặc yêu cầu admin) */
authRouter.post('/register', /*requireRole('admin'),*/ async (req, res) => {
  try {
    const { username, password, role } = req.body ?? {};
    if (!username || !password) return res.status(400).json({ message: 'Missing username/password' });

    // role mặc định staff nếu không truyền
    const userRole = role && ['staff','accountant','admin'].includes(role) ? role : 'staff';

    const exist = await prisma.user.findUnique({ where: { username } });
    if (exist) return res.status(409).json({ message: 'Username already exists' });

    // NOTE: hiện password đang plaintext theo schema của bạn
    const user = await prisma.user.create({
      data: { username, password, role: userRole as any }
    });

    return res.status(201).json({ id: user.id, username: user.username, role: user.role });
  } catch (e: any) {
    return res.status(500).json({ message: e.message ?? 'Internal error' });
  }
});

/** Đăng nhập lấy JWT */
authRouter.post('/login', async (req, res) => {
  try {
    const { username, password } = req.body ?? {};
    if (!username || !password) return res.status(400).json({ message: 'Missing username/password' });

    const user = await prisma.user.findUnique({ where: { username } });
    if (!user || user.password !== password) {
      return res.status(401).json({ message: 'Sai tài khoản hoặc mật khẩu' });
    }

    const token = signJwt({ id: user.id, username: user.username, role: user.role as any });
    return res.json({ token });
  } catch (e: any) {
    return res.status(500).json({ message: e.message ?? 'Internal error' });
  }
});

/** Ai đang đăng nhập */
authRouter.get('/me', requireAuth, meHandler);
