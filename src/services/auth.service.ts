import { PrismaClient } from '@prisma/client';
import bcrypt from 'bcrypt';
import jwt from 'jsonwebtoken';
import { config } from '../config';
import { verifyPassword, hashPassword } from '../lib/password';

const prisma = new PrismaClient();

export async function register(username: string, password: string, role: 'staff'|'accountant'|'admin'='staff') {
  const hash = await bcrypt.hash(password, 10);
  const user = await prisma.user.create({ data: { username, password: hash, role }});
  return { id: user.id, username: user.username, role: user.role };
}

export async function login(username: string, password: string) {
  const user = await prisma.user.findUnique({ where: { username }});
  // ✅ không nói rõ "sai tài khoản" hay "sai mật khẩu" (tránh dò tên đăng nhập); hỗ trợ cả mật khẩu cũ dạng chữ thường
  const check = await verifyPassword(user?.password, password);
  if (!user || !check.ok) throw new Error('Sai tài khoản hoặc mật khẩu');
  if (check.needsUpgrade) {
    await prisma.user.update({ where: { id: user.id }, data: { password: await hashPassword(password) } }).catch(() => undefined);
  }
  const token = jwt.sign({ id: user.id, username: user.username, role: user.role }, config.jwtSecret, { expiresIn: '7d' });
  return { token, user: { id: user.id, username: user.username, role: user.role }};
}
