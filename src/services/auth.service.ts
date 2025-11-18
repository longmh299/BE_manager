import { PrismaClient } from '@prisma/client';
import bcrypt from 'bcrypt';
import jwt from 'jsonwebtoken';
import { config } from '../config';

const prisma = new PrismaClient();

export async function register(username: string, password: string, role: 'staff'|'accountant'|'admin'='staff') {
  const hash = await bcrypt.hash(password, 10);
  const user = await prisma.user.create({ data: { username, password: hash, role }});
  return { id: user.id, username: user.username, role: user.role };
}

export async function login(username: string, password: string) {
  const user = await prisma.user.findUnique({ where: { username }});
  if (!user) throw new Error('User not found');
  const ok = await bcrypt.compare(password, user.password);
  if (!ok) throw new Error('Wrong password');
  const token = jwt.sign({ id: user.id, username: user.username, role: user.role }, config.jwtSecret, { expiresIn: '7d' });
  return { token, user: { id: user.id, username: user.username, role: user.role }};
}
