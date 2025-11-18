// src/routes/users.routes.ts
import { Router } from "express";
import { PrismaClient } from "@prisma/client";
import {requireAuth} from "../middlewares/auth"; // hoặc đường dẫn middleware auth hiện tại

const prisma = new PrismaClient();
const r = Router();

// bắt buộc đăng nhập
r.use(requireAuth);

// GET /api/users?pageSize=100&page=1
r.get("/", async (req, res, next) => {
  try {
    const pageSize = Number(req.query.pageSize) || 50;
    const page = Number(req.query.page) || 1;

    const [items, total] = await Promise.all([
      prisma.user.findMany({
        skip: (page - 1) * pageSize,
        take: pageSize,
        orderBy: { username: "asc" },
        select: {
          id: true,
          username: true,
          role: true,
        },
      }),
      prisma.user.count(),
    ]);

    res.json({ items, total });
  } catch (err) {
    next(err);
  }
});

export default r;
