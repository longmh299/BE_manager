import { Router } from "express";
import { PrismaClient } from "@prisma/client";
import {
  signJwt,
  requireAuth,
  meHandler,
  requireRole,
  getUser,
} from "../middlewares/auth";

const prisma = new PrismaClient();
export const authRouter = Router();

/** Đăng ký tài khoản mới (tùy bạn: có thể mở tạm thời, hoặc yêu cầu admin) */
authRouter.post(
  "/register",
  /*requireRole('admin'),*/ async (req, res) => {
    try {
      const { username, password, role } = req.body ?? {};
      if (!username || !password) {
        return res
          .status(400)
          .json({ message: "Missing username/password" });
      }

      // role mặc định staff nếu không truyền
      const userRole =
        role && ["staff", "accountant", "admin"].includes(role)
          ? role
          : "staff";

      const exist = await prisma.user.findUnique({ where: { username } });
      if (exist) {
        return res.status(409).json({ message: "Username already exists" });
      }

      // NOTE: hiện password đang plaintext theo schema của bạn
      const user = await prisma.user.create({
        data: { username, password, role: userRole as any },
      });

      return res
        .status(201)
        .json({ id: user.id, username: user.username, role: user.role });
    } catch (e: any) {
      return res
        .status(500)
        .json({ message: e.message ?? "Internal error" });
    }
  }
);

/** Đăng nhập lấy JWT */
authRouter.post("/login", async (req, res) => {
  try {
    const { username, password } = req.body ?? {};
    if (!username || !password) {
      return res
        .status(400)
        .json({ message: "Missing username/password" });
    }

    const user = await prisma.user.findUnique({ where: { username } });
    if (!user || user.password !== password) {
      return res
        .status(401)
        .json({ message: "Sai tài khoản hoặc mật khẩu" });
    }

    const token = signJwt({
      id: user.id,
      username: user.username,
      role: user.role as any,
    });
    return res.json({ token });
  } catch (e: any) {
    return res
      .status(500)
      .json({ message: e.message ?? "Internal error" });
  }
});

/** Ai đang đăng nhập */
authRouter.get("/me", requireAuth, meHandler);

/** Đổi mật khẩu */
authRouter.post("/change-password", requireAuth, async (req, res) => {
  try {
    const { oldPassword, newPassword } = req.body ?? {};

    if (!oldPassword || !newPassword) {
      return res.status(400).json({
        message: "Vui lòng nhập đủ mật khẩu hiện tại và mật khẩu mới.",
      });
    }

    const user = getUser(req);
    if (!user) {
      return res.status(401).json({ message: "Unauthorized" });
    }

    // Lấy user từ DB
    const dbUser = await prisma.user.findUnique({
      where: { id: user.id },
    });

    if (!dbUser) {
      return res.status(404).json({ message: "User not found" });
    }

    // Hiện tại bạn đang lưu password dạng plaintext
    if (dbUser.password !== oldPassword) {
      return res
        .status(400)
        .json({ message: "Mật khẩu hiện tại không đúng." });
    }

    if (newPassword.length < 6) {
      return res
        .status(400)
        .json({ message: "Mật khẩu mới phải có ít nhất 6 ký tự." });
    }

    await prisma.user.update({
      where: { id: user.id },
      data: {
        password: newPassword,
      },
    });

    return res.json({ message: "Đổi mật khẩu thành công." });
  } catch (e: any) {
    console.error("Change password error:", e);
    return res.status(500).json({
      message: e.message ?? "Không đổi được mật khẩu. Thử lại.",
    });
  }
});

/** ADMIN – Lấy danh sách user */
authRouter.get(
  "/users",
  requireAuth,
  requireRole("admin"),
  async (_req, res) => {
    try {
      const users = await prisma.user.findMany({
        select: {
          id: true,
          username: true,
          role: true,
          createdAt: true,
        },
        orderBy: {
          createdAt: "desc",
        },
      });

      return res.json(users);
    } catch (e: any) {
      return res
        .status(500)
        .json({ message: e.message ?? "Internal error" });
    }
  }
);

/** ADMIN – Đổi role của 1 user */
authRouter.patch(
  "/users/:id/role",
  requireAuth,
  requireRole("admin"),
  async (req, res) => {
    try {
      const { id } = req.params;
      const { role } = req.body ?? {};

      if (!role || !["staff", "accountant", "admin"].includes(role)) {
        return res.status(400).json({ message: "Role không hợp lệ." });
      }

      // Có thể chặn không cho tự đổi role của chính mình nếu muốn
      // const current = getUser(req);
      // if (current?.id === id) {
      //   return res
      //     .status(400)
      //     .json({ message: "Không thể đổi role của chính mình." });
      // }

      const user = await prisma.user.update({
        where: { id },
        data: { role },
        select: {
          id: true,
          username: true,
          role: true,
          createdAt: true,
        },
      });

      return res.json(user);
    } catch (e: any) {
      console.error("Update user role error:", e);
      return res
        .status(500)
        .json({ message: e.message ?? "Internal error" });
    }
  }
);
