import { Router } from "express";
import { PrismaClient } from "@prisma/client";
import {
  signJwt,
  requireAuth,
  requireRole,
  getUser,
} from "../middlewares/auth";
import { hashPassword, verifyPassword } from "../lib/password"; // ✅ bảo mật: mật khẩu mã hoá bcrypt
import { lockedSeconds, recordFail, recordSuccess, clientIp } from "../lib/loginLimiter"; // ✅ bảo mật: chặn dò mật khẩu

const prisma = new PrismaClient();
export const authRouter = Router();

/** Tạo tài khoản mới — ✅ bảo mật: CHỈ ADMIN đang đăng nhập (trước đây ai trên mạng cũng tự tạo được tài khoản admin) */
authRouter.post(
  "/register",
  requireAuth,
  requireRole("admin"),
  async (req, res) => {
    try {
      const { username, password, role } = req.body ?? {};
      if (!username || !password) {
        return res
          .status(400)
          .json({ message: "Missing username/password" });
      }
      if (typeof username !== "string" || typeof password !== "string") {
        return res.status(400).json({ message: "Username / mật khẩu không hợp lệ." });
      }
      if (password.length < 6) {
        return res.status(400).json({ message: "Mật khẩu phải có ít nhất 6 ký tự." });
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

      // ✅ lưu bản mã hoá, không lưu chữ thường
      const user = await prisma.user.create({
        data: { username, password: await hashPassword(password), role: userRole as any },
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

    if (typeof username !== "string" || typeof password !== "string") {
      return res.status(400).json({ message: "Missing username/password" });
    }

    // ✅ sai quá 5 lần -> khoá 15 phút
    const ip = clientIp(req);
    const wait = lockedSeconds(username, ip);
    if (wait > 0) {
      res.setHeader("Retry-After", String(wait));
      return res.status(429).json({ message: `Đăng nhập sai quá nhiều lần. Thử lại sau ${Math.ceil(wait / 60)} phút.` });
    }

    const user = await prisma.user.findUnique({ where: { username } });
    const check = await verifyPassword(user?.password, password);
    if (!user || !check.ok) {
      recordFail(username, ip);
      return res
        .status(401)
        .json({ message: "Sai tài khoản hoặc mật khẩu" });
    }
    recordSuccess(username, ip);

    // ✅ mật khẩu cũ còn lưu chữ thường -> đổi sang bản mã hoá ngay (người dùng không phải làm gì)
    if (check.needsUpgrade) {
      await prisma.user
        .update({ where: { id: user.id }, data: { password: await hashPassword(password) } })
        .catch((e) => console.error("Upgrade password hash error:", e));
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

/** Ai đang đăng nhập — trả thêm email/phone (dùng để tự động chèn vào file báo giá khi tải xuống) */
authRouter.get("/me", requireAuth, async (req, res) => {
  const u = getUser(req);
  if (!u) return res.status(401).json({ message: "Unauthorized" });
  try {
    const dbUser = await prisma.user.findUnique({
      where: { id: u.id },
      select: { id: true, username: true, role: true, email: true, phone: true },
    });
    if (!dbUser) return res.status(404).json({ message: "User not found" });
    return res.json(dbUser);
  } catch (e: any) {
    return res.status(500).json({ message: e.message ?? "Internal error" });
  }
});

/** ✅ Lưu email/SĐT liên hệ của chính user — dùng để tự động chèn vào file báo giá khi tải xuống */
authRouter.put("/profile", requireAuth, async (req, res) => {
  try {
    const u = getUser(req);
    if (!u) return res.status(401).json({ message: "Unauthorized" });

    const { email, phone } = req.body ?? {};
    if (email !== undefined && email !== null && email.trim() && !/^\S+@\S+\.\S+$/.test(email.trim())) {
      return res.status(400).json({ message: "Email không hợp lệ." });
    }

    const updated = await prisma.user.update({
      where: { id: u.id },
      data: {
        email: email !== undefined ? (email.trim() || null) : undefined,
        phone: phone !== undefined ? (phone.trim() || null) : undefined,
      },
      select: { id: true, username: true, role: true, email: true, phone: true },
    });
    return res.json(updated);
  } catch (e: any) {
    console.error("Update profile error:", e);
    return res.status(500).json({ message: e.message ?? "Không lưu được thông tin." });
  }
});

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

    // ✅ so sánh được cả mật khẩu cũ dạng chữ thường lẫn bản mã hoá
    if (!(await verifyPassword(dbUser.password, String(oldPassword))).ok) {
      return res
        .status(400)
        .json({ message: "Mật khẩu hiện tại không đúng." });
    }

    if (typeof newPassword !== "string" || newPassword.length < 6) {
      return res
        .status(400)
        .json({ message: "Mật khẩu mới phải có ít nhất 6 ký tự." });
    }

    await prisma.user.update({
      where: { id: user.id },
      data: {
        password: await hashPassword(newPassword),
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