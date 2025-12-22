// src/middlewares/auth.ts
import type { Request, Response, NextFunction } from "express";
import jwt, { JwtPayload, SignOptions, Secret } from "jsonwebtoken";
import { PrismaClient } from "@prisma/client";
import { config } from "../config";

const prisma = new PrismaClient();

export type UserRole = "staff" | "accountant" | "admin";

export interface JwtUser {
  id: string;
  username: string;
  role: UserRole;
}

/**
 * Ký JWT cho user
 */
export function signJwt(user: JwtUser, opts?: { expiresIn?: string | number }) {
  const payload: JwtPayload & JwtUser = {
    ...user,
    sub: user.id,
  } as any;

  const expiresIn: SignOptions["expiresIn"] = (opts?.expiresIn ?? "7d") as any;

  const options: SignOptions = {
    algorithm: "HS256",
    expiresIn,
  };

  return jwt.sign(payload, config.jwtSecret as Secret, options);
}

/** READ Bearer token (case-insensitive), trim whitespace; fallback cookie 'token' */
function parseToken(req: Request): string | null {
  const raw =
    (req.headers["authorization"] as string | undefined) ??
    (req.headers as any)["Authorization"];

  if (raw && typeof raw === "string") {
    const m = raw.match(/^Bearer\s+(.+)$/i);
    if (m && m[1]) return m[1].trim();
  }

  // @ts-ignore
  const cookieToken: string | undefined = req.cookies?.token;
  return cookieToken || null;
}

async function hydrateUserFromDb(payload: JwtPayload & Partial<JwtUser>) {
  const claimedId = (payload.sub as string) || (payload as any).id;
  const claimedUsername = (payload as any).username;

  // 1) ưu tiên tìm theo id trong token (nếu có)
  if (claimedId) {
    const u = await prisma.user.findUnique({ where: { id: String(claimedId) } });
    if (u) {
      return {
        id: String(u.id),
        username: String((u as any).username ?? claimedUsername ?? ""),
        role: String((u as any).role) as UserRole,
      } as JwtUser;
    }
  }

  // 2) fallback tìm theo username (phòng token sub/id sai)
  if (claimedUsername) {
    const u = await prisma.user.findFirst({
      where: { username: String(claimedUsername) },
    });
    if (u) {
      return {
        id: String(u.id),
        username: String((u as any).username),
        role: String((u as any).role) as UserRole,
      } as JwtUser;
    }
  }

  return null;
}

export async function requireAuth(req: Request, res: Response, next: NextFunction) {
  if (req.method === "OPTIONS") return next();

  const token = parseToken(req);
  if (!token) {
    return res.status(401).json({ code: "UNAUTHORIZED", message: "Unauthorized" });
  }

  try {
    const payload = jwt.verify(token, config.jwtSecret as Secret, {
      algorithms: ["HS256"],
      clockTolerance: 5,
    }) as JwtPayload & Partial<JwtUser>;

    const user = await hydrateUserFromDb(payload);

    if (!user?.id || !user?.username || !user?.role) {
      return res.status(401).json({
        code: "INVALID_USER",
        message: "User not found / token payload invalid",
      });
    }

    (req as any).user = user;
    next();
  } catch (_err: any) {
    return res.status(401).json({ code: "INVALID_TOKEN", message: "Invalid token" });
  }
}

export function requireRole(...roles: UserRole[]) {
  return (req: Request, res: Response, next: NextFunction) => {
    const user = (req as any).user as JwtUser | undefined;
    if (!user) {
      return res.status(401).json({ code: "UNAUTHORIZED", message: "Unauthorized" });
    }
    if (!roles.includes(user.role)) {
      return res.status(403).json({ code: "FORBIDDEN", message: "Forbidden" });
    }
    next();
  };
}

export function requireAnyRole(roles: UserRole[]) {
  return requireRole(...roles);
}

export function getUser(req: Request) {
  return (req as any).user as JwtUser | undefined;
}

export function meHandler(req: Request, res: Response) {
  const u = getUser(req);
  if (!u) {
    return res.status(401).json({ code: "UNAUTHORIZED", message: "Unauthorized" });
  }
  res.json(u);
}
