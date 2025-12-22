import { Prisma } from "@prisma/client";
import { Request, Response, NextFunction } from "express";

export function errorHandler(err: any, req: Request, res: Response, next: NextFunction) {
  // Prisma known errors
  if (err instanceof Prisma.PrismaClientKnownRequestError) {
    // P1001: cannot reach database server
    if (err.code === "P1001") {
      return res.status(503).json({
        ok: false,
        message: "Không kết nối được database (P1001). Kiểm tra DATABASE_URL / Neon / mạng.",
        meta: err.meta,
      });
    }
  }

  // fallback
  const status = err?.statusCode || 500;
  res.status(status).json({
    ok: false,
    message: err?.message || "Internal Server Error",
  });
}
