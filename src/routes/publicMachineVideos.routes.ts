// src/routes/publicMachineVideos.routes.ts
// ✅ Router CÔNG KHAI — KHÔNG gắn requireAuth. Dùng cho link chia sẻ video
// cho khách xem (không cần đăng nhập). Cố tình tách file riêng khỏi
// machineVideos.routes.ts để không lỡ tay áp middleware auth vào.
import { Router } from "express";
import { getPublicMachineVideoShare } from "../services/machineVideos.service";

const r = Router();

/** GET /api/public/machine-videos/:token -> { title, machineCode, note, mimeType, url } */
r.get("/:token", async (req, res, next) => {
  try {
    const data = await getPublicMachineVideoShare(req.params.token);
    res.json(data);
  } catch (err) {
    next(err);
  }
});

export default r;