// src/routes/machineVideos.routes.ts
import { Router } from "express";
import { requireAuth, requireRole, getUser } from "../middlewares/auth";
import {
  listMachineVideos,
  initMachineVideoUpload,
  completeMachineVideoUpload,
  getMachineVideoDownloadUrl,
  getMachineVideoPreviewUrl,
  updateMachineVideoMeta,
  deleteMachineVideo,
} from "../services/machineVideos.service";

const r = Router();

function buildAuditMeta(req: any) {
  return {
    ip: req.ip,
    userAgent: req.headers?.["user-agent"],
    path: req.originalUrl || req.url,
    method: req.method,
  };
}

r.use(requireAuth);

/** GET /api/machine-videos?q=&category=&page=&pageSize= */
r.get("/", async (req, res, next) => {
  try {
    const { q, category, page, pageSize } = req.query as any;
    const data = await listMachineVideos({
      q,
      category,
      page: page ? Number(page) : undefined,
      pageSize: pageSize ? Number(pageSize) : undefined,
    });
    res.json(data);
  } catch (err) {
    next(err);
  }
});

/**
 * POST /api/machine-videos/init
 * Body JSON: { title, machineCode?, category?, note?, fileName, mimeType, fileSize }
 * -> trả { id, uploadUrl } để FE PUT file thẳng lên R2 (không qua backend)
 */
r.post("/init", async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const data = await initMachineVideoUpload(req.body ?? {}, {
      userId: u.id,
      userRole: u.role,
      meta: buildAuditMeta(req),
    });
    res.status(201).json(data);
  } catch (err) {
    next(err);
  }
});

/** POST /api/machine-videos/:id/complete  (FE gọi sau khi PUT lên R2 thành công) */
r.post("/:id/complete", async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const data = await completeMachineVideoUpload(req.params.id, {
      userId: u.id,
      userRole: u.role,
      meta: buildAuditMeta(req),
    });
    res.json(data);
  } catch (err) {
    next(err);
  }
});

/** GET /api/machine-videos/:id/download-url  -> { url, fileName } (URL có hạn 10 phút) */
r.get("/:id/download-url", async (req, res, next) => {
  try {
    const data = await getMachineVideoDownloadUrl(req.params.id);
    res.json(data);
  } catch (err) {
    next(err);
  }
});

/** GET /api/machine-videos/:id/preview-url  -> { url, mimeType } (xem trực tiếp trong trang, không ép tải xuống) */
r.get("/:id/preview-url", async (req, res, next) => {
  try {
    const data = await getMachineVideoPreviewUrl(req.params.id);
    res.json(data);
  } catch (err) {
    next(err);
  }
});

/** PUT /api/machine-videos/:id  (sửa tên/mã máy/ghi chú, không đổi file) */
r.put("/:id", async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const data = await updateMachineVideoMeta(req.params.id, req.body ?? {}, {
      userId: u.id,
      userRole: u.role,
      meta: buildAuditMeta(req),
    });
    res.json(data);
  } catch (err) {
    next(err);
  }
});

/** DELETE /api/machine-videos/:id  (chỉ admin — xoá cả file thật trên R2) */
r.delete("/:id", requireRole("admin"), async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const data = await deleteMachineVideo(req.params.id, {
      userId: u.id,
      userRole: u.role,
      meta: buildAuditMeta(req),
    });
    res.json(data);
  } catch (err) {
    next(err);
  }
});

export default r;