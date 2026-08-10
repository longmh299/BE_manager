// src/routes/machineImages.routes.ts
import { Router } from "express";
import { requireAuth, requireRole, getUser } from "../middlewares/auth";
import {
  listMachineImages,
  initMachineImageUpload,
  completeMachineImageUpload,
  getMachineImageDownloadUrl,
  getMachineImagePreviewUrl,
  updateMachineImageMeta,
  deleteMachineImage,
} from "../services/machineImages.service";

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

r.get("/", async (req, res, next) => {
  try {
    const { q, category, page, pageSize } = req.query as any;
    const data = await listMachineImages({
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

r.post("/init", async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const data = await initMachineImageUpload(req.body ?? {}, {
      userId: u.id,
      userRole: u.role,
      meta: buildAuditMeta(req),
    });
    res.status(201).json(data);
  } catch (err) {
    next(err);
  }
});

r.post("/:id/complete", async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const data = await completeMachineImageUpload(req.params.id, {
      userId: u.id,
      userRole: u.role,
      meta: buildAuditMeta(req),
    });
    res.json(data);
  } catch (err) {
    next(err);
  }
});

r.get("/:id/download-url", async (req, res, next) => {
  try {
    const data = await getMachineImageDownloadUrl(req.params.id);
    res.json(data);
  } catch (err) {
    next(err);
  }
});

r.get("/:id/preview-url", async (req, res, next) => {
  try {
    const data = await getMachineImagePreviewUrl(req.params.id);
    res.json(data);
  } catch (err) {
    next(err);
  }
});

r.put("/:id", async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const data = await updateMachineImageMeta(req.params.id, req.body ?? {}, {
      userId: u.id,
      userRole: u.role,
      meta: buildAuditMeta(req),
    });
    res.json(data);
  } catch (err) {
    next(err);
  }
});

r.delete("/:id", requireRole("admin"), async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const data = await deleteMachineImage(req.params.id, {
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