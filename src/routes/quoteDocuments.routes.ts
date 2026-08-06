// src/routes/quoteDocuments.routes.ts
import { Router, type Request, type Response, type NextFunction, type ErrorRequestHandler } from "express";
import multer from "multer";
import { requireAuth, requireRole, getUser } from "../middlewares/auth";
import {
  listQuoteDocuments,
  getQuoteDocumentMeta,
  getQuoteDocumentFileForDownload,
  createQuoteDocument,
  updateQuoteDocument,
  deleteQuoteDocument,
} from "../services/Quotedocuments.service";

const r = Router();

const ALLOWED_EXT = [".doc", ".docx", ".pdf"];
const ALLOWED_MIME = new Set([
  "application/msword",
  "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  "application/pdf",
]);

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 }, // 20MB / file — đủ cho file Word/PDF báo giá
  // ✅ chặn từ server, không chỉ dựa vào "accept" ở FE (dễ bị bypass)
  fileFilter: (_req, file, cb) => {
    const ext = "." + (file.originalname.split(".").pop() || "").toLowerCase();
    if (!ALLOWED_EXT.includes(ext) || !ALLOWED_MIME.has(file.mimetype)) {
      return cb(new Error("Chỉ chấp nhận file .doc, .docx hoặc .pdf"));
    }
    cb(null, true);
  },
});

/** Bắt lỗi từ multer (quá dung lượng, sai định dạng) và trả message tiếng Việt dễ hiểu */
const handleUploadError: ErrorRequestHandler = (err, _req, res, next) => {
  if (!err) return next();
  if (err.code === "LIMIT_FILE_SIZE") {
    return res.status(400).json({ ok: false, message: "File vượt quá 20MB, vui lòng nén hoặc rút gọn lại." });
  }
  if (err.message?.includes("Chỉ chấp nhận file")) {
    return res.status(400).json({ ok: false, message: err.message });
  }
  next(err);
};

function buildAuditMeta(req: any) {
  return {
    ip: req.ip,
    userAgent: req.headers?.["user-agent"],
    path: req.originalUrl || req.url,
    method: req.method,
  };
}

r.use(requireAuth);

/** GET /api/quote-documents?q=&category=&page=&pageSize= */
r.get("/", async (req, res, next) => {
  try {
    const { q, category, page, pageSize } = req.query as any;
    const data = await listQuoteDocuments({
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

/** GET /api/quote-documents/:id  (chỉ metadata, không có nội dung file) */
r.get("/:id", async (req, res, next) => {
  try {
    const data = await getQuoteDocumentMeta(req.params.id);
    res.json(data);
  } catch (err) {
    next(err);
  }
});

/** GET /api/quote-documents/:id/download  (stream file nhị phân, tự chèn email/SĐT người tải nếu file .docx và đã lưu hồ sơ) */
r.get("/:id/download", async (req, res, next) => {
  try {
    const u = getUser(req);
    const doc = await getQuoteDocumentFileForDownload(req.params.id, u?.id);
    res.setHeader("Content-Type", doc.mimeType || "application/octet-stream");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename*=UTF-8''${encodeURIComponent(doc.fileName)}`
    );
    res.send(doc.fileData);
  } catch (err) {
    next(err);
  }
});

/** POST /api/quote-documents  (multipart/form-data: file, title, machineCode?, category?, note?) */
r.post("/", upload.single("file"), handleUploadError, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const u = getUser(req)!;
    if (!req.file) {
      const err: any = new Error("Thiếu file upload");
      err.statusCode = 400;
      throw err;
    }
    const { title, machineCode, category, note } = req.body ?? {};

    // ✅ multer/busboy đọc originalname theo latin1 theo mặc định của chuẩn multipart,
    // nên tên file tiếng Việt (UTF-8) bị hiểu sai thành ký tự lộn xộn (mojibake).
    // Convert lại đúng UTF-8 trước khi lưu.
    const fixedOriginalName = Buffer.from(req.file.originalname, "latin1").toString("utf8");

    const data = await createQuoteDocument(
      {
        title,
        machineCode,
        category,
        note,
        file: {
          originalname: fixedOriginalName,
          mimetype: req.file.mimetype,
          size: req.file.size,
          buffer: req.file.buffer,
        },
      },
      { userId: u.id, userRole: u.role, meta: buildAuditMeta(req) }
    );
    res.status(201).json(data);
  } catch (err) {
    next(err);
  }
});

/**
 * PUT /api/quote-documents/:id
 * multipart/form-data: title?, machineCode?, category?, note?, file? (chỉ gửi "file" khi muốn THAY file, vd giá đổi -> báo giá mới)
 */
r.put("/:id", upload.single("file"), handleUploadError, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const u = getUser(req)!;
    const { title, machineCode, category, note } = req.body ?? {};

    const file = req.file
      ? {
          // ✅ fix mojibake tên file tiếng Việt, giống chỗ upload mới
          originalname: Buffer.from(req.file.originalname, "latin1").toString("utf8"),
          mimetype: req.file.mimetype,
          size: req.file.size,
          buffer: req.file.buffer,
        }
      : null;

    const data = await updateQuoteDocument(
      req.params.id,
      { title, machineCode, category, note, file },
      { userId: u.id, userRole: u.role, meta: buildAuditMeta(req) }
    );
    res.json(data);
  } catch (err) {
    next(err);
  }
});

/** DELETE /api/quote-documents/:id  (chỉ admin được xoá) */
r.delete("/:id", requireRole("admin"), async (req, res, next) => {
  try {
    const u = getUser(req)!;
    const data = await deleteQuoteDocument(req.params.id, {
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