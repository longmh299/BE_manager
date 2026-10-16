// src/routes/invoiceImport.routes.ts
// ✅ MỚI — Nhập hoá đơn từ file (chỉ THÊM route, không sửa route hoá đơn cũ)
//   POST /api/invoice-import/analyze   form-data: file, type=PURCHASE|SALES, partnerId?  -> dòng 🟢🟡🔵 (không lưu gì)
//   POST /api/invoice-import/confirm   json sau khi LƯU phiếu thành công -> nhật ký chống trùng + ghi nhớ lựa chọn
//   GET  /api/invoice-import/template?kind=ALL|MACHINE|PART -> file mẫu .xlsx có ID hệ thống
import { Router } from "express";
import multer from "multer";
import { requireAuth, getUser } from "../middlewares/auth";
import { analyzeImport, buildTemplate, confirmImport } from "../services/invoiceImport.service";

const r = Router();
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

r.use(requireAuth);

r.post("/analyze", upload.single("file"), async (req, res, next) => {
  try {
    const f = req.file;
    if (!f) return res.status(400).json({ message: "Chưa chọn file" });
    const filename = Buffer.from(f.originalname, "latin1").toString("utf8"); // multer đọc tên theo latin1
    const data = await analyzeImport({
      buffer: f.buffer,
      filename,
      type: String(req.body?.type || ""),
      partnerId: req.body?.partnerId ? String(req.body.partnerId) : undefined,
      actor: getUser(req) as any,
    });
    res.json({ ok: true, data });
  } catch (e: any) {
    if (e?.code === "LIMIT_FILE_SIZE") return res.status(413).json({ message: "File quá lớn (tối đa 10MB)" });
    if (e?.statusCode) return res.status(e.statusCode).json({ message: e.message });
    next(e);
  }
});

r.post("/confirm", async (req, res, next) => {
  try {
    const data = await confirmImport(req.body || {}, getUser(req) as any);
    res.json({ ok: true, data });
  } catch (e: any) {
    if (e?.statusCode) return res.status(e.statusCode).json({ message: e.message });
    next(e);
  }
});

r.get("/template", async (req, res, next) => {
  try {
    const buf = await buildTemplate(String(req.query.kind || "ALL"));
    const d = new Date();
    const name = `mau-nhap-hang-${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(d.getDate()).padStart(2, "0")}.xlsx`;
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="${name}"`);
    res.send(buf);
  } catch (e) {
    next(e);
  }
});

export default r;
