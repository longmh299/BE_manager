// src/routes/translate.routes.ts
// ✅ Dịch tài liệu (Trung / Anh -> Việt) + hỏi đáp AI về tài liệu + báo giá + LỊCH SỬ tài liệu đã dịch.
// Router này có body parser RIÊNG (giới hạn 32MB) và phải được gắn TRƯỚC
// app.use(express.json()) toàn cục (mặc định chỉ 100KB) — xem app.ts.
import express, { Router } from "express";
import { requireAuth, getUser } from "../middlewares/auth";
import { PrismaClient } from "@prisma/client";
import { draftQuote, buildQuoteDocx, quoteFileName } from "../services/quote.service";
import { createQuoteDocument } from "../services/Quotedocuments.service";
import { patchDocxContact } from "../lib/docxContactPatch";
import { translateDocument, translateBlocks, streamDocChat, Meter } from "../services/translate.service";
import { createDoc, markUploaded, listDocs, getDoc, updateDoc, sourceUrls, deleteDoc } from "../services/translateHistory.service";

const prisma = new PrismaClient();
const DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";

const r = Router();

r.use(express.json({ limit: "32mb" }));
r.use(requireAuth);

/**
 * POST /api/translate/document
 * Body: { kind: "pdf" | "image", data: base64, mediaType?, pageOffset?, context? }
 * -> { pages: [...], usage: { cost, models: [{ model, calls, input, output, thinking, cost }] } }
 */
r.post("/document", async (req, res, next) => {
  try {
    res.json(await translateDocument(req.body ?? {}));
  } catch (err) {
    next(err);
  }
});

/**
 * POST /api/translate/blocks
 * Body: { items: [{ text } | { rows: string[][] }], context?, mode?: "doc" | "cells" }
 * -> { items: [{ vi, untranslated? } | { viRows }] }  (cùng thứ tự)
 */
r.post("/blocks", async (req, res, next) => {
  try {
    res.json(await translateBlocks(req.body ?? {}));
  } catch (err) {
    next(err);
  }
});

/**
 * POST /api/translate/chat   (trả lời chạy dần từng chữ — text/plain stream)
 * Body: { doc: string, docName?, messages: [{ role: "user"|"assistant", content, image?: { data, mediaType } }] }
 */
r.post("/chat", async (req, res, next) => {
  const ctrl = new AbortController();
  res.on("close", () => {
    if (!res.writableFinished) ctrl.abort(); // người dùng đóng trang / bấm dừng -> ngừng gọi AI, đỡ tốn tiền
  });
  try {
    const meter = new Meter();
    const it = streamDocChat(req.body ?? {}, ctrl.signal, meter);
    const first = await it.next(); // lỗi xảy ra trước khi có chữ nào -> trả JSON lỗi bình thường
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("X-Accel-Buffering", "no");
    if (!first.done) res.write(first.value);
    for await (const chunk of it) res.write(chunk);
    // Cuối câu trả lời: gửi kèm chi phí thật, ngăn cách bằng ký tự \u001e (FE tách ra, không hiện cho người dùng)
    res.write("\u001e" + JSON.stringify(meter.summary()));
    res.end();
  } catch (err: any) {
    if (ctrl.signal.aborted) return;
    if (!res.headersSent) return next(err);
    res.write(`\n\n⚠️ ${err?.message || "Lỗi khi trả lời"}`);
    res.end();
  }
});

// ================================================================ BÁO GIÁ
/**
 * POST /api/translate/quote-draft
 * Body: { doc: string (bản dịch tiếng Việt), context?, quality? }
 * -> { draft: QuoteData, machine: { sku, name, code, sellPrice } | null, usage }
 */
r.post("/quote-draft", async (req, res, next) => {
  try {
    res.json(await draftQuote(req.body ?? {}));
  } catch (err) {
    next(err);
  }
});

/**
 * POST /api/translate/quote-docx   Body: QuoteData
 * -> file .docx theo mẫu công ty, Email / Mobile trên header là của người đang tải
 */
r.post("/quote-docx", async (req, res, next) => {
  try {
    const q = req.body ?? {};
    let buf = await buildQuoteDocx(q);
    const u = getUser(req);
    if (u?.id) {
      const me = await prisma.user.findUnique({ where: { id: u.id }, select: { email: true, phone: true } });
      if (me?.email || me?.phone) buf = (await patchDocxContact(buf, { email: me.email, phone: me.phone })).buffer;
    }
    res.setHeader("Content-Type", DOCX_MIME);
    res.setHeader("Content-Disposition", `attachment; filename*=UTF-8''${encodeURIComponent(quoteFileName(q))}`);
    res.send(buf);
  } catch (err) {
    next(err);
  }
});

/**
 * POST /api/translate/quote-save   Body: QuoteData
 * -> lưu vào "Kho báo giá" (QuoteDocument) -> { id, title }
 * File lưu giữ header gốc; khi tải từ Kho, hệ thống tự thay Email/Mobile theo người tải như mọi báo giá khác.
 */
r.post("/quote-save", async (req, res, next) => {
  try {
    const q = req.body ?? {};
    const buf = await buildQuoteDocx(q);
    const u = getUser(req)!;
    const created = await createQuoteDocument(
      {
        title: String(q.tenMay || "").trim(),
        machineCode: String(q.maHang || "").trim() || null,
        category: "Báo giá tạo từ tài liệu dịch",
        note: "Tạo tự động từ tab Dịch tài liệu",
        file: { originalname: quoteFileName(q), mimetype: DOCX_MIME, size: buf.length, buffer: buf },
      },
      { userId: u.id, userRole: u.role, meta: { ip: req.ip, path: req.originalUrl, method: req.method } } as any
    );
    res.status(201).json({ id: created.id, title: created.title });
  } catch (err) {
    next(err);
  }
});

// ================================================================ LỊCH SỬ TÀI LIỆU ĐÃ DỊCH
// Người dịch chỉ thấy tài liệu của mình, admin thấy hết.
const who = (req: express.Request) => {
  const u = getUser(req)!;
  return { id: u.id, role: u.role };
};

/** GET /api/translate/history?q=&cursor=  -> { items, nextCursor } */
r.get("/history", async (req, res, next) => {
  try {
    res.json(await listDocs(who(req), req.query));
  } catch (err) {
    next(err);
  }
});

/** POST /api/translate/history  (tự gọi khi dịch xong) -> { id, uploads: [{ index, url }] } */
r.post("/history", async (req, res, next) => {
  try {
    res.status(201).json(await createDoc(who(req), req.body ?? {}));
  } catch (err) {
    next(err);
  }
});

/** GET /api/translate/history/:id  -> tài liệu đầy đủ (bản dịch + nháp báo giá) */
r.get("/history/:id", async (req, res, next) => {
  try {
    res.json(await getDoc(who(req), req.params.id));
  } catch (err) {
    next(err);
  }
});

/** PATCH /api/translate/history/:id  Body: { pages?, sheets?, usage?, quoteDraft? } */
r.patch("/history/:id", async (req, res, next) => {
  try {
    res.json(await updateDoc(who(req), req.params.id, req.body ?? {}));
  } catch (err) {
    next(err);
  }
});

/** POST /api/translate/history/:id/uploaded  Body: { indexes: number[] } — FE báo đã PUT xong file gốc */
r.post("/history/:id/uploaded", async (req, res, next) => {
  try {
    res.json(await markUploaded(who(req), req.params.id, req.body?.indexes));
  } catch (err) {
    next(err);
  }
});

/** GET /api/translate/history/:id/sources  -> { files: [{ name, size, url }] } link tải file gốc */
r.get("/history/:id/sources", async (req, res, next) => {
  try {
    res.json(await sourceUrls(who(req), req.params.id));
  } catch (err) {
    next(err);
  }
});

/** DELETE /api/translate/history/:id  (xoá cả file gốc trên R2) */
r.delete("/history/:id", async (req, res, next) => {
  try {
    res.json(await deleteDoc(who(req), req.params.id));
  } catch (err) {
    next(err);
  }
});

export default r;