// src/services/translate.service.ts
// ✅ Dịch tài liệu kỹ thuật (Trung / Anh -> Việt) + hỏi đáp AI về tài liệu đã dịch.
// KHÔNG lưu gì: nhận dữ liệu -> gọi AI -> trả kết quả về cho FE.
//
// MODEL & CHI PHÍ (giá chính thức, USD / 1 triệu token vào / ra):
//   Haiku 5.5  $0.10 / $0.50  (prompt ≤ 100K token; trên 100K giá x5)
//   Sonnet 5.5 $2    / $10
//   Opus 5.5   $4    / $20
// -> MẶC ĐỊNH DÙNG HAIKU 5.5 CHO TẤT CẢ (rẻ hơn Sonnet ~20 lần, Opus ~40 lần), đủ tốt cho dịch
//    tài liệu kỹ thuật và hỏi đáp dựa trên tài liệu.
//    Nếu bản scan mờ / chữ Hán viết tay đọc chưa chuẩn: đặt TRANSLATE_MODEL_VISION=claude-sonnet-5-5.
// -> Tắt bớt "suy nghĩ" của AI (effort = low): phần suy nghĩ bị tính tiền như chữ trả lời,
//    mà dịch / tra cứu tài liệu không cần suy nghĩ sâu.
//
// CHỐNG SÓT CHỮ:
//   1. Bắt AI trả kết quả đúng khuôn JSON (structured outputs) — không để AI tự viết JSON tuỳ ý
//   2. Sau khi dịch, tự quét đoạn nào bản dịch vẫn y nguyên chữ gốc -> gửi dịch lại 1 lần
//   3. Vẫn còn sót -> đánh dấu `untranslated: true` để FE hiện cảnh báo, không giấu đi

import { loadDocx, saveDocx, hasWords } from "../lib/docxTranslate";

const ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages";

// ---------------------------------------------------------------- ĐO CHI PHÍ THẬT
// Giá chính thức (USD / 1 triệu token). Haiku 5.5 có 2 bậc theo độ dài prompt (≤ / > 100K token).
const PRICE: Record<string, { in: number; out: number; cw: number; cr: number; big?: number }> = {
  "claude-haiku-5-5": { in: 0.1, out: 0.5, cw: 0.125, cr: 0.01, big: 5 },
  "claude-sonnet-5-5": { in: 2, out: 10, cw: 2.5, cr: 0.1 },
  "claude-opus-5-5": { in: 4, out: 20, cw: 5, cr: 0.2 },
};

export type Usage = { model: string; calls: number; input: number; output: number; thinking: number; cacheWrite: number; cacheRead: number; cost: number };

/** Cộng dồn token + tiền của mọi lần gọi AI trong 1 request (kể cả lần vá chỗ sót) */
export class Meter {
  byModel = new Map<string, Usage>();
  add(model: string, u: any, task: string) {
    if (!u) return;
    const input = Number(u.input_tokens) || 0;
    const output = Number(u.output_tokens) || 0;
    const cw = Number(u.cache_creation_input_tokens) || 0;
    const cr = Number(u.cache_read_input_tokens) || 0;
    const thinking = Number(u.output_tokens_details?.thinking_tokens) || 0;
    const p = PRICE[model] ?? PRICE["claude-sonnet-5-5"];
    const mult = p.big && input + cw + cr > 100_000 ? p.big : 1; // Haiku: prompt > 100K -> giá x5
    const cost = ((input * p.in + output * p.out + cw * p.cw + cr * p.cr) * mult) / 1e6;
    const cur = this.byModel.get(model) ?? { model, calls: 0, input: 0, output: 0, thinking: 0, cacheWrite: 0, cacheRead: 0, cost: 0 };
    Object.assign(cur, {
      calls: cur.calls + 1,
      input: cur.input + input,
      output: cur.output + output,
      thinking: cur.thinking + thinking,
      cacheWrite: cur.cacheWrite + cw,
      cacheRead: cur.cacheRead + cr,
      cost: cur.cost + cost,
    });
    this.byModel.set(model, cur);
    // Log lên Render để đối chiếu với trang Usage của Anthropic Console
    console.log(
      `[AI-COST] ${task} model=${model} in=${input} out=${output} (think=${thinking}) cacheW=${cw} cacheR=${cr} cost=$${cost.toFixed(5)}`
    );
  }
  summary() {
    const models = Array.from(this.byModel.values());
    return { cost: models.reduce((s, m) => s + m.cost, 0), models };
  }
}

export const MODELS = {
  vision: process.env.TRANSLATE_MODEL_VISION || "claude-haiku-5-5", // đọc PDF / scan / ảnh
  text: process.env.TRANSLATE_MODEL_TEXT || "claude-haiku-5-5", // dịch Word + vá đoạn sót
  cells: process.env.TRANSLATE_MODEL_CELLS || "claude-haiku-5-5", // dịch ô Excel
  chat: process.env.DOC_CHAT_MODEL || "claude-haiku-5-5", // hỏi đáp về tài liệu
  // Nút "Dịch chuyên sâu" trên trang: dùng cho tài liệu quan trọng / bản scan khó đọc (~20 lần giá Haiku)
  deep: process.env.TRANSLATE_MODEL_DEEP || "claude-sonnet-5-5",
};
export type Quality = "fast" | "deep";
const pick = (quality: Quality | undefined, normal: string) => (quality === "deep" ? MODELS.deep : normal);
const EFFORT = {
  translate: process.env.TRANSLATE_EFFORT || "low",
  chat: process.env.DOC_CHAT_EFFORT || "low",
};
/** Haiku 5.5 tính giá x5 khi prompt > 100K token -> giới hạn tài liệu gửi cho chat (~80K token) */
const CHAT_DOC_MAX_CHARS = Number(process.env.DOC_CHAT_MAX_CHARS || 200_000);

export type DocBlock = {
  type: "heading" | "paragraph" | "list" | "table" | "note";
  src?: string;
  vi?: string;
  srcRows?: string[][];
  viRows?: string[][];
  untranslated?: boolean;
};
export type DocPage = { page: number; blocks: DocBlock[] };

export function httpError(status: number, message: string, code?: string) {
  const err: any = new Error(message);
  err.statusCode = status;
  if (code) err.data = { code };
  return err;
}

function apiKey() {
  const k = process.env.ANTHROPIC_API_KEY;
  if (!k) throw httpError(500, "Thiếu ANTHROPIC_API_KEY trong biến môi trường");
  return k;
}

const HEADERS = () => ({
  "content-type": "application/json",
  "x-api-key": apiKey(),
  "anthropic-version": "2023-06-01",
});

const TRANSLATE_SYSTEM = `Bạn là biên dịch viên kỹ thuật chuyên ngành MÁY CHẾ BIẾN & ĐÓNG GÓI (thực phẩm, dược phẩm, mỹ phẩm) cho công ty MCBROTHER tại Việt Nam.
Dịch từ tiếng Trung hoặc tiếng Anh sang TIẾNG VIỆT.

QUY TẮC:
- PHẢI dịch TẤT CẢ chữ tiếng Trung / tiếng Anh sang tiếng Việt, kể cả tiêu đề mục, chú thích trên hình, nhãn ngắn 1 từ (vd "hook" = "móc", "Power outlet" = "ổ cắm điện"). Không được để nguyên tiếng Anh.
- Thuật ngữ quen thuộc với kỹ thuật viên Việt Nam (vd: 灌装机 = máy chiết rót, 封口机 = máy hàn miệng túi, sealing belt = dây đai hàn, toothed belt = dây đai răng, heating block = khối gia nhiệt, carbon brush = chổi than, fuse = cầu chì, 伺服电机 = động cơ servo, 变频器 = biến tần, 气缸 = xi lanh khí, 不锈钢304 = inox 304).
- GIỮ NGUYÊN: mã model, mã linh kiện, số liệu, đơn vị (mm, kW, V, Hz, MPa, pcs/min...), tên thương hiệu, ký hiệu nút bấm (ON/OFF, START, PLC, HMI).
- Đánh số mục giữ nguyên định dạng (7.2, ①, 1., VIII.).
- Cảnh báo an toàn (警告, 注意, WARNING, CAUTION, DANGER) dịch rõ: "CẢNH BÁO", "CHÚ Ý", "NGUY HIỂM".
- Dịch đầy đủ, chính xác, không tóm tắt, không thêm bình luận.
- Chữ không đọc được ghi "[không đọc được]".`;

// ---------------------------------------------------------------- khuôn dữ liệu bắt buộc (structured outputs)
// Sonnet 5.5 / Opus 5.5 KHÔNG hỗ trợ ép tool (tool_choice "tool"/"any" -> lỗi 400),
// nên dùng output_config.format = json_schema: API đảm bảo câu trả lời đúng khuôn JSON.
// Lưu ý khuôn: mọi object phải có additionalProperties: false.
const ROWS = { type: "array", items: { type: "array", items: { type: "string" } } };

const PAGES_SCHEMA = {
  type: "object",
  properties: {
    pages: {
      type: "array",
      items: {
        type: "object",
        properties: {
          page: { type: "integer", description: "Số thứ tự trang trong đoạn tài liệu này, bắt đầu từ 1" },
          blocks: {
            type: "array",
            items: {
              type: "object",
              properties: {
                type: { type: "string", enum: ["heading", "paragraph", "list", "table", "note"] },
                src: { type: "string", description: "Chữ gốc (không dùng cho table)" },
                vi: { type: "string", description: "Bản dịch tiếng Việt (không dùng cho table)" },
                srcRows: { ...ROWS, description: "Bảng gốc (chỉ cho table)" },
                viRows: { ...ROWS, description: "Bảng đã dịch, cùng số hàng/cột (chỉ cho table)" },
              },
              required: ["type"],
              additionalProperties: false,
            },
          },
        },
        required: ["page", "blocks"],
        additionalProperties: false,
      },
    },
  },
  required: ["pages"],
  additionalProperties: false,
};

const ITEMS_SCHEMA = {
  type: "object",
  properties: {
    items: {
      type: "array",
      items: {
        type: "object",
        properties: {
          i: { type: "integer" },
          vi: { type: "string", description: "Bản dịch (cho phần tử dạng text)" },
          rows: { ...ROWS, description: "Bảng đã dịch (cho phần tử dạng rows)" },
        },
        required: ["i"],
        additionalProperties: false,
      },
    },
  },
  required: ["items"],
  additionalProperties: false,
};

export async function callStructured(opts: {
  model: string;
  content: any[];
  schema: any;
  maxTokens: number;
  context?: string;
  meter?: Meter;
  task: string;
}) {
  const res = await fetch(ANTHROPIC_API_URL, {
    method: "POST",
    headers: HEADERS(),
    body: JSON.stringify({
      model: opts.model,
      max_tokens: opts.maxTokens,
      system:
        TRANSLATE_SYSTEM +
        (opts.context?.trim() ? `\n\nNGỮ CẢNH TÀI LIỆU (người dùng cung cấp): ${opts.context.trim().slice(0, 300)}` : ""),
      output_config: { format: { type: "json_schema", schema: opts.schema }, effort: EFFORT.translate },
      messages: [{ role: "user", content: opts.content }],
    }),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    if (res.status === 413 || /too large|exceeds|too long/i.test(text)) {
      throw httpError(413, "Phần tài liệu này quá lớn để dịch một lần.", "TOO_LARGE");
    }
    if (res.status === 429 || res.status === 529) {
      throw httpError(503, "Dịch vụ AI đang quá tải, bấm Dịch lại sau ít giây.", "BUSY");
    }
    throw httpError(502, `Lỗi dịch vụ AI (${res.status}): ${text.slice(0, 300)}`);
  }

  const data: any = await res.json();
  opts.meter?.add(opts.model, data.usage, opts.task);
  if (data.stop_reason === "max_tokens") {
    throw httpError(422, "Nội dung quá dài cho một lần dịch, cần chia nhỏ hơn.", "TOO_LONG");
  }
  if (data.stop_reason === "refusal") throw httpError(502, "AI từ chối xử lý phần này.", "REFUSAL");
  // Có thể có khối "thinking" trước — lấy khối text chứa JSON
  const text = (data.content || []).filter((c: any) => c.type === "text").map((c: any) => c.text || "").join("");
  try {
    return JSON.parse(text);
  } catch {
    throw httpError(502, "AI trả về sai định dạng, bấm Dịch lại.", "BAD_FORMAT");
  }
}

// ---------------------------------------------------------------- phát hiện chữ chưa dịch
/** Có chữ cần dịch: chữ Hán, hoặc từ tiếng Anh (bỏ qua mã in hoa như PLC, JL-1000, ON/OFF) */
export function needsTranslate(s?: string) {
  const t = String(s ?? "").trim();
  if (!t) return false;
  if (/[㐀-鿿]/.test(t)) return true;
  const words = t.match(/[A-Za-z]{3,}/g) || [];
  return words.some((w) => w !== w.toUpperCase());
}
const same = (a?: string, b?: string) =>
  String(a ?? "").replace(/\s+/g, " ").trim().toLowerCase() === String(b ?? "").replace(/\s+/g, " ").trim().toLowerCase();
const isMissed = (src?: string, vi?: string) => needsTranslate(src) && (!String(vi ?? "").trim() || same(src, vi));
/** Sau khi đã dịch lại mà vẫn y nguyên: chỉ cảnh báo nếu chắc chắn là câu (có chữ Hán hoặc ≥ 2 từ) */
const stillMissed = (src?: string, vi?: string) =>
  isMissed(src, vi) &&
  (/[㐀-鿿]/.test(String(src)) || (String(src).match(/[A-Za-z]{2,}/g) || []).length >= 2);

const asRows = (v: any): string[][] | undefined =>
  Array.isArray(v) ? v.map((r: any) => (Array.isArray(r) ? r.map((c: any) => String(c ?? "")) : [String(r ?? "")])) : undefined;

/** Dịch danh sách câu ngắn (dùng để vá chỗ sót) — trả Map gốc -> dịch */
async function translateTexts(texts: string[], model: string, context?: string, meter?: Meter) {
  const out = new Map<string, string>();
  const uniq = Array.from(new Set(texts.map((t) => t.trim()).filter(Boolean)));
  for (let k = 0; k < uniq.length; k += 80) {
    const batch = uniq.slice(k, k + 80);
    const input = await callStructured({
      model,
      meter,
      task: "va-cho-sot",
      schema: ITEMS_SCHEMA,
      maxTokens: 8000,
      context,
      content: [
        {
          type: "text",
          text: `Dịch sang tiếng Việt TỪNG phần tử (kể cả nhãn 1 từ). Giữ đúng chỉ số i, đủ ${batch.length} phần tử.\n${JSON.stringify(
            batch.map((text, i) => ({ i, text }))
          )}`,
        },
      ],
    });
    for (const r of Array.isArray(input?.items) ? input.items : []) {
      const src = batch[Number(r?.i)];
      const vi = r?.vi ?? r?.text;
      if (src != null && vi != null) out.set(src, String(vi));
    }
  }
  return out;
}

/** Quét các khối của trang, đoạn nào còn chữ gốc -> dịch lại 1 lần -> còn sót thì đánh dấu */
async function fixMissed(blocks: DocBlock[], context?: string, meter?: Meter, model = MODELS.text) {
  const todo: string[] = [];
  for (const b of blocks) {
    if (b.type === "table") {
      (b.srcRows || []).forEach((r, i) =>
        r.forEach((c, j) => {
          if (isMissed(c, b.viRows?.[i]?.[j])) todo.push(c);
        })
      );
    } else if (isMissed(b.src, b.vi)) todo.push(b.src!);
  }
  if (!todo.length) return;

  let dict = new Map<string, string>();
  try {
    dict = await translateTexts(todo, model, context, meter);
  } catch {
    /* vá không được thì vẫn trả bản đang có, kèm cảnh báo bên dưới */
  }
  const get = (s?: string) => dict.get(String(s ?? "").trim());

  for (const b of blocks) {
    if (b.type === "table") {
      const src = b.srcRows || [];
      b.viRows = src.map((r, i) =>
        r.map((c, j) => {
          const cur = b.viRows?.[i]?.[j] ?? "";
          if (!isMissed(c, cur)) return cur;
          const fixed = get(c) ?? cur ?? c;
          if (stillMissed(c, fixed)) b.untranslated = true;
          return fixed || c;
        })
      );
    } else if (isMissed(b.src, b.vi)) {
      b.vi = get(b.src) ?? b.vi ?? b.src;
      if (stillMissed(b.src, b.vi)) b.untranslated = true;
    }
  }
}

function cleanBlock(b: any): DocBlock | null {
  if (!b || typeof b !== "object") return null;
  const type = ["heading", "paragraph", "list", "table", "note"].includes(b.type) ? b.type : "paragraph";
  if (type === "table") {
    const srcRows = asRows(b.srcRows);
    const viRows = asRows(b.viRows);
    if (!srcRows?.length && !viRows?.length) return null;
    return { type, srcRows: srcRows ?? viRows, viRows };
  }
  const src = String(b.src ?? "").trim();
  const vi = String(b.vi ?? "").trim();
  if (!src && !vi) return null;
  return { type, src: src || vi, vi };
}

// ---------------------------------------------------------------- PDF / ảnh
export async function translateDocument(input: {
  kind: "pdf" | "image";
  data: string; // base64
  mediaType?: string;
  pageOffset?: number;
  context?: string;
  quality?: Quality;
}, meter = new Meter()): Promise<{ pages: DocPage[]; usage: ReturnType<Meter["summary"]> }> {
  if (!input?.data) throw httpError(400, "Thiếu dữ liệu file");
  if (input.data.length > 30 * 1024 * 1024) throw httpError(413, "Phần tài liệu quá lớn", "TOO_LARGE");

  const offset = Math.max(0, Number(input.pageOffset) || 0);
  const isPdf = input.kind === "pdf";
  const mediaType = isPdf
    ? "application/pdf"
    : ["image/jpeg", "image/png", "image/webp", "image/gif"].includes(String(input.mediaType))
      ? input.mediaType
      : "image/jpeg";

  const fileBlock = isPdf
    ? { type: "document", source: { type: "base64", media_type: mediaType, data: input.data } }
    : { type: "image", source: { type: "base64", media_type: mediaType, data: input.data } };

  const instruction = `${isPdf ? "Đây là một đoạn PDF (có thể là bản scan)." : "Đây là ảnh chụp tài liệu / tem máy / màn hình."}
Đọc TOÀN BỘ chữ trên ${isPdf ? "từng trang" : "ảnh"} theo đúng thứ tự đọc từ trên xuống, trái sang phải (bỏ qua số trang, header/footer lặp lại), chia thành các khối và dịch HẾT sang tiếng Việt — kể cả chú thích nằm trên hình minh hoạ.
- heading: tiêu đề mục; paragraph: đoạn văn; list: danh sách (mỗi mục 1 dòng); note: cảnh báo/chú ý; table: bảng (srcRows/viRows cùng số hàng, số cột).
- "page" đánh số từ 1 ${isPdf ? "theo thứ tự trang trong đoạn PDF này" : "(ảnh = trang 1)"}. Trang chỉ có hình, không có chữ -> blocks rỗng.
Trả kết quả đúng khuôn JSON đã cho.`;

  const visionModel = pick(input.quality, MODELS.vision);
  const out = await callStructured({
    model: visionModel,
    meter,
    task: isPdf ? "dich-pdf" : "dich-anh",
    schema: PAGES_SCHEMA,
    maxTokens: 16000,
    context: input.context,
    content: [fileBlock, { type: "text", text: instruction }],
  });

  const pages: DocPage[] = (Array.isArray(out?.pages) ? out.pages : []).map((p: any, i: number) => ({
    page: offset + (Number(p?.page) || i + 1),
    blocks: (Array.isArray(p?.blocks) ? p.blocks : []).map(cleanBlock).filter(Boolean) as DocBlock[],
  }));

  await fixMissed(pages.flatMap((p) => p.blocks), input.context, meter, pick(input.quality, MODELS.text));
  return { pages, usage: meter.summary() };
}

// ---------------------------------------------------------------- Word / Excel
/**
 * Dịch danh sách đoạn văn bản đã tách sẵn, giữ nguyên thứ tự.
 * mode "doc" (Word) / "cells" (ô Excel) -> model theo MODELS; bật "Dịch chuyên sâu" -> MODELS.deep.
 */
export async function translateBlocks(input: {
  items: Array<{ text?: string; rows?: string[][] }>;
  context?: string;
  mode?: "doc" | "cells";
  quality?: Quality;
}, meter = new Meter()): Promise<{
  items: Array<{ vi?: string; viRows?: string[][]; untranslated?: boolean }>;
  usage: ReturnType<Meter["summary"]>;
}> {
  const items = Array.isArray(input?.items) ? input.items : [];
  if (!items.length) return { items: [], usage: meter.summary() };
  if (JSON.stringify(items).length > 40000) throw httpError(413, "Gửi quá nhiều chữ một lần", "TOO_LARGE");

  const model = pick(input.quality, input.mode === "cells" ? MODELS.cells : MODELS.text);
  const payload = items.map((it, i) => (it.rows ? { i, rows: asRows(it.rows) } : { i, text: String(it.text ?? "") }));

  const out = await callStructured({
    model,
    meter,
    task: input.mode === "cells" ? "dich-excel" : "dich-word",
    schema: ITEMS_SCHEMA,
    maxTokens: 16000,
    context: input.context,
    content: [
      {
        type: "text",
        text: `Dịch sang tiếng Việt TỪNG phần tử dưới đây, giữ đúng chỉ số "i", đủ ${payload.length} phần tử.
- Phần tử có "text" -> trả {"i", "vi"}.  Phần tử có "rows" (bảng) -> trả {"i", "rows"} cùng số hàng, số cột.
- Ô chỉ có số / mã / ký hiệu in hoa -> giữ nguyên.
${JSON.stringify(payload)}`,
      },
    ],
  });

  const byIndex = new Map<number, any>();
  for (const r of Array.isArray(out?.items) ? out.items : []) byIndex.set(Number(r?.i), r);

  // Ghép kết quả theo dạng khối để dùng chung bộ vá chỗ sót
  const blocks: DocBlock[] = payload.map((p: any) => {
    const r = byIndex.get(p.i);
    if (p.rows) return { type: "table", srcRows: p.rows, viRows: asRows(r?.rows) };
    return { type: "paragraph", src: p.text, vi: r?.vi ?? r?.text };
  });
  await fixMissed(blocks, input.context, meter, pick(input.quality, MODELS.text));

  return {
    items: blocks.map((b) =>
      b.type === "table"
        ? { viRows: b.viRows ?? b.srcRows, untranslated: b.untranslated }
        : { vi: b.vi ?? b.src, untranslated: b.untranslated }
    ),
    usage: meter.summary(),
  };
}

// ---------------------------------------------------------------- Word -> file Word tiếng Việt (giữ bố cục)
const DOCX_BATCH_CHARS = Number(process.env.DOCX_BATCH_CHARS || 5000);
const DOCX_BATCH_ITEMS = 60;
const DOCX_CONCURRENCY = Number(process.env.DOCX_CONCURRENCY || 4);
const DOCX_MAX_CHARS = Number(process.env.DOCX_MAX_CHARS || 600_000); // ~250-300 trang

/**
 * Nhận file .docx (base64) -> trả file .docx tiếng Việt: CHỈ thay chữ, giữ nguyên hình ảnh,
 * đánh số, bảng, text box, header/footer, định dạng. Xem src/lib/docxTranslate.ts.
 */
export async function translateDocx(
  input: { data: string; context?: string; quality?: Quality },
  meter = new Meter()
): Promise<{ buffer: Buffer; stats: { segments: number; translated: number; untranslated: number }; usage: ReturnType<Meter["summary"]> }> {
  if (!input?.data) throw httpError(400, "Thiếu dữ liệu file");
  if (input.data.length > 30 * 1024 * 1024) throw httpError(413, "File quá lớn (tối đa ~22MB)", "TOO_LARGE");

  const doc = await loadDocx(Buffer.from(input.data, "base64"));

  const texts = Array.from(new Set(doc.segments.map((s) => s.text.trim()).filter(hasWords)));
  const total = texts.reduce((n, t) => n + t.length, 0);
  if (total > DOCX_MAX_CHARS) {
    throw httpError(413, `Tài liệu quá dài (${total.toLocaleString("vi-VN")} ký tự), hãy tách file nhỏ hơn.`, "TOO_LARGE");
  }

  // Gom lô theo số ký tự / số câu
  const batches: string[][] = [];
  let cur: string[] = [];
  let len = 0;
  for (const t of texts) {
    if (cur.length && (len + t.length > DOCX_BATCH_CHARS || cur.length >= DOCX_BATCH_ITEMS)) {
      batches.push(cur);
      cur = [];
      len = 0;
    }
    cur.push(t);
    len += t.length;
  }
  if (cur.length) batches.push(cur);

  const dict = new Map<string, string>();
  let untranslated = 0;
  const run = async (batch: string[]): Promise<void> => {
    try {
      const out = await translateBlocks({ items: batch.map((text) => ({ text })), context: input.context, mode: "doc", quality: input.quality }, meter);
      out.items.forEach((it, i) => {
        if (it.vi != null) dict.set(batch[i], it.vi);
        if (it.untranslated) untranslated++;
      });
    } catch (e: any) {
      // Lô quá dài cho 1 lần trả lời -> chia đôi rồi dịch lại
      if ((e?.data?.code === "TOO_LONG" || e?.data?.code === "TOO_LARGE") && batch.length > 1) {
        const mid = Math.ceil(batch.length / 2);
        await run(batch.slice(0, mid));
        await run(batch.slice(mid));
        return;
      }
      throw e;
    }
  };

  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(DOCX_CONCURRENCY, batches.length) }, async () => {
      while (next < batches.length) await run(batches[next++]);
    })
  );

  const buffer = await saveDocx(doc, (src) => dict.get(src));
  return { buffer, stats: { segments: texts.length, translated: dict.size, untranslated }, usage: meter.summary() };
}

// ---------------------------------------------------------------- Hỏi đáp AI về tài liệu (stream)
const CHAT_SYSTEM = `Bạn là kỹ sư hỗ trợ kỹ thuật của MCBROTHER (máy chế biến & đóng gói thực phẩm, dược phẩm, mỹ phẩm).
Kỹ thuật viên đang đọc TÀI LIỆU bên dưới (đã dịch sang tiếng Việt) và hỏi bạn qua điện thoại.

CÁCH TRẢ LỜI:
- Trả lời tiếng Việt, ngắn gọn, đi thẳng vào việc; dùng gạch đầu dòng / các bước đánh số khi hướng dẫn thao tác.
- Dựa trên tài liệu là chính. Ghi rõ nguồn dạng "(Trang 12)" ngay sau ý lấy từ tài liệu.
- Nếu tài liệu KHÔNG có thông tin: nói rõ "Tài liệu không đề cập", rồi mới gợi ý theo kinh nghiệm chung và ghi rõ là "kinh nghiệm chung, cần kiểm tra lại với máy thực tế".
- Thông số (nhiệt độ, áp suất, mô-men, khe hở...) phải lấy đúng từ tài liệu, không tự bịa số.
- Thao tác sửa chữa / thay thế: luôn nhắc ngắt điện, xả khí nén, chờ khối gia nhiệt nguội trước khi làm.
- Nếu người dùng gửi ảnh (màn hình lỗi, linh kiện...), đối chiếu với tài liệu để chẩn đoán.`;

type ChatMsg = { role: "user" | "assistant"; content: string; image?: { data: string; mediaType?: string } };

/** Gọi AI dạng stream, trả về async iterator các đoạn chữ */
export async function* streamDocChat(
  input: { doc: string; docName?: string; messages: ChatMsg[]; quality?: Quality },
  signal?: AbortSignal,
  meter = new Meter()
): AsyncGenerator<string> {
  const doc = String(input?.doc ?? "").slice(0, CHAT_DOC_MAX_CHARS);
  if (!doc.trim()) throw httpError(400, "Chưa có nội dung tài liệu để hỏi");
  const msgs = (Array.isArray(input?.messages) ? input.messages : []).slice(-20);
  if (!msgs.length || msgs[msgs.length - 1].role !== "user") throw httpError(400, "Thiếu câu hỏi");

  const messages = msgs.map((m) => {
    if (m.role === "user" && m.image?.data) {
      const mt = ["image/jpeg", "image/png", "image/webp"].includes(String(m.image.mediaType)) ? m.image.mediaType : "image/jpeg";
      return {
        role: "user",
        content: [
          { type: "image", source: { type: "base64", media_type: mt, data: m.image.data } },
          { type: "text", text: m.content || "Ảnh này là gì, liên quan gì tới tài liệu?" },
        ],
      };
    }
    return { role: m.role, content: String(m.content || "") };
  });

  const res = await fetch(ANTHROPIC_API_URL, {
    method: "POST",
    headers: HEADERS(),
    signal,
    body: JSON.stringify({
      model: pick(input.quality, MODELS.chat),
      max_tokens: 4000,
      stream: true,
      output_config: { effort: EFFORT.chat },
      system: [
        { type: "text", text: CHAT_SYSTEM },
        // Tài liệu được cache: câu hỏi thứ 2 trở đi trong vòng 5 phút chỉ tính ~10% giá phần tài liệu
        {
          type: "text",
          text: `TÀI LIỆU: ${String(input.docName || "").slice(0, 200)}\n\n${doc}`,
          cache_control: { type: "ephemeral" },
        },
      ],
      messages,
    }),
  });

  if (!res.ok || !res.body) {
    const text = await res.text().catch(() => "");
    if (res.status === 429 || res.status === 529) throw httpError(503, "Dịch vụ AI đang quá tải, hỏi lại sau ít giây.", "BUSY");
    throw httpError(502, `Lỗi dịch vụ AI (${res.status}): ${text.slice(0, 300)}`);
  }

  let usage: any = null;
  const reader = (res.body as any).getReader();
  const decoder = new TextDecoder();
  let buf = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    let nl: number;
    while ((nl = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, nl).trim();
      buf = buf.slice(nl + 1);
      if (!line.startsWith("data:")) continue;
      try {
        const ev = JSON.parse(line.slice(5));
        if (ev.type === "content_block_delta" && ev.delta?.type === "text_delta") yield ev.delta.text as string;
        // Token đầu vào báo ở message_start, token đầu ra báo ở message_delta (cuối stream)
        if (ev.type === "message_start") usage = { ...(ev.message?.usage || {}) };
        if (ev.type === "message_delta" && ev.usage) usage = { ...usage, ...ev.usage };
        if (ev.type === "error") throw httpError(502, ev.error?.message || "Lỗi dịch vụ AI");
      } catch (e: any) {
        if (e?.statusCode) throw e;
      }
    }
  }
  meter.add(pick(input.quality, MODELS.chat), usage, "hoi-ai");
}