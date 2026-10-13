// src/services/quote.service.ts
// ✅ Tạo BÁO GIÁ từ tài liệu vừa dịch, đúng MẪU BÁO GIÁ CỦA MCBROTHER.
//
// 1. draftQuote: AI đọc bản dịch tiếng Việt -> trích tên máy, quy trình vận hành, đặc điểm,
//    thông số kỹ thuật (chỉ lấy từ tài liệu, không bịa). Giá lấy từ CRM (Item.sellPrice) nếu
//    khớp được mã máy — AI KHÔNG BAO GIỜ tự đặt giá.
// 2. buildQuoteDocx: điền nội dung vào chính file mẫu assets/quote-template.docx
//    -> giữ nguyên header công ty, logo, font, kiểu bảng; tự đổi ngày; chèn ảnh máy
//    (ảnh người dùng chọn, hoặc tự lấy từ "Kho ảnh máy" theo mã máy).
// 3. Tải về: thay Email / Mobile trên header bằng của người đang tải (dùng lại patchDocxContact
//    giống Kho báo giá). Lưu vào Kho báo giá: dùng lại createQuoteDocument.
import fs from "fs";
import path from "path";
import JSZip from "jszip";
import { PrismaClient } from "@prisma/client";
import { GetObjectCommand } from "@aws-sdk/client-s3";
import { r2, R2_BUCKET_NAME } from "../lib/r2Client";
import { resolveMachine } from "../utils/machineMatch";
import { callStructured, httpError, Meter, MODELS, type Quality } from "./translate.service";

const prisma = new PrismaClient();
const TEMPLATE_PATH = path.join(process.cwd(), "assets", "quote-template.docx");

export type QuoteSpec = { ten: string; giaTri: string };
export type QuoteData = {
  tenMay: string;
  maHang: string;
  quyTrinh: string; // các bước nối bằng " ---> "
  dacDiem: string[];
  thongSo: QuoteSpec[];
  donGia: string; // "365000000" | "365,000,000" | "Liên hệ"
  dieuKhoan: string[]; // các dòng ghi chú dưới bảng
  thanhToan: string[]; // các dòng điều khoản thanh toán
  image?: { data: string; mediaType?: string } | null; // base64
};

export const DEFAULT_TERMS = {
  dieuKhoan: [
    "Đơn giá chưa bao gồm thuế VAT",
    "Giao hàng tại TP.HCM",
    "Giao hàng sau 40-45 ngày kể từ khi nhận đặt cọc lần đầu.",
  ],
  thanhToan: [
    "Đặt cọc 40% sau khi ký hợp đồng.",
    "Thanh toán tiếp 40% khi hàng về đến cảng và bên bán thông báo giao hàng.",
    "Thanh toán 20% sau khi lắp đặt, ký nghiệm thu.",
  ],
};

// ================================================================ 1. AI soạn nội dung
const DRAFT_SCHEMA = {
  type: "object",
  properties: {
    tenMay: { type: "string", description: "Tên máy tiếng Việt, VIẾT HOA, vd MÁY GẤP MẶT NẠ CHO VÀO TÚI" },
    maHang: { type: "string", description: "Mã model trong tài liệu, vd SSDZD-66; không có thì để rỗng" },
    quyTrinh: { type: "string", description: "Quy trình vận hành: các bước ngắn nối bằng ' ---> '" },
    dacDiem: { type: "array", items: { type: "string" }, description: "4-6 đặc điểm nổi bật, mỗi ý 1 câu" },
    thongSo: {
      type: "array",
      items: {
        type: "object",
        properties: { ten: { type: "string" }, giaTri: { type: "string" } },
        required: ["ten", "giaTri"],
        additionalProperties: false,
      },
      description: "6-10 thông số kỹ thuật quan trọng nhất",
    },
    khongBaoGom: { type: "string", description: "Thiết bị đi kèm cần có nhưng không thuộc máy (vd máy nén khí); không có thì rỗng" },
  },
  required: ["tenMay", "maHang", "quyTrinh", "dacDiem", "thongSo", "khongBaoGom"],
  additionalProperties: false,
};

export async function draftQuote(
  input: { doc: string; context?: string; quality?: Quality },
  meter = new Meter()
) {
  const doc = String(input?.doc ?? "").slice(0, 150_000);
  if (!doc.trim()) throw httpError(400, "Chưa có nội dung tài liệu");

  const draft = await callStructured({
    model: input.quality === "deep" ? MODELS.deep : MODELS.text,
    meter,
    task: "soan-bao-gia",
    schema: DRAFT_SCHEMA,
    maxTokens: 4000,
    context: input.context,
    content: [
      {
        type: "text",
        text: `Từ TÀI LIỆU KỸ THUẬT (đã dịch) bên dưới, soạn nội dung cho BÁO GIÁ MÁY của MCBROTHER gửi khách hàng Việt Nam.
- tenMay: tên máy tiếng Việt VIẾT HOA, ngắn gọn, mô tả đúng công dụng.
- quyTrinh: tóm quy trình hoạt động thành 3-7 bước ngắn nối bằng " ---> " (vd "Cấp liệu ---> Định lượng ---> Chiết rót ---> Đóng nắp ---> Ra thành phẩm").
- dacDiem: 4-6 ưu điểm nổi bật cho khách (tốc độ, độ chính xác, vật liệu, dễ vệ sinh, động cơ, điều khiển...), văn phong bán hàng chuyên nghiệp, đúng sự thật trong tài liệu.
- thongSo: 6-10 thông số quan trọng nhất, tên thông số tiếng Việt chuẩn (Sản lượng / Năng suất, Nguồn điện, Công suất, Áp suất khí, Trọng lượng máy, Kích thước máy...). GIỮ NGUYÊN số và đơn vị như tài liệu. KHÔNG ghi mã hàng và KHÔNG ghi giá trong thongSo.
- TUYỆT ĐỐI không bịa số liệu; thông tin nào tài liệu không có thì bỏ qua. Không đề cập giá.

TÀI LIỆU:
${doc}`,
      },
    ],
  });

  // Tra máy trong CRM để lấy mã chuẩn + giá bán (nếu có)
  let machine: { sku: string; name: string; code: string; sellPrice: number | null } | null = null;
  const tries = [draft?.maHang, input.context, draft?.tenMay].filter(Boolean) as string[];
  for (const q of tries) {
    const { match } = await resolveMachine(q);
    if (match) {
      const item = await prisma.item.findUnique({ where: { id: match.id }, select: { sellPrice: true } });
      const price = item?.sellPrice != null ? Number(item.sellPrice) : null;
      machine = { sku: match.sku, name: match.name, code: match.code, sellPrice: price && price > 0 ? price : null };
      break;
    }
  }

  const dieuKhoan = [...DEFAULT_TERMS.dieuKhoan];
  if (draft?.khongBaoGom?.trim()) dieuKhoan.splice(1, 0, `Không bao gồm ${draft.khongBaoGom.trim().replace(/^không bao gồm\s*/i, "")}`);

  return {
    draft: {
      tenMay: String(draft?.tenMay || "").toUpperCase(),
      maHang: machine?.code || String(draft?.maHang || ""),
      quyTrinh: String(draft?.quyTrinh || ""),
      dacDiem: Array.isArray(draft?.dacDiem) ? draft.dacDiem.map(String) : [],
      thongSo: Array.isArray(draft?.thongSo) ? draft.thongSo : [],
      donGia: machine?.sellPrice ? String(machine.sellPrice) : "",
      dieuKhoan,
      thanhToan: [...DEFAULT_TERMS.thanhToan],
    } as QuoteData,
    machine,
    usage: meter.summary(),
  };
}

// ================================================================ 2. Điền vào file mẫu
const esc = (s: string) =>
  String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

const run = (rPr: string, text: string) => `<w:r>${rPr}<w:t xml:space="preserve">${esc(text)}</w:t></w:r>`;

/** rPr của run đầu tiên có chữ (hoặc thoả điều kiện) trong đoạn mẫu */
function firstRPr(p: string, test: (r: string) => boolean = () => true) {
  const runs = p.match(/<w:r>[\s\S]*?<\/w:r>/g) || [];
  const r = runs.find((x) => /<w:t[ >]/.test(x) && test(x)) ?? runs.find((x) => /<w:t[ >]/.test(x));
  return r?.match(/<w:rPr>[\s\S]*?<\/w:rPr>/)?.[0] ?? "";
}

/** Giữ định dạng đoạn mẫu (pPr + rPr), thay toàn bộ chữ */
function setParaText(p: string, text: string, rPr = firstRPr(p)) {
  const open = p.match(/^<w:p[^>]*>/)![0];
  const pPr = p.match(/<w:pPr>[\s\S]*?<\/w:pPr>/)?.[0] ?? "";
  return `${open}${pPr}${run(rPr, text)}</w:p>`;
}

function setRowCells(row: string, values: string[]) {
  let i = 0;
  return row.replace(/<w:tc>[\s\S]*?<\/w:tc>/g, (tc) => {
    const v = values[i++] ?? "";
    let first = true;
    return tc.replace(/<w:p[ >][\s\S]*?<\/w:p>/g, (p) => {
      if (!first) return "";
      first = false;
      return setParaText(p, v);
    });
  });
}

export function formatPrice(v: string) {
  const s = String(v ?? "").trim();
  if (!s) return "Liên hệ";
  const digits = s.replace(/[^\d]/g, "");
  if (!digits || /[a-zA-Zà-ỹ]/i.test(s.replace(/vn[dđ]|đ/gi, ""))) return s; // chữ tự do, vd "Liên hệ"
  return `${Number(digits).toLocaleString("en-US")} VND`;
}

function imageSize(buf: Buffer): { w: number; h: number; ext: "png" | "jpeg" } | null {
  if (buf.length > 24 && buf.readUInt32BE(0) === 0x89504e47) return { w: buf.readUInt32BE(16), h: buf.readUInt32BE(20), ext: "png" };
  if (buf[0] === 0xff && buf[1] === 0xd8) {
    let o = 2;
    while (o < buf.length - 9) {
      if (buf[o] !== 0xff) {
        o++;
        continue;
      }
      const m = buf[o + 1];
      const len = buf.readUInt16BE(o + 2);
      if (m >= 0xc0 && m <= 0xcf && ![0xc4, 0xc8, 0xcc].includes(m)) {
        return { h: buf.readUInt16BE(o + 5), w: buf.readUInt16BE(o + 7), ext: "jpeg" };
      }
      o += 2 + len;
    }
  }
  return null; // webp, gif... -> không chèn
}

/** Ảnh máy trong "Kho ảnh máy" theo mã máy (ảnh mới nhất) */
async function findMachineImage(code: string): Promise<Buffer | null> {
  if (!code?.trim()) return null;
  try {
    const img = await prisma.machineImage.findFirst({
      where: { status: "READY", machineCode: { equals: code.trim(), mode: "insensitive" }, mimeType: { in: ["image/jpeg", "image/png", "image/jpg"] } },
      orderBy: { createdAt: "desc" },
      select: { r2Key: true },
    });
    if (!img) return null;
    const obj = await r2.send(new GetObjectCommand({ Bucket: R2_BUCKET_NAME, Key: img.r2Key }));
    const bytes = await (obj.Body as any)?.transformToByteArray?.();
    return bytes ? Buffer.from(bytes) : null;
  } catch {
    return null;
  }
}

export async function buildQuoteDocx(q: QuoteData): Promise<Buffer> {
  if (!fs.existsSync(TEMPLATE_PATH)) throw httpError(500, "Thiếu file mẫu báo giá assets/quote-template.docx");
  if (!q?.tenMay?.trim()) throw httpError(400, "Thiếu tên máy");

  const zip = await JSZip.loadAsync(fs.readFileSync(TEMPLATE_PATH));
  const docXml = await zip.file("word/document.xml")!.async("string");
  const bStart = docXml.indexOf("<w:body>") + "<w:body>".length;
  const bEnd = docXml.indexOf("</w:body>");
  const els: string[] = docXml.slice(bStart, bEnd).match(/<w:p[ >][\s\S]*?<\/w:p>|<w:tbl>[\s\S]*?<\/w:tbl>|<w:sectPr[\s\S]*?<\/w:sectPr>/g) || [];
  if (els.length < 23) throw httpError(500, "File mẫu báo giá không đúng cấu trúc");

  // Các đoạn mẫu (theo đúng thứ tự trong file mẫu của công ty)
  const at = (i: number) => els[i] as string;
  const T = {
    title: at(0), image: at(1), lblProcess: at(2), process: at(3), lblFeatures: at(5), bullet: at(6),
    blank: at(11), lblSpecs: at(12), table: at(13), note: at(15), lblPay: at(19), pay: at(20),
    sectPr: els.find((e) => e.startsWith("<w:sectPr")) as string,
  };

  const out: string[] = [setParaText(T.title, q.tenMay.trim().toUpperCase())];

  // ---- ảnh máy
  let imgBuf: Buffer | null = q.image?.data ? Buffer.from(q.image.data, "base64") : null;
  if (!imgBuf) imgBuf = await findMachineImage(q.maHang);
  const size = imgBuf ? imageSize(imgBuf) : null;
  if (imgBuf && size) {
    const maxCx = 3476625; // đúng bề ngang ảnh trong mẫu
    const maxCy = 3200400; // tối đa ~3.5 inch chiều cao
    let cx = maxCx;
    let cy = Math.round((cx * size.h) / size.w);
    if (cy > maxCy) {
      cy = maxCy;
      cx = Math.round((cy * size.w) / size.h);
    }
    const mediaName = `quote_machine.${size.ext}`;
    zip.file(`word/media/${mediaName}`, imgBuf);
    const relsPath = "word/_rels/document.xml.rels";
    let rels = await zip.file(relsPath)!.async("string");
    rels = rels.replace(
      "</Relationships>",
      `<Relationship Id="rIdQuoteImg" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" Target="media/${mediaName}"/></Relationships>`
    );
    zip.file(relsPath, rels);
    const pt = (emu: number) => (emu / 12700).toFixed(2);
    out.push(
      T.image
        .replace(/r:embed="[^"]+"/g, 'r:embed="rIdQuoteImg"')
        .replace(/r:id="[^"]+"/g, 'r:id="rIdQuoteImg"')
        .replace(/<wp:extent cx="\d+" cy="\d+"\/>/, `<wp:extent cx="${cx}" cy="${cy}"/>`)
        .replace(/<a:ext cx="\d+" cy="\d+">/, `<a:ext cx="${cx}" cy="${cy}">`)
        .replace(/width:[\d.]+pt;height:[\d.]+pt/, `width:${pt(cx)}pt;height:${pt(cy)}pt`)
    );
  }

  // ---- quy trình vận hành: bước in đậm, mũi tên chữ thường (giống mẫu)
  if (q.quyTrinh?.trim()) {
    out.push(T.lblProcess);
    const bold = firstRPr(T.process, (r) => r.includes("<w:b/>"));
    const normal = firstRPr(T.process, (r) => !r.includes("<w:b/>") && !r.includes("<w:i/>") && r.includes("w:color"));
    const steps = q.quyTrinh.split(/\s*-{2,}>\s*|\s*→\s*/).map((s) => s.trim()).filter(Boolean);
    const open = T.process.match(/^<w:p[^>]*>/)![0];
    const pPr = T.process.match(/<w:pPr>[\s\S]*?<\/w:pPr>/)?.[0] ?? "";
    out.push(`${open}${pPr}${steps.map((s, i) => (i ? run(normal, " ---> ") : "") + run(bold, s)).join("")}</w:p>`);
  }

  // ---- đặc điểm (gạch đầu dòng giữ đúng kiểu danh sách của mẫu)
  const feats = (q.dacDiem || []).map((s) => String(s).trim()).filter(Boolean);
  if (feats.length) {
    out.push(T.blank, T.lblFeatures, ...feats.map((f) => setParaText(T.bullet, f)));
  }

  // ---- bảng thông số: hàng đầu "Mã hàng" (đậm) ... hàng cuối "Đơn giá" (đậm)
  out.push(T.blank, T.lblSpecs);
  const rows: string[] = T.table.match(/<w:tr[ >][\s\S]*?<\/w:tr>/g) || [];
  if (rows.length < 3) throw httpError(500, "Bảng thông số trong file mẫu không đúng cấu trúc");
  const [rowHead, rowMid, rowLast] = [rows[0] as string, rows[1] as string, rows[rows.length - 1] as string];
  const head = T.table.slice(0, T.table.indexOf(rowHead));
  const specRows = (q.thongSo || []).filter((r) => r?.ten?.trim() || r?.giaTri?.trim());
  out.push(
    head +
      [
        setRowCells(rowHead, ["Mã hàng", q.maHang?.trim() || ""]),
        ...specRows.map((r) => setRowCells(rowMid, [String(r.ten ?? "").trim(), String(r.giaTri ?? "").trim()])),
        setRowCells(rowLast, ["Đơn giá", formatPrice(q.donGia)]),
      ].join("") +
      "</w:tbl>"
  );
  out.push(T.blank);

  // ---- điều khoản + thanh toán
  for (const line of (q.dieuKhoan || []).map((s) => String(s).trim()).filter(Boolean)) out.push(setParaText(T.note, line));
  const pays = (q.thanhToan || []).map((s) => String(s).trim()).filter(Boolean);
  if (pays.length) out.push(T.lblPay, ...pays.map((l) => setParaText(T.pay, l)));
  out.push(T.blank, T.sectPr);

  zip.file("word/document.xml", docXml.slice(0, bStart) + out.join("") + docXml.slice(bEnd));

  // ---- đổi ngày trên header sang hôm nay (giờ Việt Nam)
  const d = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Ho_Chi_Minh" }));
  const today = `Ngày ${String(d.getDate()).padStart(2, "0")} tháng ${String(d.getMonth() + 1).padStart(2, "0")} năm ${d.getFullYear()}`;
  for (const f of Object.keys(zip.files).filter((n) => /^word\/header\d*\.xml$/.test(n))) {
    const x = await zip.file(f)!.async("string");
    zip.file(f, x.replace(/Ngày\s*\d{1,2}\s*tháng\s*\d{1,2}\s*năm\s*\d{4}/g, today));
  }

  return zip.generateAsync({ type: "nodebuffer", compression: "DEFLATE" });
}

export function quoteFileName(q: QuoteData) {
  const base = (q.maHang?.trim() || q.tenMay || "bao-gia")
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/đ/gi, "d")
    .replace(/[^A-Za-z0-9-]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return `Bao_gia_${base}.docx`;
}