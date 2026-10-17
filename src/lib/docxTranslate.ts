// src/lib/docxTranslate.ts
// ✅ Dịch file Word (.docx) thành BẢN TIẾNG VIỆT GIỮ NGUYÊN BỐ CỤC:
//    chỉ thay chữ, KHÔNG đụng hình ảnh, đánh số (1. / 1.1 / a)), bullet, bảng, text box,
//    header/footer, kiểu chữ, khổ giấy... — mọi thứ khác trong file giữ nguyên byte.
//
// Cách làm: .docx là file ZIP chứa XML. Chữ nằm trong các thẻ <w:t> thuộc từng đoạn <w:p>.
//   1. Quét XML, gom chữ của mỗi đoạn thành các "khúc" (segment). Khúc bị cắt tại tab, xuống dòng,
//      hình ảnh, trường tự động (số trang...) -> tab / ảnh / số trang vẫn đứng đúng chỗ sau khi dịch.
//   2. Dịch từng khúc (gom lô gửi AI, câu trùng chỉ dịch 1 lần).
//   3. Ghi bản dịch vào run (<w:r>) dài nhất của khúc (giữ định dạng chính của câu),
//      các run còn lại của khúc để trống.
// Đánh số tự động của Word nằm trong <w:numPr> (không phải chữ) nên luôn giữ nguyên.
// Ảnh nằm trong <w:drawing>/<w:pict> và word/media/* -> không bị sửa.
// Ảnh đã crop trong Word được cắt hẳn (docxCropImages.ts) để app điện thoại hiển thị đúng.
// Giới hạn: chữ nằm TRONG ảnh (ảnh chụp/scan), biểu đồ (charts) và SmartArt không được dịch.
import JSZip from "jszip";
import { bakeImageCrops } from "./docxCropImages";

/** Các phần XML có chữ cần dịch */
export const DOCX_TEXT_PARTS = /^word\/(document|header\d*|footer\d*|footnotes|endnotes|comments)\.xml$/;

type Part = { start: number; end: number; text: string };
export type Segment = { file: string; parts: Part[]; text: string };

const decode = (s: string) =>
  s
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCodePoint(parseInt(h, 16)))
    .replace(/&#(\d+);/g, (_, d) => String.fromCodePoint(parseInt(d, 10)))
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&");

const encode = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

// Thẻ cần quan tâm khi quét (thứ tự nhóm quan trọng):
//  1: mở đoạn <w:p ...>  (không khớp <w:pPr>, <w:pStyle>)
//  2: đóng đoạn </w:p>
//  3: chữ <w:t ...>...</w:t> (không khớp <w:tab>, <w:tbl>, <w:tc>, <w:tr>)
//  4: điểm cắt khúc: tab, xuống dòng, ảnh, đối tượng, trường tự động, ký hiệu, chú thích cuối trang
const TOKEN =
  /(<w:p(?=[\s>/])[^>]*>)|(<\/w:p>)|<w:t(?=[\s>/])[^>]*?(?:\/>|>([^<]*)<\/w:t>)|(<w:(?:tab|ptab|br|cr|drawing|pict|object|fldChar|sym|footnoteReference|endnoteReference)(?=[\s>/]))/g;

/** Tách các khúc chữ trong 1 file XML của Word */
export function extractSegments(xml: string, file: string): Segment[] {
  const out: Segment[] = [];
  const stack: Part[][] = []; // mỗi đoạn đang mở giữ khúc hiện tại
  const flush = () => {
    const cur = stack[stack.length - 1];
    if (!cur || !cur.length) return;
    out.push({ file, parts: cur.slice(), text: cur.map((p) => p.text).join("") });
    cur.length = 0;
  };

  TOKEN.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = TOKEN.exec(xml))) {
    if (m[1]) {
      if (m[1].endsWith("/>")) continue; // <w:p/> đoạn rỗng
      flush(); // đoạn lồng (text box nằm giữa câu) -> cắt khúc của đoạn ngoài
      stack.push([]);
    } else if (m[2]) {
      flush();
      stack.pop();
    } else if (m[4]) {
      flush();
    } else if (stack.length) {
      // thẻ <w:t>
      stack[stack.length - 1].push({ start: m.index, end: m.index + m[0].length, text: decode(m[3] ?? "") });
    }
  }
  return out;
}

/** Khúc có chữ cần dịch (chữ Hán/Nhật/Hàn hoặc chữ cái Latin), bỏ qua khúc chỉ có số / ký hiệu */
/** Đã là tiếng Việt (có dấu) và không lẫn chữ Hán -> không cần gửi dịch, đỡ tốn tiền */
const CJK = /[㐀-鿿぀-ヿ가-힯]/;
const VIET = /[ăâđêôơưàáạảãầấậẩẫằắặẳẵèéẹẻẽềếệểễìíịỉĩòóọỏõồốộổỗờớợởỡùúụủũừứựửữỳýỵỷỹ]/i;
export const hasWords = (s: string) => CJK.test(s) || (/[A-Za-z]{2,}/.test(s) && !VIET.test(s));

/** Ghi bản dịch vào XML. `tr` trả về bản dịch cho chữ gốc (đã trim), undefined = giữ nguyên */
export function applySegments(xml: string, segs: Segment[], tr: (src: string) => string | undefined) {
  const edits: Array<{ start: number; end: number; text: string }> = [];
  for (const s of segs) {
    const vi = tr(s.text.trim());
    if (vi == null || !vi.trim()) continue;
    const lead = s.text.match(/^\s*/)![0];
    const trail = s.text.match(/\s*$/)![0];
    const full = lead + vi.trim() + (s.text.trim() ? trail : "");
    // run giữ chữ nhiều nhất = định dạng chính của câu
    let main = 0;
    s.parts.forEach((p, i) => {
      if (p.text.trim().length > s.parts[main].text.trim().length) main = i;
    });
    s.parts.forEach((p, i) => {
      edits.push({
        start: p.start,
        end: p.end,
        text: i === main ? `<w:t xml:space="preserve">${encode(full)}</w:t>` : "<w:t></w:t>",
      });
    });
  }
  edits.sort((a, b) => b.start - a.start);
  let res = xml;
  for (const e of edits) res = res.slice(0, e.start) + e.text + res.slice(e.end);
  // Font gợi ý "eastAsia" (file gốc tiếng Trung) làm chữ Việt có dấu hiển thị lệch font -> bỏ gợi ý
  return res.replace(/\s+w:hint="eastAsia"/g, "");
}

/** Đọc file .docx -> danh sách khúc chữ của mọi phần XML */
export async function loadDocx(buf: Buffer) {
  let zip: JSZip;
  try {
    zip = await JSZip.loadAsync(buf);
  } catch {
    throw Object.assign(new Error("File không phải .docx hợp lệ (file .doc cũ hãy mở bằng Word rồi Lưu thành .docx)"), {
      statusCode: 400,
    });
  }
  if (!zip.file("word/document.xml")) {
    throw Object.assign(new Error("File không phải tài liệu Word (.docx)"), { statusCode: 400 });
  }
  const xmls = new Map<string, string>();
  const segments: Segment[] = [];
  for (const name of Object.keys(zip.files)) {
    if (!DOCX_TEXT_PARTS.test(name)) continue;
    const xml = await zip.file(name)!.async("string");
    xmls.set(name, xml);
    segments.push(...extractSegments(xml, name));
  }
  return { zip, xmls, segments };
}

/** Ghi bản dịch vào từng phần XML và đóng gói lại .docx (ảnh / media giữ nguyên) */
export async function saveDocx(
  doc: Awaited<ReturnType<typeof loadDocx>>,
  tr: (src: string) => string | undefined
): Promise<Buffer> {
  for (const [name, xml] of doc.xmls) {
    const segs = doc.segments.filter((s) => s.file === name);
    doc.zip.file(name, applySegments(xml, segs, tr));
  }
  // Cắt hẳn ảnh đã crop -> mở trên điện thoại cũng đúng như trên máy tính (lỗi thì giữ nguyên ảnh)
  try {
    const n = await bakeImageCrops(doc.zip);
    if (n) console.log(`[DOCX] đã cắt hẳn ${n} ảnh crop`);
  } catch (e) {
    console.warn("[DOCX] bỏ qua bước cắt ảnh:", (e as Error)?.message);
  }
  return doc.zip.generateAsync({ type: "nodebuffer", compression: "DEFLATE" });
}