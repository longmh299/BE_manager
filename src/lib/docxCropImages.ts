// src/lib/docxCropImages.ts
// ✅ "Cắt hẳn" ảnh đã crop trong file Word để xem trên ĐIỆN THOẠI cũng đúng như trên máy tính.
//
// Vì sao cần: khi crop ảnh trong Word, phần bị cắt KHÔNG bị xoá — Word chỉ ghi chú vùng hiển thị
// (<a:srcRect l t r b> hoặc VML croptop/cropleft...). Word trên máy tính đọc ghi chú này, còn nhiều
// app xem file trên điện thoại (Zalo, Android preview, WPS...) bỏ qua -> hiện nguyên ảnh gốc
// (vd: cả cửa sổ phần mềm SolidWorks thay vì chỉ hình cái máy).
//
// Cách làm: với mỗi ảnh có crop -> cắt điểm ảnh thật bằng sharp -> lưu thành file ảnh mới
// -> trỏ ảnh trong tài liệu sang file mới, bỏ ghi chú crop. Kích thước hiển thị giữ nguyên.
//
// AN TOÀN: chỉ xử lý ảnh PNG / JPEG / WebP / GIF / TIFF. Bỏ qua (giữ nguyên như cũ):
//   - ảnh vector EMF / WMF / SVG (thư viện ảnh không cắt được)
//   - crop "âm" (thêm viền quanh ảnh), ảnh có bản SVG kèm theo
//   - bất kỳ ảnh nào cắt bị lỗi -> giữ ảnh gốc, không bao giờ làm hỏng file
import JSZip from "jszip";
import sharp from "sharp";
import path from "path";

/** Phần XML có thể chứa ảnh (mỗi phần có file rels riêng) */
const PARTS = /^word\/(document|header\d*|footer\d*|footnotes|endnotes|comments)\.xml$/;
const RASTER = new Set(["png", "jpeg", "jpg", "webp", "gif", "tiff", "tif"]);

/** crop theo tỉ lệ 0..1 của chiều rộng / cao ảnh gốc */
type Crop = { l: number; t: number; r: number; b: number };

const relsPath = (part: string) => `${path.posix.dirname(part)}/_rels/${path.posix.basename(part)}.rels`;

/** Đọc crop dạng DrawingML: đơn vị 1/1000 phần trăm (100000 = 100%) */
function parseSrcRect(tag: string): Crop | null {
  const v = (k: string) => Number(tag.match(new RegExp(`\\s${k}="(-?\\d+)"`))?.[1] || 0) / 100000;
  const c = { l: v("l"), t: v("t"), r: v("r"), b: v("b") };
  return c.l || c.t || c.r || c.b ? c : null;
}

/** Đọc crop dạng VML: "26013f" = 26013/65536, hoặc số thập phân "0.25" */
function parseVmlCrop(tag: string): Crop | null {
  const v = (k: string) => {
    const s = tag.match(new RegExp(`\\scrop${k}="([^"]+)"`))?.[1];
    if (!s) return 0;
    return /f$/i.test(s) ? Number(s.slice(0, -1)) / 65536 : Number(s);
  };
  const c = { l: v("left"), t: v("top"), r: v("right"), b: v("bottom") };
  return c.l || c.t || c.r || c.b ? c : null;
}

const validCrop = (c: Crop) =>
  [c.l, c.t, c.r, c.b].every((x) => Number.isFinite(x) && x >= 0) && c.l + c.r < 0.98 && c.t + c.b < 0.98;

async function cropBuffer(buf: Buffer, c: Crop): Promise<{ data: Buffer; ext: string } | null> {
  const img = sharp(buf, { animated: false });
  const m = await img.metadata();
  const fmt = String(m.format || "") === "jpg" ? "jpeg" : String(m.format || "");
  if (!m.width || !m.height || !fmt || !RASTER.has(fmt)) return null;
  const left = Math.round(m.width * c.l);
  const top = Math.round(m.height * c.t);
  const width = Math.round(m.width * (1 - c.l - c.r));
  const height = Math.round(m.height * (1 - c.t - c.b));
  if (width < 2 || height < 2 || left + width > m.width || top + height > m.height) return null;
  const out = img.extract({ left, top, width, height });
  if (fmt === "jpeg") return { data: await out.jpeg({ quality: 92 }).toBuffer(), ext: "jpeg" };
  return { data: await out.png().toBuffer(), ext: "png" }; // png / gif / webp / tiff -> png
}

/**
 * Cắt hẳn mọi ảnh đang crop trong file .docx (sửa trực tiếp trên zip).
 * Trả số ảnh đã cắt. Lỗi ở ảnh nào thì bỏ qua ảnh đó.
 */
export async function bakeImageCrops(zip: JSZip): Promise<number> {
  let done = 0;
  let seq = 0;
  const cache = new Map<string, Promise<string | null>>(); // "media gốc|crop" -> file media mới
  const touchedMedia = new Set<string>();
  const exts = new Set<string>();

  const makeCropped = (target: string, c: Crop) => {
    const key = `${target}|${c.l},${c.t},${c.r},${c.b}`;
    if (!cache.has(key)) {
      cache.set(
        key,
        (async () => {
          const f = zip.file(target);
          if (!f) return null;
          const res = await cropBuffer(await f.async("nodebuffer"), c).catch(() => null);
          if (!res) return null;
          const name = `word/media/vi_crop${++seq}.${res.ext}`;
          zip.file(name, res.data);
          exts.add(res.ext);
          touchedMedia.add(target);
          return name;
        })()
      );
    }
    return cache.get(key)!;
  };

  for (const part of Object.keys(zip.files).filter((n) => PARTS.test(n))) {
    const rp = relsPath(part);
    const relsFile = zip.file(rp);
    if (!relsFile) continue;
    let xml = await zip.file(part)!.async("string");
    let rels = await relsFile.async("string");
    const targets = new Map<string, string>(); // rId -> đường dẫn trong zip
    for (const m of rels.matchAll(/<Relationship\b[^>]*>/g)) {
      const id = m[0].match(/\sId="([^"]+)"/)?.[1];
      const t = m[0].match(/\sTarget="([^"]+)"/)?.[1];
      if (id && t && !/TargetMode="External"/.test(m[0])) {
        targets.set(id, t.startsWith("/") ? t.slice(1) : path.posix.normalize(`${path.posix.dirname(part)}/${t}`));
      }
    }
    let newRel = 0;
    const addRel = (mediaPath: string) => {
      const id = `rIdViCrop${++newRel}`;
      const rel = path.posix.relative(path.posix.dirname(part), mediaPath);
      rels = rels.replace(
        "</Relationships>",
        `<Relationship Id="${id}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" Target="${rel}"/></Relationships>`
      );
      return id;
    };

    // Thu thập các chỗ cần thay rồi thay từ cuối lên đầu (giữ đúng vị trí)
    const edits: Array<{ start: number; end: number; text: Promise<string | null> }> = [];

    // 1) DrawingML: <pic:blipFill> ... <a:blip r:embed="rIdX"> ... <a:srcRect l t r b/> ...
    for (const m of xml.matchAll(/<(\w+):blipFill\b[^>]*>[\s\S]*?<\/\1:blipFill>/g)) {
      const block = m[0];
      if (/svgBlip/.test(block)) continue;
      const rect = block.match(/<a:srcRect\b[^>]*\/>/)?.[0];
      const rid = block.match(/<a:blip\b[^>]*\sr:embed="([^"]+)"/)?.[1];
      const c = rect ? parseSrcRect(rect) : null;
      const target = rid ? targets.get(rid) : undefined;
      if (!c || !rid || !target || !validCrop(c)) continue;
      edits.push({
        start: m.index!,
        end: m.index! + block.length,
        text: makeCropped(target, c).then((media) =>
          media
            ? block.replace(`r:embed="${rid}"`, `r:embed="${addRel(media)}"`).replace(rect!, "<a:srcRect/>")
            : null
        ),
      });
    }

    // 2) VML kiểu cũ: <v:imagedata r:id="rIdX" croptop=".." cropright=".." .../>
    for (const m of xml.matchAll(/<v:imagedata\b[^>]*>/g)) {
      const tag = m[0];
      const rid = tag.match(/\sr:id="([^"]+)"/)?.[1];
      const c = parseVmlCrop(tag);
      const target = rid ? targets.get(rid) : undefined;
      if (!c || !rid || !target || !validCrop(c)) continue;
      edits.push({
        start: m.index!,
        end: m.index! + tag.length,
        text: makeCropped(target, c).then((media) =>
          media ? tag.replace(`r:id="${rid}"`, `r:id="${addRel(media)}"`).replace(/\scrop(?:top|bottom|left|right)="[^"]*"/g, "") : null
        ),
      });
    }

    if (!edits.length) continue;
    const texts = await Promise.all(edits.map((e) => e.text.catch(() => null)));
    edits
      .map((e, i) => ({ ...e, out: texts[i] }))
      .sort((a, b) => b.start - a.start)
      .forEach((e) => {
        if (e.out == null) return;
        xml = xml.slice(0, e.start) + e.out + xml.slice(e.end);
        done++;
      });
    zip.file(part, xml);
    zip.file(rp, rels);
  }

  if (!done) return 0;

  // Content type cho đuôi file mới (png/jpeg thường đã có sẵn)
  const ctFile = zip.file("[Content_Types].xml");
  if (ctFile) {
    let ct = await ctFile.async("string");
    for (const ext of exts) {
      if (!new RegExp(`Extension="${ext}"`, "i").test(ct)) {
        ct = ct.replace(/(<Types\b[^>]*>)/, `$1<Default Extension="${ext}" ContentType="image/${ext}"/>`);
      }
    }
    zip.file("[Content_Types].xml", ct);
  }

  // Ảnh gốc không còn chỗ nào dùng -> xoá cho file nhẹ (rels trỏ tới mà XML không dùng -> gỡ rels)
  const stillUsed = new Set<string>();
  for (const part of Object.keys(zip.files).filter((n) => /\.xml$/.test(n) && !/_rels\//.test(n))) {
    const rf = zip.file(relsPath(part));
    if (!rf) continue;
    const xml = await zip.file(part)!.async("string");
    let rels = await rf.async("string");
    rels = rels.replace(/<Relationship\b[^>]*\/>/g, (rel) => {
      const id = rel.match(/\sId="([^"]+)"/)?.[1] || "";
      const t = rel.match(/\sTarget="([^"]+)"/)?.[1] || "";
      const full = t.startsWith("/") ? t.slice(1) : path.posix.normalize(`${path.posix.dirname(part)}/${t}`);
      if (!touchedMedia.has(full)) return rel;
      if (!new RegExp(`"${id}"`).test(xml)) return ""; // không còn dùng trong phần này
      stillUsed.add(full);
      return rel;
    });
    zip.file(relsPath(part), rels);
  }
  for (const m of touchedMedia) if (!stillUsed.has(m)) zip.remove(m);

  return done;
}