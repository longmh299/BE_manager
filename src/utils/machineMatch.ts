// src/utils/machineMatch.ts
// ✅ Dò "tên máy người dùng gõ" -> đúng 1 máy trong danh mục.
// NGUỒN DỮ LIỆU: bảng Item, kind = MACHINE (cùng danh sách với trang "Máy" trong CRM),
// dò theo cả cột sku và cột name.
//
// MÃ MÁY dùng để đặt tên file (field `code`):
//   - Nếu sku là mã thật (vd "JL-1000")                      -> dùng sku
//   - Nếu sku là mã TỰ SINH (vd "MCRCR-20261013-0001", do buildSkuFrom tạo khi
//     không nhập sku) -> lấy mã model nằm trong tên máy ("Máy cắt rau củ JL-1000" -> "JL-1000")
//   - Tên cũng không có mã model -> đành dùng sku
import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

/** code = mã máy dùng để hiển thị + đặt tên file; sku/name = đúng như trong CRM */
export type MachineRef = { id: string; sku: string; name: string; code: string };

/** Bỏ dấu tiếng Việt, viết hoa, chỉ giữ chữ + số. "Máy cắt JL-1000" -> "MAYCATJL1000" */
export function norm(s: string | null | undefined) {
  return String(s ?? "")
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/đ/gi, "d")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

/** Slug an toàn cho đường dẫn R2: không dấu, không khoảng trắng. "JL-1000" -> "JL-1000" */
export function keySafe(s: string) {
  return (
    String(s)
      .normalize("NFD")
      .replace(/[̀-ͯ]/g, "")
      .replace(/đ/gi, "d")
      .replace(/[^A-Za-z0-9-]+/g, "-")
      .replace(/-+/g, "-")
      .replace(/^-|-$/g, "") || "chua-ro"
  );
}

/** SKU do hệ thống tự sinh (src/utils/sku.ts -> buildSkuFrom): "ABC-20261013-0001" */
export function isAutoSku(sku: string) {
  return /^[A-Z0-9]{1,12}-\d{8}-\d{4}$/i.test(String(sku || "").trim());
}

/** Tìm mã model trong tên máy: "Máy cắt rau củ JL-1000" -> "JL-1000", "Máy đóng vỉ DPP-150E" -> "DPP-150E" */
export function modelFromName(name: string): string | null {
  const s = String(name || "")
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/đ/gi, "d")
    .toUpperCase()
    .replace(/[_.,()]+/g, " ");
  const m = s.match(/\b([A-Z]{1,5}-?[A-Z]{0,2}\d{2,5}[A-Z0-9]*(?:-[A-Z0-9]+)?)\b/);
  return m ? m[1] : null;
}

export function machineCodeOf(it: { sku: string; name: string }) {
  if (it.sku && !isAutoSku(it.sku)) return it.sku.trim();
  return modelFromName(it.name) || it.sku;
}

type Row = MachineRef & { nSku: string; nName: string; nCode: string };

// Cache danh mục máy 5 phút — danh mục ít đổi, tránh query mỗi lần gõ phím.
let cache: { at: number; list: Row[] } | null = null;

async function loadMachines(): Promise<Row[]> {
  if (cache && Date.now() - cache.at < 5 * 60 * 1000) return cache.list;
  const rows = await prisma.item.findMany({
    where: { kind: "MACHINE" },
    select: { id: true, sku: true, name: true },
  });
  const list = rows
    .filter((r) => r.sku || r.name)
    .map((r) => {
      const code = machineCodeOf(r);
      return { ...r, code, nSku: norm(r.sku), nName: norm(r.name), nCode: norm(code) };
    });
  cache = { at: Date.now(), list };
  return list;
}

export function clearMachineCache() {
  cache = null;
}

/** Mã quá ngắn / chỉ toàn số dễ khớp nhầm (vd "100" khớp vào "1000021067") */
function codeUsable(n: string) {
  return n.length >= 3 && (/[A-Z]/.test(n) || n.length >= 5);
}

/**
 * Trả về:
 *  - match: máy khớp chắc chắn (hoặc null)
 *  - suggestions: tối đa 5 máy gần đúng để người dùng bấm chọn
 */
export async function resolveMachine(input: string | null | undefined): Promise<{
  match: MachineRef | null;
  suggestions: MachineRef[];
}> {
  const q = norm(input);
  if (q.length < 2) return { match: null, suggestions: [] };
  const list = await loadMachines();
  const out = ({ id, sku, name, code }: Row): MachineRef => ({ id, sku, name, code });

  // 1) Gõ đúng mã (mã model hoặc sku)
  const exact = list.filter((m) => m.nCode === q || m.nSku === q);
  if (exact.length === 1) return { match: out(exact[0]), suggestions: [] };

  // 2) Chuỗi gõ có chứa mã -> lấy mã DÀI nhất khớp (SH-109S thắng SH-109)
  const contained = list
    .map((m) => {
      const hits = [m.nCode, m.nSku].filter((c) => codeUsable(c) && q.includes(c));
      return { m, len: hits.length ? Math.max(...hits.map((h) => h.length)) : 0 };
    })
    .filter((x) => x.len > 0)
    .sort((a, b) => b.len - a.len);
  if (contained.length === 1 || (contained.length > 1 && contained[0].len > contained[1].len)) {
    return { match: out(contained[0].m), suggestions: [] };
  }

  // 3) Gõ đúng tên máy
  const byName = list.filter((m) => m.nName === q);
  if (byName.length === 1) return { match: out(byName[0]), suggestions: [] };

  // 4) Không chắc -> gợi ý
  const digits = q.replace(/[^0-9]/g, "");
  const scored = list
    .map((m) => {
      let s = 0;
      if (m.nCode.includes(q) || m.nSku.includes(q)) s += 3;
      if (contained.some((x) => x.m === m)) s += 3;
      if (q.length >= 3 && m.nName.includes(q)) s += 2;
      if (digits.length >= 3 && (m.nCode.includes(digits) || m.nSku.includes(digits))) s += 1; // gõ thiếu chữ: "1000"
      return { m, s };
    })
    .filter((x) => x.s > 0)
    .sort((a, b) => b.s - a.s || a.m.nCode.length - b.m.nCode.length)
    .slice(0, 5)
    .map((x) => out(x.m));

  return { match: null, suggestions: scored };
}

/** Giờ Việt Nam dạng { ym: "2026-10", stamp: "20261013_095012" } */
export function vnStamp(d = new Date()) {
  const t = d.toLocaleString("sv-SE", { timeZone: "Asia/Ho_Chi_Minh" }); // "2026-10-13 09:50:12"
  return { ym: t.slice(0, 7), stamp: t.replace(/[-:]/g, "").replace(" ", "_") };
}

/** Lấy đuôi file an toàn (mặc định theo mime) */
export function fileExt(fileName: string, mimeType?: string) {
  const m = /\.([A-Za-z0-9]{2,5})$/.exec(fileName || "");
  if (m) return m[1].toLowerCase();
  if (mimeType?.includes("quicktime")) return "mov";
  if (mimeType?.startsWith("image/")) return mimeType.split("/")[1].replace("jpeg", "jpg");
  return "mp4";
}