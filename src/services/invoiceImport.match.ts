// src/services/invoiceImport.match.ts
// ✅ MỚI — Nhập hoá đơn từ file: ĐỌC FILE + KHỚP MÃ (thuần logic, không đụng DB -> test được độc lập)
//
// Nguyên tắc chính xác:
//  - Số lượng / đơn giá / thành tiền đọc THẲNG từ ô Excel / thẻ XML, KHÔNG dùng AI, không tự tính lại.
//  - GIỮ MÃ CŨ: 1 mã có thể nhiều linh kiện -> mã chỉ để THU HẸP, tên mới QUYẾT ĐỊNH.
//  - 🟢 (tự khớp) chỉ khi CHẮC CHẮN: theo ID file mẫu / đã xác nhận lần trước / trùng tên / đúng mã + đủ tên + đúng thông số.
//    Mọi trường hợp "gần giống" -> 🟡 bắt buộc người chọn. Không có gì giống -> 🔵 tạo mã mới.
import * as XLSX from "xlsx";

// ============================================================ kiểu dữ liệu
export type LineKind = "hang" | "chiet_khau" | "khuyen_mai";
export type SrcLine = {
  row: number; // số dòng trong file (để người dùng dò lại)
  id: string; // cột "ID hệ thống" của file mẫu (nếu có)
  code: string;
  name: string;
  unit: string;
  qty: number;
  price: number;
  amount: number;
  kind: LineKind;
  flags: string[]; // điểm cần kiểm tra -> dòng không bao giờ được 🟢
};
export type Party = { name: string; taxCode: string; address: string; phone: string };
export type Parsed = {
  source: "xml" | "excel" | "template";
  invoiceNo: string;
  invoiceKey: string;
  date: string;
  seller: Party;
  buyer: Party;
  lines: SrcLine[];
  fileTotal: number | null; // tổng tiền hàng ghi trong file (chưa thuế) nếu có
  skipped: number; // dòng file mẫu để trống số lượng -> bỏ qua
  critical: string[];
  notes: string[];
};

export type CatalogItem = {
  id: string;
  sku: string;
  name: string;
  kind: string; // MACHINE | PART
  unit: string;
  price: number;
  stockQty: number;
};
export type Cand = { itemId: string; sku: string; name: string; kind: string; unit: string; price: number; stockQty: number; score: number };
export type MatchStatus = "ok" | "check" | "new" | "skip";
export type MatchResult = {
  status: MatchStatus;
  itemId: string | null;
  reason: string;
  candidates: Cand[];
  rawKey: string;
};

const EMPTY_PARTY: Party = { name: "", taxCode: "", address: "", phone: "" };

export function httpError(status: number, message: string) {
  const err: any = new Error(message);
  err.statusCode = status;
  err.status = status;
  return err;
}

// ============================================================ chữ
export function plain(s: string) {
  return String(s ?? "")
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/đ/g, "d")
    .replace(/Đ/g, "D")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
export const compact = (s: string) => plain(s).replace(/ /g, "");
/** Tên CRM dạng "Máy rửa sục khí[QX-22]" -> bỏ phần [mã] */
export const baseName = (s: string) => String(s ?? "").replace(/\[[^\]]*\]/g, " ").replace(/\s+/g, " ").trim();
/** Khoá ghi nhớ của 1 dòng file: mã|tên (không dấu) */
export const rawKeyOf = (code: string, name: string) => `${compact(code)}|${plain(name)}`;

const ABBR: Record<string, string> = { ck: "chan khong", tđ: "tu dong", td: "tu dong" };
// "5 bộ", "4 cái" trong TÊN là số lượng đóng gói, không phải thông số
const COUNT_RE = /\b\d+\s*(bo|cai|chiec|cuon|cay|con|tam|hop|thung|goi|doi|cap)\b/g;
const STOP = new Set(["may", "cho", "va", "cua", "loai", "hang", "cac", "the", "nhung", "voi", "bang"]);

export function tokens(s: string): string[] {
  const p = plain(baseName(s)).replace(COUNT_RE, " ");
  return p
    .split(" ")
    .flatMap((t) => (ABBR[t] ? ABBR[t].split(" ") : [t]))
    .filter((t) => t && !STOP.has(t));
}
const hasDigit = (t: string) => /\d/.test(t);

function bigrams(s: string) {
  const t = ` ${s} `;
  const out = new Set<string>();
  for (let i = 0; i < t.length - 1; i++) out.add(t.slice(i, i + 2));
  return out;
}
export function dice(a: string, b: string) {
  if (!a || !b) return 0;
  const A = bigrams(a);
  const B = bigrams(b);
  let n = 0;
  A.forEach((x) => B.has(x) && n++);
  return (2 * n) / (A.size + B.size || 1);
}

// ============================================================ số (kiểu VN + kiểu Anh)
/** "70,859,000" | "1.500.000" | "2,5" | 47280000 | "4 cái" -> số. ambiguous: "1,500" / "1.500" (1 nhóm 3 số) */
export function parseNum(v: any): { value: number; ok: boolean; ambiguous: boolean; unitWord: string } {
  if (typeof v === "number") return { value: v, ok: Number.isFinite(v), ambiguous: false, unitWord: "" };
  const s = String(v ?? "").trim();
  if (!s) return { value: 0, ok: false, ambiguous: false, unitWord: "" };
  const m = s.match(/-?\d[\d.,\s]*/);
  if (!m) return { value: 0, ok: false, ambiguous: false, unitWord: "" };
  let t = m[0].replace(/\s+/g, "").replace(/[.,]+$/, "");
  const unitWord = s.slice((m.index || 0) + m[0].length).trim().replace(/^[.,]/, "").trim();
  let ambiguous = false;
  const hasDot = t.includes(".");
  const hasComma = t.includes(",");
  if (hasDot && hasComma) {
    // dấu xuất hiện SAU CÙNG là dấu thập phân
    const dec = t.lastIndexOf(".") > t.lastIndexOf(",") ? "." : ",";
    const th = dec === "." ? "," : ".";
    t = t.split(th).join("").replace(dec, ".");
  } else if (hasDot || hasComma) {
    const sep = hasDot ? "." : ",";
    const parts = t.split(sep);
    if (parts.length > 2 || (parts.length === 2 && parts[1].length === 3 && parts[0] !== "0" && parts[0] !== "-0")) {
      // 1.500.000 / 70,859,000 / 1,500 -> phân cách hàng nghìn
      if (parts.length === 2) ambiguous = true;
      t = parts.join("");
    } else {
      t = parts.join("."); // 2,5 / 0.13
    }
  }
  const value = Number(t);
  return { value, ok: Number.isFinite(value), ambiguous, unitWord };
}
const fmt = (n: number) => (Math.round(n * 1000) / 1000).toLocaleString("vi-VN");

// ============================================================ 1) Hoá đơn điện tử XML (TT78/NĐ123)
function decode(s: string) {
  return (s || "")
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&#(\d+);/g, (_, d) => String.fromCodePoint(+d))
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCodePoint(parseInt(h, 16)))
    .replace(/&amp;/g, "&")
    .trim();
}
const reTag = (name: string, g = "") => new RegExp(`<(?:[\\w-]+:)?${name}(?:\\s[^>]*)?>([\\s\\S]*?)</(?:[\\w-]+:)?${name}>`, g);
const one = (xml: string, name: string) => {
  const m = xml.match(reTag(name));
  return m ? m[1] : "";
};
const all = (xml: string, name: string) => [...xml.matchAll(reTag(name, "g"))].map((m) => m[1]);
const txt = (xml: string, name: string) => {
  const parts = one(xml, name).split(/(<!\[CDATA\[[\s\S]*?\]\]>)/);
  return decode(parts.map((x) => (x.startsWith("<![CDATA[") ? x : x.replace(/<[^>]+>/g, ""))).join(""));
};
export const normTax = (s: string) => (s || "").replace(/[^0-9-]/g, "").replace(/-+$/, "");
const xnum = (s: string) => {
  // số trong XML HĐĐT luôn dạng máy: 1500000.50
  const n = Number(String(s ?? "").trim());
  return Number.isFinite(n) ? n : 0;
};
const party = (xml: string): Party => ({
  name: txt(xml, "Ten"),
  taxCode: normTax(txt(xml, "MST")),
  address: txt(xml, "DChi"),
  phone: txt(xml, "SDThoai"),
});

export function parseEInvoiceXml(raw: string): Parsed {
  const xml = raw.replace(/^﻿/, "").replace(/<DSCKS>[\s\S]*<\/DSCKS>/g, "");
  const hd = one(xml, "DLHDon") || xml;
  if (!/<(?:[\w-]+:)?HHDVu[\s>]/.test(hd)) throw httpError(400, "File XML này không phải hoá đơn điện tử (không thấy danh sách hàng hoá HHDVu)");
  const ttc = one(hd, "TTChung");
  const nd = one(hd, "NDHDon");
  const tt = one(nd, "TToan");
  const critical: string[] = [];
  const notes: string[] = [];

  const lq = one(ttc, "TTHDLQuan");
  if (lq) {
    const tc = txt(lq, "TCHDon");
    const goc = [txt(lq, "KHMSHDCLQuan"), txt(lq, "KHHDCLQuan"), txt(lq, "SHDCLQuan")].filter(Boolean).join("");
    critical.push(
      `Đây là hoá đơn ${tc === "1" ? "THAY THẾ" : tc === "2" ? "ĐIỀU CHỈNH" : "liên quan"} cho hoá đơn ${goc || "khác"}. ` +
        `Nếu hoá đơn gốc đã nhập vào CRM thì phải SỬA phiếu cũ, không tạo phiếu mới (sẽ cộng trùng tồn kho và công nợ).`
    );
  }

  const cur = (txt(ttc, "DVTTe") || "VND").toUpperCase();
  const rate = xnum(txt(ttc, "TGia"));
  let fx = 1;
  if (cur !== "VND") {
    if (rate > 0) {
      fx = rate;
      notes.push(`Hoá đơn tính bằng ${cur}, đã quy ra VND theo tỷ giá ghi trên hoá đơn: ${fmt(rate)}.`);
    } else critical.push(`Hoá đơn tính bằng ${cur} nhưng không ghi tỷ giá — đơn giá đang là ${cur}, KHÔNG phải VND.`);
  }

  const lines: SrcLine[] = [];
  all(one(nd, "DSHHDVu") || nd, "HHDVu").forEach((h, i) => {
    const tchat = txt(h, "TChat") || "1"; // 1 hàng, 2 khuyến mại, 3 chiết khấu, 4 ghi chú
    if (tchat === "4") return;
    const name = txt(h, "THHDVu");
    if (!name) return;
    const flags: string[] = [];
    const qty = xnum(txt(h, "SLuong"));
    const dgia = xnum(txt(h, "DGia"));
    const thtien = xnum(txt(h, "ThTien"));
    const ck = xnum(txt(h, "STCKhau"));
    const kind: LineKind = tchat === "3" ? "chiet_khau" : tchat === "2" ? "khuyen_mai" : "hang";
    let price = dgia;
    let amount = thtien || qty * dgia;
    if (kind === "chiet_khau") {
      amount = -Math.abs(thtien || dgia * (qty || 1));
      flags.push("Dòng chiết khấu thương mại — không đưa vào phiếu, hạch toán tay nếu cần.");
    } else if (kind === "khuyen_mai") {
      flags.push("Hàng khuyến mại / tặng kèm — kiểm tra có nhập kho không.");
    } else if (ck > 0 && qty > 0 && thtien > 0) {
      price = Math.round((thtien / qty) * 100) / 100;
      flags.push(`Dòng có chiết khấu ${fmt(ck)} đ: đơn giá ${fmt(dgia)} → giá thực ${fmt(price)}.`);
    }
    if (kind === "hang" && qty > 0 && dgia > 0 && thtien > 0 && !ck && Math.abs(qty * dgia - thtien) > 1)
      flags.push(`SL × đơn giá (${fmt(qty * dgia)}) ≠ thành tiền (${fmt(thtien)}).`);
    lines.push({ row: i + 1, id: "", code: txt(h, "MHHDVu"), name, unit: txt(h, "DVTinh"), qty, price: price * fx, amount: amount * fx, kind, flags });
  });

  const seller = party(one(nd, "NBan"));
  const khms = txt(ttc, "KHMSHDon");
  const khhd = txt(ttc, "KHHDon");
  const shd = txt(ttc, "SHDon");
  const invoiceNo = `${khms}${khhd}-${shd}`.replace(/^-/, "");
  return {
    source: "xml",
    invoiceNo,
    invoiceKey: shd && seller.taxCode ? `HDDT|${invoiceNo}|${seller.taxCode}` : "",
    date: (txt(ttc, "NLap") || "").slice(0, 10),
    seller,
    buyer: party(one(nd, "NMua")),
    lines,
    fileTotal: txt(tt, "TgTCThue") ? xnum(txt(tt, "TgTCThue")) * fx : null,
    skipped: 0,
    critical,
    notes,
  };
}

// ============================================================ 2) Excel: nhận cột theo TIÊU ĐỀ (không AI)
type Field = "id" | "code" | "name" | "unit" | "qty" | "price" | "amount";
const HEADER: [Field, RegExp][] = [
  ["id", /^(id|id he thong|item id|ma he thong)$/],
  ["qty", /^(so luong|sl|s l|qty|quantity|q ty)( .*)?$/],
  ["price", /^(don gia|gia|gia nhap|gia mua|unit price|price|ip|u price)( .*)?$/],
  ["amount", /^(thanh tien|amount|total amount|tong tien)( .*)?$/],
  ["unit", /^(dvt|d v t|don vi|don vi tinh|unit)$/],
  ["code", /^(ma|ma hang|ma sp|ma san pham|ma may|ma vt|ma vat tu|ma hang hoa|model|sku|code|item code|part no)$/],
  ["name", /^(ten|ten hang|ten sp|ten san pham|ten hang hoa|ten vat tu|hang hoa|san pham|dien giai|noi dung|description|description of goods?|goods|item|item name|product)( .*)?$/],
];

function detectHeader(rows: any[][]) {
  let best: { row: number; cols: Partial<Record<Field, number>>; n: number } | null = null;
  for (let r = 0; r < Math.min(rows.length, 25); r++) {
    const cols: Partial<Record<Field, number>> = {};
    (rows[r] || []).forEach((cell, c) => {
      const p = plain(String(cell ?? ""));
      if (!p || p.length > 40) return;
      for (const [f, re] of HEADER) {
        if (cols[f] === undefined && re.test(p)) {
          cols[f] = c;
          break;
        }
      }
    });
    const n = Object.keys(cols).length;
    if (cols.name !== undefined && cols.qty !== undefined && (cols.price !== undefined || cols.amount !== undefined) && (!best || n > best.n))
      best = { row: r, cols, n };
  }
  return best;
}

export function parseExcel(buf: Buffer): Parsed {
  const wb = XLSX.read(buf, { type: "buffer", cellDates: true });
  let found: { sheet: string; rows: any[][]; h: NonNullable<ReturnType<typeof detectHeader>> } | null = null;
  for (const sn of wb.SheetNames) {
    const rows: any[][] = XLSX.utils.sheet_to_json(wb.Sheets[sn], { header: 1, raw: true, defval: "" });
    const h = detectHeader(rows);
    if (h) {
      found = { sheet: sn, rows, h };
      break;
    }
  }
  if (!found)
    throw httpError(
      422,
      "Không nhận ra cột trong file. File cần có dòng tiêu đề gồm ít nhất: Tên hàng (hoặc Description), Số lượng (QTY), Đơn giá (hoặc Thành tiền). Có thể tải File mẫu để dùng."
    );

  const { rows, h } = found;
  const C = h.cols;
  const isTemplate = C.id !== undefined;
  const cell = (r: any[], f: Field) => (C[f] === undefined ? "" : r[C[f]!]);
  const str = (v: any) => String(v ?? "").replace(/\s+/g, " ").trim();
  const lines: SrcLine[] = [];
  let fileTotal: number | null = null;
  let skipped = 0;
  const notes: string[] = [];
  const critical: string[] = [];

  for (let r = h.row + 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const name = str(cell(row, "name"));
    const code = str(cell(row, "code"));
    const id = str(cell(row, "id"));
    const qRaw = cell(row, "qty");
    const pRaw = cell(row, "price");
    const aRaw = cell(row, "amount");

    // dòng tổng cộng -> lấy làm tổng đối chiếu, không phải hàng
    if (/^(tong|cong|total|sum)\b/.test(plain(name || code))) {
      const t = parseNum(aRaw !== "" ? aRaw : pRaw);
      if (t.ok && t.value > 0) fileTotal = t.value;
      continue;
    }
    if (!name && !code && !id) continue; // dòng trống / dòng "0" thừa cuối bảng

    const q = parseNum(qRaw);
    if (isTemplate && (!q.ok || q.value === 0) && str(pRaw) === "") {
      skipped++; // file mẫu: dòng không điền số lượng = không nhập
      continue;
    }
    const p = parseNum(pRaw);
    const a = parseNum(aRaw);
    const flags: string[] = [];

    let qty = q.ok ? q.value : 0;
    let price = p.ok ? p.value : 0;
    const amount = a.ok ? a.value : 0;
    if (!q.ok) flags.push(`Không đọc được số lượng "${str(qRaw)}".`);
    else if (qty <= 0) flags.push("Số lượng phải > 0.");
    else if (q.ambiguous) flags.push(`Số lượng "${str(qRaw)}" có thể là ${fmt(qty)} hoặc ${str(qRaw).replace(/[.,]/, ",")} — kiểm tra lại.`);
    if (typeof qRaw === "string" && q.ok && q.unitWord && /[a-zA-ZÀ-ỹ]/.test(q.unitWord))
      notes.push(`Dòng ${r + 1}: số lượng ghi kèm đơn vị "${str(qRaw)}" → SL ${fmt(qty)}, ĐVT "${q.unitWord}".`);
    if (!p.ok && amount > 0 && qty > 0) {
      price = Math.round((amount / qty) * 100) / 100;
      flags.push(`Không có đơn giá, tính từ thành tiền: ${fmt(amount)} / ${fmt(qty)} = ${fmt(price)}.`);
    } else if (!p.ok) flags.push(`Không đọc được đơn giá "${str(pRaw)}".`);
    else if (price <= 0) flags.push("Đơn giá = 0.");
    if (p.ok && amount > 0 && qty > 0 && Math.abs(qty * price - amount) > 1)
      flags.push(`SL × đơn giá (${fmt(qty * price)}) ≠ thành tiền trong file (${fmt(amount)}).`);

    lines.push({
      row: r + 1,
      id,
      code,
      name: name || code,
      unit: str(cell(row, "unit")) || (typeof qRaw === "string" ? q.unitWord : ""),
      qty,
      price,
      amount: amount || qty * price,
      kind: "hang",
      flags,
    });
  }
  if (lines.length > 500) throw httpError(422, `File có ${lines.length} dòng hàng — tối đa 500 dòng / lần, hãy tách file.`);
  if (!lines.length && isTemplate) throw httpError(422, `File mẫu chưa điền số lượng dòng nào (${skipped} dòng để trống).`);

  return {
    source: isTemplate ? "template" : "excel",
    invoiceNo: "",
    invoiceKey: "",
    date: "",
    seller: EMPTY_PARTY,
    buyer: EMPTY_PARTY,
    lines,
    fileTotal,
    skipped,
    critical,
    notes,
  };
}

// ============================================================ 3) Khớp với danh mục — GIỮ MÃ CŨ
type Ix = { it: CatalogItem; skuKey: string; base: string; tok: Set<string>; specs: Set<string>; skuToks: Set<string> };

export function buildIndex(items: CatalogItem[]) {
  const ix: Ix[] = items.map((it) => {
    const skuToks = new Set(tokens(it.sku));
    const tok = new Set([...tokens(it.name), ...skuToks]);
    return {
      it,
      skuKey: compact(it.sku),
      base: plain(baseName(it.name)),
      tok,
      specs: new Set(tokens(it.name).filter((t) => hasDigit(t) && !skuToks.has(t))),
      skuToks,
    };
  });
  const byId = new Map(ix.map((x) => [x.it.id, x]));
  const bySku = new Map<string, Ix[]>();
  const byBase = new Map<string, Ix[]>();
  for (const x of ix) {
    if (x.skuKey) bySku.set(x.skuKey, [...(bySku.get(x.skuKey) || []), x]);
    if (x.base) byBase.set(x.base, [...(byBase.get(x.base) || []), x]);
  }
  return { ix, byId, bySku, byBase };
}
export type CatalogIndex = ReturnType<typeof buildIndex>;

export const lineKindOf = (name: string) => (/^may\b/.test(plain(name)) ? "MACHINE" : "PART");

/** tên trong file ĐỦ CHỮ + ĐÚNG THÔNG SỐ so với 1 mã hàng (bỏ qua phần mã) */
function nameFits(l: SrcLine, x: Ix) {
  const codeToks = new Set([...tokens(l.code), compact(l.code)]);
  const lt = tokens(l.name).filter((t) => !codeToks.has(t) && !x.skuToks.has(t));
  if (!lt.length) return false;
  const lineCompactName = compact(l.name);
  if (!lt.every((t) => x.tok.has(t) || (hasDigit(t) && compact(x.it.name).includes(t)))) return false;
  const ls = new Set(lt.filter(hasDigit));
  for (const s of x.specs) if (!ls.has(s) && !lineCompactName.includes(s)) return false; // mã CRM có thông số mà file không ghi -> chưa chắc
  return true;
}

function toCand(x: Ix, score: number): Cand {
  const { id, sku, name, kind, unit, price, stockQty } = x.it;
  return { itemId: id, sku, name, kind, unit, price, stockQty, score: Math.round(score * 100) / 100 };
}

const OPPOSITE: [string, string][] = [
  ["dai", "ngan"],
  ["don", "doi"],
  ["to", "nho"],
  ["trai", "phai"],
  ["tren", "duoi"],
  ["trong", "ngoai"],
  ["nong", "lanh"],
  ["dung", "nam"],
];

function score(l: SrcLine, x: Ix, lkind: string) {
  const codeKey = compact(l.code);
  const ln = plain(baseName(l.name)).replace(COUNT_RE, " ").replace(/\s+/g, " ").trim();
  let s = dice(ln, x.base);
  if (codeKey && x.skuKey === codeKey) s += 0.6;
  else if (codeKey.length >= 3 && x.skuKey.length >= 3 && (x.skuKey.includes(codeKey) || codeKey.includes(x.skuKey))) s += 0.25;
  else if (codeKey.length >= 4 && compact(x.it.name).includes(codeKey)) s += 0.3;
  const lt = tokens(l.name);
  const miss = lt.filter((t) => !x.tok.has(t)).length;
  s -= 0.08 * miss;
  const ls = lt.filter(hasDigit);
  if (ls.length) s += 0.25 * (ls.filter((t) => x.tok.has(t)).length / ls.length);
  s += x.it.kind === lkind ? 0.15 : -0.35;
  // từ trái nghĩa (dài/ngắn, đơn/đôi...) -> chắc chắn KHÁC món
  for (const [a, b] of OPPOSITE) if ((lt.includes(a) && x.tok.has(b)) || (lt.includes(b) && x.tok.has(a))) s -= 0.6;
  return s;
}

export function matchLine(
  l: SrcLine,
  cat: CatalogIndex,
  alias: Map<string, string> // rawKey -> itemId (đã lọc theo NCC)
): MatchResult {
  const rawKey = rawKeyOf(l.code, l.name);
  const res = (status: MatchStatus, itemId: string | null, reason: string, candidates: Cand[] = []): MatchResult => ({
    status,
    itemId,
    reason,
    candidates,
    rawKey,
  });
  if (l.kind === "chiet_khau") return res("skip", null, "Dòng chiết khấu — không đưa vào phiếu");

  const lkind = lineKindOf(l.name);
  const codeKey = compact(l.code);
  const top = (pool: Ix[], n = 3) =>
    pool
      .map((x) => ({ x, s: score(l, x, lkind) }))
      .sort((a, b) => b.s - a.s)
      .slice(0, n)
      .map((r) => toCand(r.x, r.s));
  const near = () => top(cat.ix).filter((c) => c.score >= 0.55);
  // có điểm nghi vấn (số lượng / giá) -> không bao giờ cho 🟢, vẫn gợi ý mã
  const sure = (x: Ix, reason: string): MatchResult =>
    l.flags.length ? res("check", x.it.id, `${reason} · cần kiểm tra số liệu`, [toCand(x, 2)]) : res("ok", x.it.id, reason, [toCand(x, 2)]);

  // 0) file mẫu có ID hệ thống -> chính xác tuyệt đối
  if (l.id) {
    const x = cat.byId.get(l.id);
    if (x) return sure(x, "Theo ID trong file mẫu");
    return res("check", null, "ID trong file mẫu không còn trong danh mục (hàng đã bị xoá?)", near());
  }

  // 1) đã xác nhận ở lần nhập trước
  const aId = alias.get(rawKey);
  if (aId && cat.byId.has(aId)) return sure(cat.byId.get(aId)!, "Đã xác nhận ở lần nhập trước");

  // 2) trùng tên (bỏ phần [mã] của tên CRM)
  const ln = plain(baseName(l.name));
  const sameName = (cat.byBase.get(ln) || []).concat(cat.ix.filter((x) => plain(x.it.name) === plain(`${l.name} ${l.code}`)));
  const uniq = [...new Map(sameName.map((x) => [x.it.id, x])).values()];
  if (uniq.length) {
    const withCode = codeKey ? uniq.filter((x) => x.skuKey === codeKey) : uniq;
    if (withCode.length === 1) return sure(withCode[0], codeKey ? "Trùng mã + trùng tên" : "Trùng tên");
    if (uniq.length === 1 && codeKey) return res("check", uniq[0].it.id, `Trùng tên nhưng mã khác (file: ${l.code}, CRM: ${uniq[0].it.sku})`, top(uniq.concat(cat.bySku.get(codeKey) || [])));
    return res("check", null, `${uniq.length} mã hàng cùng tên — chọn đúng mã`, top(uniq));
  }

  // 3) cùng mã (họ mã) cùng loại
  const fam = (codeKey ? cat.bySku.get(codeKey) || [] : []).filter((x) => x.it.kind === lkind);
  if (fam.length) {
    if (lkind === "MACHINE") {
      if (fam.length === 1) return sure(fam[0], "Trùng mã máy");
      return res("check", null, `${fam.length} máy đang dùng chung mã ${l.code} — chọn đúng máy`, top(fam));
    }
    const fits = fam.filter((x) => nameFits(l, x));
    if (fits.length === 1) return sure(fits[0], `Đúng mã ${l.code} + đủ tên (trong ${fam.length} linh kiện cùng mã)`);
    const why = fits.length > 1 ? `${fits.length} linh kiện mã ${l.code} cùng khớp tên — chọn đúng món` : `${fam.length} linh kiện mã ${l.code}, chưa món nào đủ tên — chọn hoặc tạo mới`;
    const pool = fam.concat(near().map((c) => cat.byId.get(c.itemId)!).filter(Boolean));
    return res("check", null, why, top([...new Map(pool.map((x) => [x.it.id, x])).values()]));
  }

  // 4) không trùng mã / tên: chỉ gợi ý, không tự khớp
  const cands = near();
  if (cands.length && cands[0].score >= 0.8) return res("check", null, "Chỉ gần giống — chọn đúng mã hoặc tạo mới", cands);
  return res("new", null, codeKey ? `Mã ${l.code} chưa có trong danh mục (${lkind === "MACHINE" ? "máy" : "linh kiện"})` : "Chưa có trong danh mục", cands);
}
