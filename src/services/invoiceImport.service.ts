// src/services/invoiceImport.service.ts
// ✅ MỚI — Nhập hoá đơn từ file -> đổ dòng vào form Nhập / Bán (KHÔNG lưu hoá đơn, KHÔNG đụng tồn kho).
//    Tồn kho + giá vốn chỉ thay đổi khi hoá đơn được DUYỆT bằng đúng luồng cũ.
//
//  POST analyze : đọc file (.xml HĐĐT / .xlsx container / file mẫu) -> dòng 🟢🟡🔵 + gợi ý + đối soát
//  POST confirm : sau khi LƯU phiếu thành công -> ghi nhật ký file + ghi nhớ các lựa chọn (kế toán/admin)
//  GET  template: xuất file mẫu có cột ID hệ thống (ẩn) -> nhập theo ID, đúng 100%
import crypto from "crypto";
import ExcelJS from "exceljs";
import { PrismaClient } from "@prisma/client";
import {
  buildIndex,
  CatalogItem,
  Cand,
  httpError,
  matchLine,
  normTax,
  parseEInvoiceXml,
  parseExcel,
  Parsed,

  rawKeyOf,
} from "./invoiceImport.match";

const prisma = new PrismaClient();
const MODEL = process.env.INVOICE_IMPORT_MODEL || "claude-haiku-5-5";
const OUR_TAX = () => normTax(process.env.COMPANY_TAX_CODE || "");
const fmt = (n: number) => Math.round(n).toLocaleString("vi-VN");

type Actor = { id?: string; role?: string } | undefined;
const canTeach = (a: Actor) => ["admin", "accountant"].includes(String(a?.role || "").toLowerCase());

// ------------------------------------------------------------ danh mục + tồn
async function loadCatalog(): Promise<CatalogItem[]> {
  const [rows, stocks] = await Promise.all([
    prisma.item.findMany({
      select: { id: true, sku: true, name: true, kind: true, sellPrice: true, unit: { select: { code: true, name: true } } },
    }),
    prisma.stock.groupBy({ by: ["itemId"], _sum: { qty: true } }),
  ]);
  const st = new Map(stocks.map((s) => [s.itemId, Number(s._sum.qty || 0)]));
  return rows.map((r) => ({
    id: r.id,
    sku: r.sku || "",
    name: r.name,
    kind: String(r.kind),
    unit: r.unit?.name || r.unit?.code || "",
    price: Number(r.sellPrice || 0),
    stockQty: st.get(r.id) || 0,
  }));
}

// ------------------------------------------------------------ AI: CHỈ sắp lại thứ tự gợi ý của dòng 🟡 (không bao giờ tự xác nhận)
async function aiRank(rows: { key: number; name: string; code: string; cands: Cand[] }[]) {
  const key = process.env.ANTHROPIC_API_KEY;
  const out = new Map<number, string>();
  if (!key || !rows.length) return out;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 20_000);
  try {
    const prompt = rows
      .map((r) => `Dòng ${r.key}: "${r.name}"${r.code ? ` (mã ${r.code})` : ""}\n` + r.cands.map((c) => `  - id=${c.itemId} | mã ${c.sku} | ${c.name}`).join("\n"))
      .join("\n\n");
    const res = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      signal: ctrl.signal,
      headers: { "content-type": "application/json", "x-api-key": key, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({
        model: MODEL,
        max_tokens: 3000,
        system:
          "Bạn khớp dòng hàng trên chứng từ nhập hàng với danh mục của công ty bán máy chế biến & đóng gói. " +
          "Mỗi dòng chọn 1 id GIỐNG NHẤT (cùng loại: máy khác linh kiện; cùng thông số: dài≠ngắn, 2mm≠3mm). Không có id nào đúng thì để rỗng.",
        output_config: {
          format: {
            type: "json_schema",
            schema: {
              type: "object",
              properties: {
                picks: {
                  type: "array",
                  items: {
                    type: "object",
                    properties: { line: { type: "integer" }, itemId: { type: "string" } },
                    required: ["line", "itemId"],
                    additionalProperties: false,
                  },
                },
              },
              required: ["picks"],
              additionalProperties: false,
            },
          },
          effort: "low",
        },
        messages: [{ role: "user", content: [{ type: "text", text: prompt }] }],
      }),
    });
    if (!res.ok) return out;
    const data: any = await res.json();
    const j = JSON.parse((data.content || []).filter((c: any) => c.type === "text").map((c: any) => c.text).join(""));
    for (const p of j.picks || []) if (p?.itemId) out.set(Number(p.line), String(p.itemId));
  } catch {
    /* AI lỗi / quá giờ -> giữ thứ tự theo điểm */
  } finally {
    clearTimeout(timer);
  }
  return out;
}

// ------------------------------------------------------------ đối tác (chỉ có trong XML HĐĐT)
async function matchPartner(parsed: Parsed, isPurchase: boolean, critical: string[], warnings: string[]) {
  let src = isPurchase ? parsed.seller : parsed.buyer;
  const other = isPurchase ? parsed.buyer : parsed.seller;
  const ours = (t: string) => !!t && !!OUR_TAX() && t.slice(0, 10) === OUR_TAX().slice(0, 10);
  if (OUR_TAX() && ours(src.taxCode) && !ours(other.taxCode)) {
    critical.push(`Trong file, công ty mình là bên ${isPurchase ? "BÁN" : "MUA"} — có mở nhầm form ${isPurchase ? "NHẬP" : "BÁN"} không?`);
    src = other;
  }
  if (!src.taxCode && !src.name) return { data: src, match: null as any };
  let match: any = null;
  if (src.taxCode) {
    const cands = await prisma.partner.findMany({ where: { taxCode: { contains: src.taxCode.replace(/-/g, "").slice(0, 4) } }, take: 200 });
    const same = cands.filter((c) => normTax(c.taxCode || "") === src.taxCode);
    if (same.length) {
      match = same[0];
      if (same.length > 1) warnings.push(`Có ${same.length} đối tác cùng MST ${src.taxCode} trong CRM — đang gợi ý ${same[0].code || same[0].name}.`);
    }
  }
  return {
    data: src,
    match: match ? { id: match.id, code: match.code, name: match.name, taxCode: match.taxCode, phone: match.phone, address: match.address } : null,
  };
}

// ============================================================ ANALYZE
export async function analyzeImport(input: { buffer: Buffer; filename: string; type: string; partnerId?: string; actor?: Actor }) {
  const type = String(input.type || "").toUpperCase();
  if (type !== "PURCHASE" && type !== "SALES") throw httpError(400, "Chỉ nhập từ file cho hoá đơn Nhập và Bán");
  const isPurchase = type === "PURCHASE";
  const ext = (input.filename.split(".").pop() || "").toLowerCase();

  let parsed: Parsed;
  if (ext === "xml") parsed = parseEInvoiceXml(input.buffer.toString("utf8"));
  else if (["xlsx", "xls", "xlsm", "csv"].includes(ext)) parsed = parseExcel(input.buffer);
  else throw httpError(400, "Chỉ nhận file .xml (hoá đơn điện tử) hoặc Excel .xlsx / .xls / .csv");
  if (!parsed.lines.length) throw httpError(422, "Không tìm thấy dòng hàng nào trong file");

  const critical = [...parsed.critical];
  const warnings = [...parsed.notes];
  const fileHash = crypto.createHash("sha256").update(input.buffer).digest("hex");

  // ---- chống nhập trùng: cùng file, hoặc cùng HĐĐT (ký hiệu + số + MST)
  const prev = await prisma.invoiceImportLog.findFirst({
    where: { OR: [{ fileHash }, ...(parsed.invoiceKey ? [{ invoiceKey: parsed.invoiceKey }] : [])] },
    orderBy: { createdAt: "desc" },
  });
  let duplicate: { invoiceId: string | null; code: string | null; at: Date } | null = null;
  if (prev) {
    const inv = prev.invoiceId ? await prisma.invoice.findUnique({ where: { id: prev.invoiceId }, select: { id: true, code: true } }) : null;
    if (inv || !prev.invoiceId) {
      duplicate = { invoiceId: inv?.id || null, code: inv?.code || null, at: prev.createdAt };
      critical.push(
        `${parsed.invoiceKey && prev.invoiceKey === parsed.invoiceKey ? `Hoá đơn ${parsed.invoiceNo}` : "File này"} ĐÃ ĐƯỢC NHẬP` +
          `${inv?.code ? ` ở phiếu ${inv.code}` : ""} (${prev.createdAt.toLocaleDateString("vi-VN")}). Nhập lại sẽ cộng trùng tồn kho và công nợ.`
      );
    }
  }

  // ---- khớp hàng
  const catalog = await loadCatalog();
  const cat = buildIndex(catalog);
  const supplierKey = String(input.partnerId || "");
  const keys = [...new Set(parsed.lines.map((l) => rawKeyOf(l.code, l.name)))];
  const aliasRows = await prisma.itemAlias.findMany({ where: { rawKey: { in: keys }, supplierKey: { in: supplierKey ? [supplierKey, ""] : [""] } } });
  const alias = new Map<string, string>();
  aliasRows.sort((a, b) => (a.supplierKey === "" ? -1 : 1) - (b.supplierKey === "" ? -1 : 1)).forEach((a) => alias.set(a.rawKey, a.itemId)); // NCC riêng ghi đè dùng chung

  const results = parsed.lines.map((l) => ({ l, m: matchLine(l, cat, alias) }));

  // AI chỉ sắp lại gợi ý cho dòng 🟡 (người dùng vẫn phải chọn)
  const yellow = results.filter((r) => r.m.status === "check" && !r.m.itemId && r.m.candidates.length > 1);
  const ai = await aiRank(yellow.map((r, i) => ({ key: i + 1, name: r.l.name, code: r.l.code, cands: r.m.candidates })));
  yellow.forEach((r, i) => {
    const id = ai.get(i + 1);
    const k = r.m.candidates.findIndex((c) => c.itemId === id);
    if (k > 0) r.m.candidates.unshift(...r.m.candidates.splice(k, 1));
    if (k >= 0) r.m.reason += " · AI xếp gợi ý đầu";
  });

  // ---- đối soát tổng
  const goods = parsed.lines.filter((l) => l.kind !== "chiet_khau");
  const linesSum = goods.reduce((s, l) => s + l.qty * l.price, 0);
  const discount = parsed.lines.filter((l) => l.kind === "chiet_khau").reduce((s, l) => s + l.amount, 0);
  let totalOk: boolean | null = null;
  if (parsed.fileTotal != null) {
    const tol = Math.max(10, Math.abs(parsed.fileTotal) * 0.0005);
    totalOk = Math.abs(linesSum + discount - parsed.fileTotal) <= tol || Math.abs(linesSum - parsed.fileTotal) <= tol;
    if (!totalOk)
      critical.push(`Cộng các dòng = ${fmt(linesSum + discount)} đ nhưng tổng ghi trong file = ${fmt(parsed.fileTotal)} đ (lệch ${fmt(linesSum + discount - parsed.fileTotal)} đ).`);
  } else {
    warnings.push(`File không có dòng tổng để đối soát — kiểm tra lại đủ ${goods.length} dòng, tổng ${fmt(linesSum)} đ so với chứng từ gốc.`);
  }
  if (discount) warnings.push(`Hoá đơn có chiết khấu thương mại ${fmt(-discount)} đ — không đưa vào phiếu, hạch toán tay nếu cần.`);
  if (parsed.skipped) warnings.push(`${parsed.skipped} dòng trong file mẫu để trống số lượng → bỏ qua.`);

  const partner = parsed.source === "xml" ? await matchPartner(parsed, isPurchase, critical, warnings) : null;

  const count = { ok: 0, check: 0, new: 0, skip: 0 } as Record<string, number>;
  results.forEach((r) => count[r.m.status]++);

  return {
    fileName: input.filename,
    fileHash,
    source: parsed.source,
    invoiceNo: parsed.invoiceNo,
    invoiceKey: parsed.invoiceKey,
    date: parsed.date,
    canTeach: canTeach(input.actor),
    lines: results.map(({ l, m }) => ({
      row: l.row,
      code: l.code,
      name: l.name,
      unit: l.unit,
      qty: l.qty,
      price: l.price,
      amount: l.amount,
      kind: l.kind,
      flags: l.flags,
      status: m.status,
      itemId: m.itemId,
      reason: m.reason,
      rawKey: m.rawKey,
      candidates: m.candidates.slice(0, 3),
    })),
    count,
    totals: { fileTotal: parsed.fileTotal, linesSum, discount, totalOk },
    partner,
    duplicate,
    critical,
    warnings,
  };
}

// ============================================================ CONFIRM (gọi SAU khi lưu phiếu thành công)
export async function confirmImport(
  body: {
    invoiceId?: string;
    fileHash?: string;
    invoiceKey?: string;
    fileName?: string;
    type?: string;
    partnerId?: string;
    picks?: { rawKey: string; rawText: string; itemId: string }[];
  },
  actor?: Actor
) {
  const fileHash = String(body.fileHash || "");
  if (!/^[a-f0-9]{64}$/.test(fileHash)) throw httpError(400, "Thiếu fileHash");
  await prisma.invoiceImportLog.create({
    data: {
      fileHash,
      invoiceKey: String(body.invoiceKey || ""),
      fileName: String(body.fileName || "").slice(0, 255),
      type: String(body.type || ""),
      invoiceId: body.invoiceId ? String(body.invoiceId) : null,
      createdById: actor?.id ? String(actor.id) : null,
    },
  });

  // chỉ kế toán / admin mới "dạy" hệ thống — tránh 1 lần chọn nhầm của người khác lan sang các lần nhập sau
  let taught = 0;
  if (canTeach(actor) && Array.isArray(body.picks) && body.picks.length) {
    const picks = body.picks.filter((p) => p && p.rawKey && p.itemId && p.rawKey.includes("|")).slice(0, 500);
    const exist = new Set((await prisma.item.findMany({ where: { id: { in: picks.map((p) => String(p.itemId)) } }, select: { id: true } })).map((x) => x.id));
    const supplierKey = String(body.partnerId || "");
    for (const p of picks) {
      if (!exist.has(String(p.itemId))) continue;
      const rawKey = String(p.rawKey).slice(0, 500);
      await prisma.itemAlias.upsert({
        where: { supplierKey_rawKey: { supplierKey, rawKey } },
        create: { supplierKey, rawKey, rawText: String(p.rawText || "").slice(0, 500), itemId: String(p.itemId), createdById: actor?.id ? String(actor.id) : null },
        update: { itemId: String(p.itemId), hits: { increment: 1 } },
      });
      taught++;
    }
  }
  return { logged: true, taught };
}

// ============================================================ FILE MẪU (có ID hệ thống, cột ẩn)
export async function buildTemplate(kind: string) {
  const k = String(kind || "ALL").toUpperCase();
  const items = (await loadCatalog())
    .filter((i) => k === "ALL" || i.kind === k)
    .sort((a, b) => (a.kind === b.kind ? (a.sku || "").localeCompare(b.sku || "") || a.name.localeCompare(b.name) : a.kind === "MACHINE" ? -1 : 1));

  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet("Nhap hang");
  ws.columns = [
    { header: "ID hệ thống", key: "id", width: 28 },
    { header: "Mã", key: "sku", width: 18 },
    { header: "Tên hàng", key: "name", width: 60 },
    { header: "ĐVT", key: "unit", width: 8 },
    { header: "Loại", key: "kind", width: 10 },
    { header: "Tồn hiện tại", key: "stock", width: 12 },
    { header: "Số lượng", key: "qty", width: 12 },
    { header: "Đơn giá", key: "price", width: 16 },
  ];
  ws.getColumn(1).hidden = true; // ID: không sửa, không xoá
  const head = ws.getRow(1);
  head.font = { bold: true, name: "Arial" };
  head.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFE2E8F0" } };
  ws.views = [{ state: "frozen", ySplit: 1 }];
  ws.autoFilter = { from: "A1", to: "H1" };
  for (const it of items)
    ws.addRow({ id: it.id, sku: it.sku, name: it.name, unit: it.unit, kind: it.kind === "MACHINE" ? "Máy" : "Linh kiện", stock: it.stockQty, qty: null, price: null });
  ws.getColumn(7).numFmt = "#,##0.###";
  ws.getColumn(8).numFmt = "#,##0";
  for (let r = 2; r <= items.length + 1; r++) {
    ws.getCell(r, 7).fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFFFF9C4" } };
    ws.getCell(r, 8).fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFFFF9C4" } };
  }
  ws.eachRow((row) => (row.font = { ...(row.font || {}), name: "Arial" }));

  const guide = wb.addWorksheet("Huong dan");
  [
    "CÁCH DÙNG FILE MẪU NHẬP HÀNG",
    "1. Chỉ điền 2 cột tô vàng: Số lượng và Đơn giá (giá nhập, chưa VAT).",
    "2. Dòng nào không nhập thì để trống Số lượng — hệ thống tự bỏ qua.",
    "3. KHÔNG sửa / xoá cột ID hệ thống (cột A đang ẩn) — nhờ cột này hệ thống khớp đúng 100% kể cả khi nhiều hàng chung mã.",
    "4. Hàng chưa có trong danh sách: thêm dòng mới ở cuối, để trống ID, ghi Mã + Tên + Số lượng + Đơn giá → khi nhập sẽ hiện 🔵 để tạo mã mới.",
    "5. Lên trang hoá đơn Nhập → bấm “Nhập từ file” → chọn file này.",
  ].forEach((t, i) => {
    const c = guide.getCell(i + 1, 1);
    c.value = t;
    c.font = { name: "Arial", bold: i === 0 };
  });
  guide.getColumn(1).width = 120;
  return Buffer.from(await wb.xlsx.writeBuffer());
}


