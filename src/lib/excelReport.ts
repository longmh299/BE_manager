// src/lib/excelReport.ts — ✅ MỚI: khuôn Excel dùng chung cho các file xuất ra
// Tiêu đề + dòng phụ (giờ xuất, người xuất, bộ lọc) + hàng tiêu đề cột cố định khi cuộn, có nút lọc,
// số định dạng 1,250,000 (vẫn là SỐ THẬT để Excel cộng được), dòng tổng dùng công thức, in ngang vừa khổ A4.
// CHỈ lo phần trình bày — dữ liệu truyền vào giữ nguyên.
import ExcelJS from "exceljs";
import type { Response } from "express";

export const BRAND = {
  navy: "FF1E3A5F", // nền hàng tiêu đề cột
  title: "FF0F2A4A",
  muted: "FF64748B",
  zebra: "FFF8FAFC",
  line: "FFE2E8F0",
  totalBg: "FFEFF4FA",
  red: "FFDC2626",
  font: "Arial",
};

export type ColType = "text" | "center" | "money" | "qty" | "int";

export type ReportCol = {
  header: string;
  key: string;
  width?: number;
  type?: ColType;
  /** cộng tổng ở dòng TỔNG CỘNG */
  total?: boolean;
  /** tô đỏ khi giá trị = 0 (vd tồn = 0) */
  redIfZero?: boolean;
  /** tô đỏ khi giá trị < 0 (vd chênh lệch âm) */
  redIfNegative?: boolean;
  wrap?: boolean;
};

export type ReportOpts = {
  sheetName: string;
  /** Không có title -> tiêu đề cột nằm ngay dòng 1 (dùng cho file cần NHẬP LẠI vào hệ thống) */
  title?: string;
  subtitle?: string;
  columns: ReportCol[];
  rows: Record<string, any>[];
  /** thêm cột STT ở đầu */
  stt?: boolean;
  /** có dòng TỔNG CỘNG (khi có cột total) */
  totals?: boolean;
  landscape?: boolean;
  /** sheet phụ dạng 2 cột (vd "Hướng dẫn"), đặt SAU sheet dữ liệu */
  notes?: { sheetName: string; lines: [string, string][] };
};

/** Số từ Decimal / chuỗi "1,234" / null -> number (rỗng giữ null để ô trống) */
export function num(v: any): number | null {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  const n = Number(typeof v === "object" && typeof v.toString === "function" ? v.toString() : String(v).replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

const NUMFMT: Partial<Record<ColType, string>> = { money: "#,##0", qty: "#,##0.###", int: "#,##0" };

/** "16:30 20/10/2026" theo giờ Việt Nam (server Render chạy giờ UTC) */
export function vnNow(d = new Date()) {
  const p = new Intl.DateTimeFormat("vi-VN", {
    timeZone: "Asia/Ho_Chi_Minh",
    hour: "2-digit",
    minute: "2-digit",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour12: false,
  }).formatToParts(d);
  const g = (t: string) => p.find((x) => x.type === t)?.value ?? "";
  return `${g("hour")}:${g("minute")} ${g("day")}/${g("month")}/${g("year")}`;
}

/** Dòng phụ: "CÔNG TY MCBROTHER · Xuất lúc ... · Người xuất: ... · <thêm>" */
export function exportSubtitle(user?: { username?: string } | null, ...extra: (string | false | null | undefined)[]) {
  return ["CÔNG TY MCBROTHER", `Xuất lúc ${vnNow()}`, user?.username ? `Người xuất: ${user.username}` : "", ...extra]
    .filter(Boolean)
    .join("  ·  ");
}

export const KIND_VN: Record<string, string> = { PART: "Phụ tùng", MACHINE: "Máy" };

export async function buildReport(o: ReportOpts): Promise<Buffer> {
  const wb = new ExcelJS.Workbook();
  wb.creator = "MCBROTHER";
  const cols: ReportCol[] = [...(o.stt ? [{ header: "STT", key: "__stt", width: 6, type: "center" as ColType }] : []), ...o.columns];
  const headRow = o.title ? 4 : 1;
  const ws = wb.addWorksheet(o.sheetName.slice(0, 31), {
    views: [{ state: "frozen", ySplit: headRow }],
    pageSetup: {
      paperSize: 9, // A4
      orientation: o.landscape === false ? "portrait" : "landscape",
      fitToPage: true,
      fitToWidth: 1,
      fitToHeight: 0,
      margins: { left: 0.4, right: 0.4, top: 0.5, bottom: 0.5, header: 0.2, footer: 0.2 },
      printTitlesRow: `${headRow}:${headRow}`,
    },
    headerFooter: { oddFooter: "&L&8MCBROTHER&R&8Trang &P / &N" },
  });
  ws.columns = cols.map((c) => ({ key: c.key, width: c.width ?? 14 }));

  if (o.title) {
    ws.mergeCells(1, 1, 1, cols.length);
    const t = ws.getCell(1, 1);
    t.value = o.title;
    t.font = { name: BRAND.font, size: 15, bold: true, color: { argb: BRAND.title } };
    t.alignment = { vertical: "middle" };
    ws.getRow(1).height = 26;
    if (o.subtitle) {
      ws.mergeCells(2, 1, 2, cols.length);
      const s = ws.getCell(2, 1);
      s.value = o.subtitle;
      s.font = { name: BRAND.font, size: 9, italic: true, color: { argb: BRAND.muted } };
    }
  }

  const h = ws.getRow(headRow);
  cols.forEach((c, i) => (h.getCell(i + 1).value = c.header));
  h.height = 22;
  h.eachCell((cell) => {
    cell.font = { name: BRAND.font, size: 10, bold: true, color: { argb: "FFFFFFFF" } };
    cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: BRAND.navy } };
    cell.alignment = { vertical: "middle", horizontal: "center", wrapText: true };
  });

  o.rows.forEach((r, idx) => {
    const row = ws.getRow(headRow + 1 + idx);
    cols.forEach((c, ci) => {
      const cell = row.getCell(ci + 1);
      const raw = c.key === "__stt" ? idx + 1 : r[c.key];
      const isNum = c.type === "money" || c.type === "qty" || c.type === "int";
      const v = isNum ? num(raw) : raw ?? "";
      cell.value = v as any;
      const red = isNum && v !== null && ((c.redIfZero && v === 0) || (c.redIfNegative && (v as number) < 0));
      cell.font = { name: BRAND.font, size: 10, ...(red ? { color: { argb: BRAND.red } } : {}) };
      cell.alignment = {
        vertical: "middle",
        horizontal: c.type === "center" ? "center" : isNum ? "right" : "left",
        wrapText: !!c.wrap,
        ...(c.type === "center" ? {} : { indent: 1 }), // chừa lề để chữ / số không dính cột bên cạnh
      };
      if (isNum && NUMFMT[c.type!]) cell.numFmt = NUMFMT[c.type!]!;
      cell.border = { bottom: { style: "thin", color: { argb: BRAND.line } } };
      if (idx % 2) cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: BRAND.zebra } };
    });
  });

  const first = headRow + 1;
  const last = headRow + o.rows.length;
  if (o.totals && o.rows.length && cols.some((c) => c.total)) {
    const tr = ws.getRow(last + 1);
    const labelCol = Math.max(1, cols.findIndex((c) => c.type !== "center" && c.key !== "__stt") + 1);
    cols.forEach((c, ci) => {
      const cell = tr.getCell(ci + 1);
      if (ci + 1 === labelCol) cell.value = "TỔNG CỘNG";
      if (c.total) {
        const L = ws.getColumn(ci + 1).letter;
        cell.value = { formula: `SUBTOTAL(9,${L}${first}:${L}${last})` } as any; // SUBTOTAL: lọc bớt dòng thì tổng tự tính lại
        if (NUMFMT[c.type!]) cell.numFmt = NUMFMT[c.type!]!;
        cell.alignment = { horizontal: "right" };
      }
      cell.font = { name: BRAND.font, size: 10, bold: true };
      cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: BRAND.totalBg } };
      cell.border = { top: { style: "medium", color: { argb: BRAND.navy } } };
    });
  }
  if (o.rows.length) ws.autoFilter = { from: { row: headRow, column: 1 }, to: { row: last, column: cols.length } };

  if (o.notes) {
    const ns = wb.addWorksheet(o.notes.sheetName.slice(0, 31));
    ns.columns = [{ width: 24 }, { width: 90 }];
    o.notes.lines.forEach(([a, b], i) => {
      const r = ns.getRow(i + 1);
      r.getCell(1).value = a;
      r.getCell(2).value = b;
      r.getCell(1).font = { name: BRAND.font, size: 10, bold: true, color: { argb: BRAND.title } };
      r.getCell(2).font = { name: BRAND.font, size: 10 };
      r.getCell(2).alignment = { wrapText: true, vertical: "top" };
    });
  }

  return Buffer.from(await wb.xlsx.writeBuffer());
}

/** Gửi file .xlsx, tên file tiếng Việt có dấu vẫn hiện đúng khi tải về */
export function sendXlsx(res: Response, buf: Buffer, filename: string) {
  const ascii = filename
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/đ/g, "d")
    .replace(/Đ/g, "D")
    .replace(/[^\w.\-]+/g, "_");
  res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
  res.setHeader("Content-Disposition", `attachment; filename="${ascii}"; filename*=UTF-8''${encodeURIComponent(filename)}`);
  res.send(buf);
}

/** "ton-kho_20261020.xlsx" */
export function stampName(base: string) {
  const p = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Ho_Chi_Minh", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date());
  return `${base}_${p.replace(/-/g, "")}.xlsx`;
}
