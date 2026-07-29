// src/services/reportMailer.service.ts
//
// Gửi báo cáo bán hàng qua email theo chuẩn báo cáo doanh nghiệp:
// - Hàng tháng: KPI tổng quan (có so sánh % với tháng trước) + top sản phẩm bán
//   chạy + danh sách cần nhập thêm, kèm 3 file Excel đính kèm (bảng kê bán hàng
//   chi tiết, xuất nhập tồn kho, cần nhập thêm tách 2 tab Máy/Linh kiện).
// - Hàng quý: KPI tổng hợp doanh thu (có so sánh % với quý trước), không đính kèm.
//
// Không tự viết lại logic tính doanh thu/giá vốn/xuất-nhập-tồn — dùng lại đúng các
// service đã có trong reports.service.ts và stockInOutReport.service.ts để đảm bảo
// số liệu khớp 100% với các trang báo cáo hiện có trên web.

import nodemailer from "nodemailer";
import ExcelJS from "exceljs";
import { getSalesLedger, exportSalesLedgerExcel } from "./reports.service";
import { getStockInOutReport } from "./stockInOutReport.service";
import { buildLowStockAlerts } from "./assistant/alerts/lowStock.service";
import { prisma } from "../tool/prisma";

const COMPANY_NAME = "MCBROTHER";
const TOP_PRODUCTS_LIMIT = 10;
const REORDER_LIMIT = 30;

function pad2(n: number) {
  return String(n).padStart(2, "0");
}

function fmtMoney(n: number) {
  return Math.round(n).toLocaleString("vi-VN");
}

function pctChange(cur: number, prev: number): number | null {
  if (!prev) return null;
  return ((cur - prev) / prev) * 100;
}

/** Khoảng ngày của THÁNG TRƯỚC (theo giờ UTC — đủ chính xác cho mục đích báo cáo) */
function prevMonthRange(now = new Date()) {
  const y = now.getUTCFullYear();
  const m = now.getUTCMonth(); // 0-based, tháng hiện tại
  const from = new Date(Date.UTC(y, m - 1, 1, 0, 0, 0, 0));
  const to = new Date(Date.UTC(y, m, 0, 23, 59, 59, 999)); // ngày cuối tháng trước
  const label = `Tháng ${from.getUTCMonth() + 1}/${from.getUTCFullYear()}`;
  const fileLabel = `${from.getUTCFullYear()}-${pad2(from.getUTCMonth() + 1)}`;
  return { from, to, label, fileLabel };
}

/** Dịch lùi thêm N tháng so với 1 khoảng [from,to] đã có — dùng để lấy kỳ liền trước để so sánh */
function shiftMonthRangeBack(from: Date, monthsBack: number) {
  const newFrom = new Date(Date.UTC(from.getUTCFullYear(), from.getUTCMonth() - monthsBack, 1, 0, 0, 0, 0));
  const newTo = new Date(Date.UTC(newFrom.getUTCFullYear(), newFrom.getUTCMonth() + 1, 0, 23, 59, 59, 999));
  return { from: newFrom, to: newTo };
}

/** Khoảng ngày của QUÝ TRƯỚC */
function prevQuarterRange(now = new Date()) {
  const y = now.getUTCFullYear();
  const curQ = Math.floor(now.getUTCMonth() / 3); // 0..3
  const prevQIdx = curQ - 1;
  const targetYear = prevQIdx < 0 ? y - 1 : y;
  const targetQ = prevQIdx < 0 ? 3 : prevQIdx;
  const startMonth = targetQ * 3;
  const from = new Date(Date.UTC(targetYear, startMonth, 1, 0, 0, 0, 0));
  const to = new Date(Date.UTC(targetYear, startMonth + 3, 0, 23, 59, 59, 999));
  const label = `Quý ${targetQ + 1}/${targetYear}`;
  return { from, to, label };
}

/** Dịch lùi thêm 1 quý so với 1 khoảng [from,to] đã có */
function shiftQuarterRangeBack(from: Date) {
  const newFrom = new Date(Date.UTC(from.getUTCFullYear(), from.getUTCMonth() - 3, 1, 0, 0, 0, 0));
  const newTo = new Date(Date.UTC(newFrom.getUTCFullYear(), newFrom.getUTCMonth() + 3, 0, 23, 59, 59, 999));
  return { from: newFrom, to: newTo };
}

function getTransport() {
  const host = process.env.SMTP_HOST;
  const port = Number(process.env.SMTP_PORT || 465);
  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;

  if (!host || !user || !pass) {
    throw new Error(
      "Thiếu cấu hình SMTP (SMTP_HOST/SMTP_USER/SMTP_PASS) trong biến môi trường."
    );
  }

  return nodemailer.createTransport({
    host,
    port,
    secure: String(process.env.SMTP_SECURE ?? "1") !== "0", // true = SSL (port 465)
    auth: { user, pass },
  });
}

function getRecipients(): string[] {
  const raw = process.env.REPORT_EMAIL_TO || "";
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

async function sendMail(params: {
  subject: string;
  html: string;
  attachments?: { filename: string; content: Buffer }[];
}) {
  const to = getRecipients();
  if (to.length === 0) {
    throw new Error("Thiếu REPORT_EMAIL_TO (danh sách email nhận báo cáo) trong biến môi trường.");
  }

  const transport = getTransport();
  await transport.sendMail({
    from: `"${COMPANY_NAME}" <${process.env.SMTP_FROM || process.env.SMTP_USER}>`,
    to: to.join(","),
    subject: params.subject,
    html: params.html,
    attachments: params.attachments,
  });
}

// ---------------------------------------------------------------------------
// Email template kiểu "báo cáo doanh nghiệp": letterhead + thẻ KPI có so sánh kỳ
// trước + bảng số liệu. Dùng inline CSS để hiển thị đúng trên mọi trình đọc mail.
// ---------------------------------------------------------------------------

const FONT = "Segoe UI,Arial,sans-serif";

type Delta = { color: string; arrow: string; text: string };

function deltaBadge(pct: number | null, label = "so với kỳ trước"): Delta | undefined {
  if (pct === null) return undefined;
  const up = pct >= 0;
  return {
    color: up ? "#16a34a" : "#dc2626",
    arrow: up ? "▲" : "▼",
    text: `${Math.abs(pct).toFixed(1)}% ${label}`,
  };
}

function kpiCard(label: string, value: string, delta?: Delta) {
  return `
    <td style="padding:8px;width:50%;vertical-align:top;">
      <div style="border:1px solid #e2e8f0;border-radius:10px;padding:16px 18px;">
        <div style="color:#94a3b8;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;font-family:${FONT};">${label}</div>
        <div style="color:#0f172a;font-size:21px;font-weight:700;margin-top:6px;font-family:${FONT};">${value}</div>
        ${
          delta
            ? `<div style="margin-top:6px;font-size:12px;font-weight:600;color:${delta.color};font-family:${FONT};">${delta.arrow} ${delta.text}</div>`
            : `<div style="margin-top:6px;font-size:12px;color:#cbd5e1;font-family:${FONT};">—</div>`
        }
      </div>
    </td>`;
}

function renderKpiGrid(cards: Array<{ label: string; value: string; delta?: Delta }>) {
  const rows: string[] = [];
  for (let i = 0; i < cards.length; i += 2) {
    const a = cards[i];
    const b = cards[i + 1];
    rows.push(`<tr>${kpiCard(a.label, a.value, a.delta)}${b ? kpiCard(b.label, b.value, b.delta) : "<td></td>"}</tr>`);
  }
  return `<table style="width:100%;border-collapse:collapse;padding:8px 16px;">${rows.join("")}</table>`;
}

function renderSectionTitle(title: string) {
  return `<div style="margin:24px 24px 8px;padding-left:10px;border-left:3px solid #0f172a;font-size:13px;font-weight:700;color:#0f172a;text-transform:uppercase;letter-spacing:0.4px;font-family:${FONT};">${title}</div>`;
}

/** Tránh lặp mã 2 lần khi tên sản phẩm đã tự chứa mã (rất phổ biến trong dữ liệu này, vd "Máy...[JL-660]") */
function formatProductLabel(name: string, sku?: string | null) {
  if (!sku) return name;
  const norm = (s: string) => s.toLowerCase().replace(/[^a-z0-9]/g, "");
  if (norm(name).includes(norm(sku))) return name;
  return `${name} <span style="color:#94a3b8;font-size:11px;">(${sku})</span>`;
}

type TopProductRow = { name: string; sku: string; qty: number; revenue: number };

function aggregateTopProducts(
  rows: Array<{ itemName: string; itemSku?: string | null; qty: number; lineAmount: number }>,
  topN = TOP_PRODUCTS_LIMIT
): TopProductRow[] {
  const map = new Map<string, TopProductRow>();
  for (const r of rows) {
    const key = (r.itemSku || r.itemName || "").trim() || r.itemName;
    const cur = map.get(key) || { name: r.itemName, sku: r.itemSku || "", qty: 0, revenue: 0 };
    cur.qty += Number(r.qty) || 0;
    cur.revenue += Number(r.lineAmount) || 0;
    map.set(key, cur);
  }
  return Array.from(map.values())
    .sort((a, b) => b.revenue - a.revenue)
    .slice(0, topN);
}

function renderProductTable(items: TopProductRow[]) {
  if (items.length === 0) {
    return `<div style="padding:8px 24px 16px;color:#94a3b8;font-size:13px;font-family:${FONT};">Không có dữ liệu bán hàng trong kỳ.</div>`;
  }

  const headHtml = `
    <tr>
      <td style="padding:8px 12px;font-size:11px;color:#94a3b8;text-transform:uppercase;text-align:center;border-bottom:2px solid #e2e8f0;font-family:${FONT};">#</td>
      <td style="padding:8px 24px;font-size:11px;color:#94a3b8;text-transform:uppercase;border-bottom:2px solid #e2e8f0;font-family:${FONT};">Sản phẩm</td>
      <td style="padding:8px 24px;font-size:11px;color:#94a3b8;text-transform:uppercase;text-align:right;border-bottom:2px solid #e2e8f0;font-family:${FONT};">SL bán</td>
      <td style="padding:8px 24px;font-size:11px;color:#94a3b8;text-transform:uppercase;text-align:right;border-bottom:2px solid #e2e8f0;font-family:${FONT};">Doanh thu</td>
    </tr>`;

  const bodyHtml = items
    .map((it, idx) => {
      const bg = idx % 2 === 1 ? "background:#f8fafc;" : "";
      return `
    <tr style="${bg}">
      <td style="padding:9px 12px;font-size:12px;color:#94a3b8;text-align:center;font-family:${FONT};">${idx + 1}</td>
      <td style="padding:9px 24px;font-size:13px;color:#0f172a;font-family:${FONT};">${formatProductLabel(it.name, it.sku)}</td>
      <td style="padding:9px 24px;font-size:13px;color:#0f172a;text-align:right;font-family:${FONT};">${it.qty}</td>
      <td style="padding:9px 24px;font-size:13px;font-weight:600;color:#0f172a;text-align:right;font-family:${FONT};">${fmtMoney(it.revenue)}đ</td>
    </tr>`;
    })
    .join("");

  return `<table style="width:100%;border-collapse:collapse;">${headHtml}${bodyHtml}</table>`;
}

function severityBadge(severity: string) {
  const isCritical = severity === "CRITICAL";
  const bg = isCritical ? "#fee2e2" : "#fef3c7";
  const color = isCritical ? "#dc2626" : "#b45309";
  const label = isCritical ? "HẾT HÀNG" : "SẮP HẾT";
  return `<span style="background:${bg};color:${color};font-size:11px;font-weight:700;padding:2px 8px;border-radius:999px;">${label}</span>`;
}

function renderReorderTable(items: Array<{ sku: string; name: string; qty: number; suggestQty: number; severity: string }>) {
  if (items.length === 0) {
    return `<div style="padding:8px 24px 16px;color:#94a3b8;font-size:13px;font-family:${FONT};">Không có sản phẩm nào cần nhập thêm gấp.</div>`;
  }

  const headHtml = `
    <tr>
      <td style="padding:8px 12px;font-size:11px;color:#94a3b8;text-transform:uppercase;text-align:center;border-bottom:2px solid #e2e8f0;font-family:${FONT};">#</td>
      <td style="padding:8px 24px;font-size:11px;color:#94a3b8;text-transform:uppercase;border-bottom:2px solid #e2e8f0;font-family:${FONT};">Sản phẩm</td>
      <td style="padding:8px 12px;font-size:11px;color:#94a3b8;text-transform:uppercase;text-align:right;border-bottom:2px solid #e2e8f0;font-family:${FONT};">Tồn</td>
      <td style="padding:8px 12px;font-size:11px;color:#94a3b8;text-transform:uppercase;text-align:right;border-bottom:2px solid #e2e8f0;font-family:${FONT};">Đề xuất nhập</td>
      <td style="padding:8px 24px;font-size:11px;color:#94a3b8;text-transform:uppercase;text-align:center;border-bottom:2px solid #e2e8f0;font-family:${FONT};">Mức độ</td>
    </tr>`;

  const bodyHtml = items
    .map((it, idx) => {
      const bg = idx % 2 === 1 ? "background:#f8fafc;" : "";
      return `
    <tr style="${bg}">
      <td style="padding:9px 12px;font-size:12px;color:#94a3b8;text-align:center;font-family:${FONT};">${idx + 1}</td>
      <td style="padding:9px 24px;font-size:13px;color:#0f172a;font-family:${FONT};">${formatProductLabel(it.name, it.sku)}</td>
      <td style="padding:9px 12px;font-size:13px;color:#0f172a;text-align:right;font-family:${FONT};">${it.qty}</td>
      <td style="padding:9px 12px;font-size:13px;font-weight:600;color:#0f172a;text-align:right;font-family:${FONT};">${it.suggestQty}</td>
      <td style="padding:9px 24px;text-align:center;font-family:${FONT};">${severityBadge(it.severity)}</td>
    </tr>`;
    })
    .join("");

  return `<table style="width:100%;border-collapse:collapse;">${headHtml}${bodyHtml}</table>`;
}

function renderEmailShell(params: {
  reportTitle: string;
  reportCode: string;
  periodLabel: string;
  kpiGridHtml: string;
  extraSectionsHtml?: string;
  note?: string;
}) {
  const generatedAt = new Date().toLocaleString("vi-VN", { hour12: false });
  return `
<div style="background:#f1f5f9;padding:32px 16px;font-family:${FONT};">
  <div style="max-width:640px;margin:0 auto;background:#ffffff;border-radius:14px;overflow:hidden;box-shadow:0 1px 4px rgba(15,23,42,0.10);">
    <div style="background:#0f172a;padding:28px 28px 24px;">
      <table style="width:100%;">
        <tr>
          <td style="vertical-align:top;">
            <div style="color:#94a3b8;font-size:11px;letter-spacing:1px;text-transform:uppercase;font-family:${FONT};">${COMPANY_NAME}</div>
            <div style="color:#ffffff;font-size:21px;font-weight:800;margin-top:8px;font-family:${FONT};">${params.reportTitle}</div>
            <div style="color:#cbd5e1;font-size:13px;margin-top:4px;font-family:${FONT};">Kỳ báo cáo: ${params.periodLabel}</div>
          </td>
          <td style="vertical-align:top;text-align:right;">
            <div style="color:#64748b;font-size:10px;text-transform:uppercase;letter-spacing:0.4px;font-family:${FONT};">Mã báo cáo</div>
            <div style="color:#e2e8f0;font-size:13px;font-weight:600;margin-top:4px;font-family:${FONT};">${params.reportCode}</div>
          </td>
        </tr>
      </table>
    </div>

    ${params.kpiGridHtml}
    ${params.extraSectionsHtml || ""}
    ${
      params.note
        ? `<div style="padding:18px 24px 4px;color:#475569;font-size:13px;line-height:1.6;font-family:${FONT};">${params.note}</div>`
        : ""
    }

    <div style="padding:16px 24px;background:#f8fafc;color:#94a3b8;font-size:11px;margin-top:16px;border-top:1px solid #e2e8f0;font-family:${FONT};">
      Báo cáo được tạo tự động lúc ${generatedAt} bởi hệ thống quản lý kho ${COMPANY_NAME}.<br/>
      Tài liệu nội bộ — vui lòng không chia sẻ ra ngoài công ty.
    </div>
  </div>
</div>`;
}

// ---------------------------------------------------------------------------
// Excel xuất-nhập-tồn (báo cáo tháng chưa có sẵn hàm export cho phần này)
// ---------------------------------------------------------------------------

function buildStockInOutWorkbook(
  rows: Array<{ sku: string; name: string; unitCode: string; openingQty: number; inQty: number; outQty: number; closingQty: number }>,
  totals: { openingQty: number; inQty: number; outQty: number; closingQty: number },
  label: string
) {
  const wb = new ExcelJS.Workbook();
  wb.creator = COMPANY_NAME;
  wb.created = new Date();

  const ws = wb.addWorksheet("Xuất nhập tồn", { views: [{ state: "frozen", ySplit: 3 }] });

  const thinBorder = { style: "thin" as const, color: { argb: "FFE2E8F0" } };
  const cellBorder = { top: thinBorder, left: thinBorder, bottom: thinBorder, right: thinBorder };

  ws.mergeCells("A1:G1");
  ws.getCell("A1").value = COMPANY_NAME;
  ws.getCell("A1").font = { size: 11, color: { argb: "FF64748B" } };
  ws.getCell("A1").alignment = { vertical: "middle", horizontal: "center" };

  ws.mergeCells("A2:G2");
  ws.getCell("A2").value = `BÁO CÁO XUẤT NHẬP TỒN KHO — ${label}`;
  ws.getCell("A2").font = { size: 14, bold: true, color: { argb: "FF0F172A" } };
  ws.getCell("A2").alignment = { vertical: "middle", horizontal: "center" };
  ws.getRow(2).height = 24;

  const header = ["Mã", "Tên sản phẩm", "ĐVT", "Tồn đầu kỳ", "Nhập trong kỳ", "Xuất trong kỳ", "Tồn cuối kỳ"];
  ws.addRow(header);
  const headerRow = ws.getRow(3);
  headerRow.font = { bold: true, color: { argb: "FFFFFFFF" } };
  headerRow.alignment = { vertical: "middle", horizontal: "center", wrapText: true };
  headerRow.height = 20;
  headerRow.eachCell((c) => {
    c.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FF1F2937" } };
    c.border = cellBorder;
  });

  ws.autoFilter = "A3:G3";

  rows.forEach((r, idx) => {
    const row = ws.addRow([r.sku, r.name, r.unitCode, r.openingQty, r.inQty, r.outQty, r.closingQty]);
    const isEven = idx % 2 === 1;
    row.eachCell((c, colNumber) => {
      c.border = cellBorder;
      if (isEven) c.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFF8FAFC" } };
      if (colNumber >= 4) {
        c.numFmt = "#,##0";
        c.alignment = { horizontal: "right" };
      }
    });
  });

  const totalRowIdx = ws.rowCount + 1;
  const totalRow = ws.getRow(totalRowIdx);
  ws.getCell(`A${totalRowIdx}`).value = "TỔNG CỘNG";
  ws.mergeCells(`A${totalRowIdx}:C${totalRowIdx}`);
  ws.getCell(`D${totalRowIdx}`).value = totals.openingQty;
  ws.getCell(`E${totalRowIdx}`).value = totals.inQty;
  ws.getCell(`F${totalRowIdx}`).value = totals.outQty;
  ws.getCell(`G${totalRowIdx}`).value = totals.closingQty;
  totalRow.eachCell((c, colNumber) => {
    c.font = { bold: true, color: { argb: "FF0F172A" } };
    c.border = { top: { style: "double", color: { argb: "FF94A3B8" } } };
    c.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFEFF6FF" } };
    if (colNumber >= 4) {
      c.numFmt = "#,##0";
      c.alignment = { horizontal: "right" };
    }
  });

  ws.getColumn(1).width = 14;
  ws.getColumn(2).width = 38;
  ws.getColumn(3).width = 10;
  ws.getColumn(4).width = 14;
  ws.getColumn(5).width = 14;
  ws.getColumn(6).width = 14;
  ws.getColumn(7).width = 14;

  return wb;
}

/**
 * Dựng file Excel "sản phẩm cần nhập thêm", tách riêng 2 tab: Máy / Linh kiện.
 */
function buildReorderWorkbook(
  reorderList: Array<{ sku: string; name: string; qty: number; suggestQty: number; severity: string; kind: string }>,
  label: string
) {
  const wb = new ExcelJS.Workbook();
  wb.creator = COMPANY_NAME;
  wb.created = new Date();

  const thinBorder = { style: "thin" as const, color: { argb: "FFE2E8F0" } };
  const cellBorder = { top: thinBorder, left: thinBorder, bottom: thinBorder, right: thinBorder };

  function addSheet(sheetName: string, items: typeof reorderList) {
    const ws = wb.addWorksheet(sheetName, { views: [{ state: "frozen", ySplit: 3 }] });

    ws.mergeCells("A1:E1");
    ws.getCell("A1").value = COMPANY_NAME;
    ws.getCell("A1").font = { size: 11, color: { argb: "FF64748B" } };
    ws.getCell("A1").alignment = { vertical: "middle", horizontal: "center" };

    ws.mergeCells("A2:E2");
    ws.getCell("A2").value = `SẢN PHẨM CẦN NHẬP THÊM — ${sheetName.toUpperCase()} — ${label}`;
    ws.getCell("A2").font = { size: 13, bold: true, color: { argb: "FF0F172A" } };
    ws.getCell("A2").alignment = { vertical: "middle", horizontal: "center" };
    ws.getRow(2).height = 22;

    const header = ["Mã", "Tên sản phẩm", "Tồn", "Đề xuất nhập", "Mức độ"];
    ws.addRow(header);
    const headerRow = ws.getRow(3);
    headerRow.font = { bold: true, color: { argb: "FFFFFFFF" } };
    headerRow.alignment = { vertical: "middle", horizontal: "center", wrapText: true };
    headerRow.height = 20;
    headerRow.eachCell((c) => {
      c.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FF1F2937" } };
      c.border = cellBorder;
    });
    ws.autoFilter = "A3:E3";

    if (items.length === 0) {
      const row = ws.addRow(["—", "Không có sản phẩm nào cần nhập gấp", "", "", ""]);
      row.font = { italic: true, color: { argb: "FF94A3B8" } };
    } else {
      items.forEach((it, idx) => {
        const severityLabel = it.severity === "CRITICAL" ? "HẾT HÀNG" : "SẮP HẾT";
        const row = ws.addRow([it.sku, it.name, it.qty, it.suggestQty, severityLabel]);
        const isEven = idx % 2 === 1;
        row.eachCell((c, colNumber) => {
          c.border = cellBorder;
          if (isEven) c.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFF8FAFC" } };
          if (colNumber === 3 || colNumber === 4) {
            c.numFmt = "#,##0";
            c.alignment = { horizontal: "right" };
          }
          if (colNumber === 5) {
            c.font = { bold: true, color: { argb: it.severity === "CRITICAL" ? "FFDC2626" : "FFB45309" } };
            c.alignment = { horizontal: "center" };
          }
        });
      });
    }

    ws.getColumn(1).width = 14;
    ws.getColumn(2).width = 40;
    ws.getColumn(3).width = 12;
    ws.getColumn(4).width = 14;
    ws.getColumn(5).width = 14;
  }

  addSheet("Máy", reorderList.filter((x) => x.kind === "MACHINE"));
  addSheet("Linh kiện", reorderList.filter((x) => x.kind === "PART"));

  return wb;
}

/**
 * Gửi báo cáo THÁNG TRƯỚC: KPI tổng quan (so với tháng liền trước) + top sản
 * phẩm bán chạy + danh sách cần nhập thêm, kèm 3 file Excel đính kèm.
 */
export async function sendMonthlyReport(forDate = new Date()) {
  const { from, to, label, fileLabel } = prevMonthRange(forDate);
  const prevRange = shiftMonthRangeBack(from, 1);

  const [{ rows: ledgerRows, totals }, { totals: prevTotals }] = await Promise.all([
    getSalesLedger({ from, to }),
    getSalesLedger({ from: prevRange.from, to: prevRange.to }),
  ]);

  const salesExcelBuffer = await exportSalesLedgerExcel({ from, to });

  const stockReport = await getStockInOutReport({ from, to });
  const stockWb = buildStockInOutWorkbook(stockReport.rows, stockReport.totals, label);
  const stockExcelBuffer = Buffer.from(await stockWb.xlsx.writeBuffer());

  const profit = totals.totalRevenue - totals.totalCost;
  const prevProfit = prevTotals.totalRevenue - prevTotals.totalCost;
  const collectionRate = totals.totalRevenue > 0 ? (totals.totalPaid / totals.totalRevenue) * 100 : 0;

  const kpiGridHtml = renderKpiGrid([
    {
      label: "Doanh thu",
      value: `${fmtMoney(totals.totalRevenue)}đ`,
      delta: deltaBadge(pctChange(totals.totalRevenue, prevTotals.totalRevenue)),
    },
    {
      label: "Giá vốn",
      value: `${fmtMoney(totals.totalCost)}đ`,
      delta: deltaBadge(pctChange(totals.totalCost, prevTotals.totalCost)),
    },
    {
      label: "Lợi nhuận gộp",
      value: `${fmtMoney(profit)}đ`,
      delta: deltaBadge(pctChange(profit, prevProfit)),
    },
    {
      label: "Tỷ lệ thu hồi công nợ",
      value: `${collectionRate.toFixed(1)}%`,
    },
    {
      label: "Đã thu",
      value: `${fmtMoney(totals.totalPaid)}đ`,
    },
    {
      label: "Còn nợ",
      value: `${fmtMoney(totals.totalDebt)}đ`,
    },
  ]);

  const topProducts = aggregateTopProducts(ledgerRows as any);
  const lowStockAlerts = await buildLowStockAlerts(prisma);
  const reorderList = lowStockAlerts.slice(0, REORDER_LIMIT);

  const reorderWb = buildReorderWorkbook(lowStockAlerts as any, label);
  const reorderExcelBuffer = Buffer.from(await reorderWb.xlsx.writeBuffer());

  const extraSectionsHtml =
    renderSectionTitle(`Top ${TOP_PRODUCTS_LIMIT} sản phẩm bán chạy`) +
    renderProductTable(topProducts) +
    renderSectionTitle(`Sản phẩm cần nhập thêm (top ${REORDER_LIMIT})`) +
    renderReorderTable(reorderList as any);

  const html = renderEmailShell({
    reportTitle: "Báo cáo kết quả kinh doanh",
    reportCode: `BC-${fileLabel}`,
    periodLabel: label,
    kpiGridHtml,
    extraSectionsHtml,
    note: "Chi tiết đầy đủ từng đơn hàng xem file <b>bảng kê bán hàng</b> đính kèm; tồn kho đầu kỳ/nhập/xuất/cuối kỳ theo từng sản phẩm xem file <b>xuất nhập tồn kho</b>; danh sách đầy đủ sản phẩm cần nhập (tách riêng Máy/Linh kiện) xem file <b>cần nhập thêm</b>.",
  });

  await sendMail({
    subject: `[Báo cáo ${label}] Bán hàng & Xuất nhập tồn kho`,
    html,
    attachments: [
      { filename: `ban-hang-chi-tiet-${fileLabel}.xlsx`, content: salesExcelBuffer },
      { filename: `xuat-nhap-ton-${fileLabel}.xlsx`, content: stockExcelBuffer },
      { filename: `can-nhap-them-${fileLabel}.xlsx`, content: reorderExcelBuffer },
    ],
  });

  return { label, totals };
}

/**
 * Gửi báo cáo QUÝ TRƯỚC: KPI tổng hợp doanh thu (so với quý liền trước), không
 * cần chi tiết/file đính kèm.
 */
export async function sendQuarterlyReport(forDate = new Date()) {
  const { from, to, label } = prevQuarterRange(forDate);
  const prevRange = shiftQuarterRangeBack(from);

  const [{ totals }, { totals: prevTotals }] = await Promise.all([
    getSalesLedger({ from, to }),
    getSalesLedger({ from: prevRange.from, to: prevRange.to }),
  ]);

  const profit = totals.totalRevenue - totals.totalCost;
  const prevProfit = prevTotals.totalRevenue - prevTotals.totalCost;

  const kpiGridHtml = renderKpiGrid([
    {
      label: "Doanh thu",
      value: `${fmtMoney(totals.totalRevenue)}đ`,
      delta: deltaBadge(pctChange(totals.totalRevenue, prevTotals.totalRevenue), "so với quý trước"),
    },
    {
      label: "Giá vốn",
      value: `${fmtMoney(totals.totalCost)}đ`,
      delta: deltaBadge(pctChange(totals.totalCost, prevTotals.totalCost), "so với quý trước"),
    },
    {
      label: "Lợi nhuận gộp",
      value: `${fmtMoney(profit)}đ`,
      delta: deltaBadge(pctChange(profit, prevProfit), "so với quý trước"),
    },
  ]);

  const html = renderEmailShell({
    reportTitle: "Báo cáo tổng hợp doanh thu quý",
    reportCode: `BC-${label.replace(/\s|\//g, "-")}`,
    periodLabel: label,
    kpiGridHtml,
  });

  await sendMail({
    subject: `[Báo cáo ${label}] Tổng hợp doanh thu`,
    html,
  });

  return { label, totals };
}