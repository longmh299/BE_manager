// src/services/assistant/fastAnswer.ts
//
// "Fast path": với câu hỏi đơn giản, nhận diện rõ ràng qua parsePro (đã có sẵn
// trong repo nhưng trước đây chưa được gọi tới), trả lời thẳng từ dữ liệu thật
// mà KHÔNG cần gọi Claude — nhanh hơn nhiều lần, không tốn chi phí API, và LUÔN
// cho ra cùng 1 kết quả với cùng 1 dữ liệu (không phụ thuộc cách Claude "diễn đạt"
// mỗi lần một khác). Chỉ những câu hỏi mơ hồ/phức tạp mới rơi xuống Claude (xem
// chatService.ts).

import { parsePro, type ParseResult } from "./parsePro";
import { searchStock, getItemFamilyReport, getItemSalesSummary, getNegativeStockItems, getCustomerInfo } from "../../assistant/tools";
import { buildLowStockAlerts } from "./alerts/lowStock.service";
import { prisma } from "../../tool/prisma";

const FAST_PATH_MIN_CONFIDENCE = 0.4; // khớp với ngưỡng parsePro tự coi là "đã nhận diện được intent"

function fmtQty(q: any) {
  const n = Number(q);
  return Number.isFinite(n) ? String(n) : String(q);
}

function fmtMoney(n: any) {
  const v = Number(n);
  if (!Number.isFinite(v)) return String(n);
  return v.toLocaleString("vi-VN");
}

function verdictLabel(v: string) {
  switch (v) {
    case "THAP":
      return "THẤP";
    case "TRUNG_BINH":
      return "TRUNG BÌNH";
    case "CAO":
      return "CAO";
    case "KHONG_DU_DU_LIEU":
      return "CHƯA ĐỦ DỮ LIỆU";
    case "KHONG_BAN_GAN_DAY":
      return "KHÔNG BÁN GẦN ĐÂY";
    default:
      return v;
  }
}

function formatCoverageLine(c: any) {
  const mark = c.verdict === "THAP" ? " ⚠️" : "";
  const noteStr = c.note ? ` — ${c.note}` : "";
  return `- ${c.sku || "(không mã)"} — ${c.name}: tồn **${fmtQty(c.qty)}**, mức **${verdictLabel(
    c.verdict
  )}**${mark}${noteStr}`;
}

/**
 * Báo cáo tồn kho CỐ ĐỊNH cho cả họ sản phẩm (máy chính + linh kiện liên quan),
 * đánh giá theo tốc độ bán thật (không theo cảm tính). Luôn cho ra đúng 1 kết quả
 * với cùng 1 dữ liệu — phù hợp để dựa vào đó quyết định nhập hàng.
 * Trả về null nếu họ sản phẩm chỉ có 1 mục (không đáng để làm báo cáo dài).
 */
async function answerStockFamilyReport(familyCode: string): Promise<string | null> {
  const report = await getItemFamilyReport(familyCode);
  const total = report.machines.length + report.parts.length;
  if (total <= 1) return null;

  const lines: string[] = [];
  lines.push(`**Tồn kho ${report.familyCode || familyCode}** — tổng ${total} sản phẩm liên quan:`);

  if (report.machines.length > 0) {
    lines.push("");
    lines.push("Máy chính:");
    for (const m of report.machines) lines.push(formatCoverageLine(m));
  }

  if (report.parts.length > 0) {
    lines.push("");
    lines.push("Linh kiện:");
    for (const p of report.parts) lines.push(formatCoverageLine(p));
  }

  const critical = [...report.machines, ...report.parts].filter((c: any) => c.verdict === "THAP");
  lines.push("");
  lines.push(
    critical.length > 0
      ? `**Cần nhập gấp ${critical.length} mục** đang ở mức **THẤP**.`
      : "Chưa có mục nào cần nhập gấp."
  );

  return lines.join("\n");
}

async function answerGetStock(entities: ParseResult["entities"]): Promise<string | null> {
  const skuQuery = entities.skus?.[0];
  const nameQuery = entities.queryText;
  if (!skuQuery && !nameQuery) return null;

  // Nếu nhận diện được mã sản phẩm rõ ràng (vd JL-660), ưu tiên báo cáo cố định
  // theo cả họ máy (máy + linh kiện liên quan dùng chung mã) — chính xác và nhất
  // quán hơn nhiều so với để AI tự soạn câu mỗi lần một kiểu.
  if (skuQuery) {
    const familyReply = await answerStockFamilyReport(skuQuery);
    if (familyReply) return familyReply;
  }

  const kind = entities.kindPref && entities.kindPref !== "ALL" ? entities.kindPref : undefined;

  const rows = await searchStock({
    sku: skuQuery,
    name: !skuQuery ? nameQuery : undefined,
    kind,
    locationCode: entities.warehouseCode,
    limit: 8,
  });

  if (rows.length === 0) return "Không tìm thấy sản phẩm phù hợp trong kho.";

  if (rows.length === 1) {
    const r = rows[0];
    return `${r.sku || "(không mã)"} — ${r.name}: tồn **${fmtQty(r.qty)} ${r.unit || ""}**`.trim();
  }

  const uniqueNames = new Set(rows.map((r) => r.name));
  const lines = rows
    .slice(0, 6)
    .map((r) => `- ${r.sku || "(không mã)"} — ${r.name}: tồn **${fmtQty(r.qty)} ${r.unit || ""}**`);

  const header =
    uniqueNames.size > 1
      ? "Có nhiều sản phẩm khác nhau khớp với yêu cầu, bạn muốn xem cái nào:"
      : "Kết quả:";

  return [header, ...lines].join("\n");
}

async function answerLowStock(entities: ParseResult["entities"]): Promise<string> {
  const rows = await buildLowStockAlerts(prisma);
  const kind = entities.kindPref && entities.kindPref !== "ALL" ? entities.kindPref : undefined;
  const filtered = kind ? rows.filter((r: any) => r.kind === kind) : rows;

  if (filtered.length === 0) return "Hiện không có sản phẩm nào sắp hết hàng.";

  const lines = filtered
    .slice(0, 8)
    .map(
      (r: any) =>
        `- ${r.sku} — ${r.name}: tồn **${r.qty}**, nên nhập thêm **~${r.suggestQty}** (**${r.severity}**)`
    );

  return [`Có ${filtered.length} sản phẩm sắp hết/hết hàng:`, ...lines].join("\n");
}

async function answerOutOfStock(entities: ParseResult["entities"]): Promise<string> {
  const rows = await buildLowStockAlerts(prisma);
  const kind = entities.kindPref && entities.kindPref !== "ALL" ? entities.kindPref : undefined;
  const filtered = rows.filter(
    (r: any) => r.severity === "CRITICAL" && (!kind || r.kind === kind)
  );

  if (filtered.length === 0) return "Hiện không có sản phẩm nào hết hàng.";

  const lines = filtered
    .slice(0, 8)
    .map((r: any) => `- ${r.sku} — ${r.name}: **hết hàng**, nên nhập thêm **~${r.suggestQty}**`);

  return [`Có ${filtered.length} sản phẩm đang hết hàng:`, ...lines].join("\n");
}

async function answerGetRevenue(entities: ParseResult["entities"]): Promise<string | null> {
  const query = entities.skus?.[0] || entities.queryText;
  if (!query) return null;

  const from = entities.date?.from;
  const to = entities.date?.to;

  const result = await getItemSalesSummary({ itemQuery: query, from, to });

  if (!result.matched || result.matched.length === 0) {
    return result.note || "Không tìm thấy sản phẩm phù hợp.";
  }

  const sold = result.matched.filter((m: any) => m.qtySold > 0 || m.revenue > 0);
  if (sold.length === 0) {
    return "Không có đơn bán nào của sản phẩm này trong khoảng thời gian đã chọn.";
  }

  const rangeLabel = from || to ? `${from || "?"} → ${to || "hiện tại"}` : "toàn bộ dữ liệu";
  const lines = sold.map(
    (m: any) => `- ${m.sku} — ${m.name}: bán **${fmtQty(m.qtySold)}**, doanh thu **${fmtMoney(m.revenue)}đ**`
  );

  return [
    `Doanh thu (${rangeLabel}):`,
    ...lines,
    `Tổng: bán **${fmtQty(result.totalQty)}**, doanh thu **${fmtMoney(result.totalRevenue)}đ**`,
  ].join("\n");
}

async function answerNegativeStock(entities: ParseResult["entities"]): Promise<string> {
  const kind = entities.kindPref && entities.kindPref !== "ALL" ? entities.kindPref : undefined;
  const rows = await getNegativeStockItems({ kind });

  if (rows.length === 0) {
    return "Không có sản phẩm nào đang bị tồn âm — dữ liệu tồn kho đang ổn.";
  }

  const lines = rows
    .slice(0, 30)
    .map((r) => `- ${r.sku || "(không mã)"} — ${r.name}: tồn **${fmtQty(r.qty)}** ${r.unit || ""}`);

  return [
    `Có **${rows.length}** sản phẩm đang bị tồn âm (dấu hiệu bán vượt quá tồn kho thực có):`,
    ...lines,
  ].join("\n");
}

async function answerCustomerInfo(entities: ParseResult["entities"]): Promise<string | null> {
  const query = entities.queryText;
  if (!query) return null;

  const result = await getCustomerInfo(query);
  if (!result.matched || result.matched.length === 0) {
    return result.note || "Không tìm thấy khách hàng phù hợp.";
  }

  const blocks = result.matched.map((c) => {
    const lines: string[] = [];
    lines.push(`**${c.name}**${c.phone ? ` — ${c.phone}` : ""}`);
    lines.push(
      `Đã mua tổng **${fmtMoney(c.totalSpent)}đ**, còn nợ **${fmtMoney(c.totalDebt)}đ**`
    );

    if (c.recentInvoices.length > 0) {
      lines.push("Hóa đơn gần đây:");
      for (const inv of c.recentInvoices.slice(0, 5)) {
        lines.push(`- ${inv.date} — ${inv.code}: **${fmtMoney(inv.total)}đ** (đã trả ${fmtMoney(inv.paid)}đ)`);
      }
    }

    if (c.recentNotes.length > 0) {
      lines.push("Ghi chú gần đây:");
      for (const n of c.recentNotes.slice(0, 3)) {
        lines.push(`- ${n.date}: ${n.content}`);
      }
    }

    return lines.join("\n");
  });

  return blocks.join("\n\n---\n\n");
}

/**
 * Thử trả lời nhanh (không gọi Claude). Trả về null nếu câu hỏi không đủ rõ ràng
 * để xử lý bằng rule — khi đó chatService sẽ tự chuyển sang gọi Claude.
 */
export async function tryFastAnswer(userMessage: string): Promise<string | null> {
  const parsed = parsePro(userMessage);
  if (parsed.confidence < FAST_PATH_MIN_CONFIDENCE) return null;

  switch (parsed.intent) {
    case "GET_STOCK":
      return answerGetStock(parsed.entities);
    case "LOW_STOCK":
      return answerLowStock(parsed.entities);
    case "OUT_OF_STOCK":
      return answerOutOfStock(parsed.entities);
    case "NEGATIVE_STOCK":
      return answerNegativeStock(parsed.entities);
    case "GET_REVENUE":
      return answerGetRevenue(parsed.entities);
    case "CUSTOMER_INFO":
      return answerCustomerInfo(parsed.entities);
    default:
      // GET_INVOICES_BY_DATE / UNKNOWN: để Claude xử lý vì cần hiểu ngày tháng
      // và ngữ cảnh linh hoạt hơn khả năng của rule-based parser.
      return null;
  }
}