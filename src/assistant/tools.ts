// src/assistant/tools.ts
import { prisma } from "../tool/prisma";

export type ItemKind = "MACHINE" | "PART";

export type SearchStockParams = {
  sku?: string;
  name?: string;
  locationCode?: string;
  onlyPositive?: boolean;
  kind?: ItemKind;
  limit?: number;
};

function normalizeSkuVariantsForTools(raw: string) {
  const s = raw.trim().replace(/\s+/g, "");
  const vars = new Set<string>();
  vars.add(s);
  vars.add(s.replace(/_/g, "-"));
  vars.add(s.replace(/-/g, "")); // phòng khi DB lưu mã không có gạch nối (vd PCX20)

  const m = s.match(/^([A-Za-z]+)(\d+)$/);
  if (m) vars.add(`${m[1]}-${m[2]}`);

  return Array.from(vars);
}

// ---------- Fuzzy matching cho tên/mã tiếng Việt (dữ liệu mã sản phẩm lộn xộn) ----------

const VN_DIACRITIC_MAP: Record<string, string> = {
  à: "a", á: "a", ạ: "a", ả: "a", ã: "a", â: "a", ầ: "a", ấ: "a", ậ: "a", ẩ: "a", ẫ: "a",
  ă: "a", ằ: "a", ắ: "a", ặ: "a", ẳ: "a", ẵ: "a",
  è: "e", é: "e", ẹ: "e", ẻ: "e", ẽ: "e", ê: "e", ề: "e", ế: "e", ệ: "e", ể: "e", ễ: "e",
  ì: "i", í: "i", ị: "i", ỉ: "i", ĩ: "i",
  ò: "o", ó: "o", ọ: "o", ỏ: "o", õ: "o", ô: "o", ồ: "o", ố: "o", ộ: "o", ổ: "o", ỗ: "o",
  ơ: "o", ờ: "o", ớ: "o", ợ: "o", ở: "o", ỡ: "o",
  ù: "u", ú: "u", ụ: "u", ủ: "u", ũ: "u", ư: "u", ừ: "u", ứ: "u", ự: "u", ử: "u", ữ: "u",
  ỳ: "y", ý: "y", ỵ: "y", ỷ: "y", ỹ: "y",
  đ: "d",
};

export function normalizeVN(raw: string) {
  const lower = String(raw || "").trim().toLowerCase();
  const noDia = lower.replace(
    /[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]/g,
    (c) => VN_DIACRITIC_MAP[c] || c
  );
  return noDia.replace(/[^a-z0-9\s-]/g, " ").replace(/\s+/g, " ").trim();
}

/** Tách 1 mã/chuỗi thành phần chữ đầu (prefix) và phần số (number), vd "JL-660" -> jl / 660 */
function splitCode(raw: string): { prefix: string; number: string } {
  const s = normalizeVN(raw).replace(/[\s-]+/g, "");
  const m = s.match(/^([a-z]*)(\d*)/);
  return { prefix: m?.[1] || "", number: m?.[2] || "" };
}

/** Chấm điểm mức độ khớp giữa câu hỏi và 1 chuỗi (tên hoặc mã sản phẩm), 0-100 */
export function scoreText(query: string, target: string): number {
  const q = normalizeVN(query);
  const t = normalizeVN(target);
  if (!q || !t) return 0;
  if (t === q) return 100;

  // So khớp kiểu "mã sản phẩm" (chữ đầu + số, vd JL-660): nếu phần CHỮ trùng nhau,
  // coi là cùng dòng/họ sản phẩm -> ưu tiên rất cao, kể cả khi phần số khác nhau.
  // Tránh trường hợp 2 mã chỉ tình cờ trùng phần SỐ (JL-600 vs DZ-600) bị xếp ngang
  // hàng với mã cùng họ thật sự (JL-600 vs JL-660).
  const qc = splitCode(query);
  const tc = splitCode(target);
  if (qc.prefix && tc.prefix && qc.prefix === tc.prefix) {
    if (qc.number && tc.number && qc.number === tc.number) return 95;
    if (qc.number && tc.number) return 55; // cùng họ, khác số cụ thể
    return 50; // chỉ khớp phần chữ
  }

  if (t.includes(q) || q.includes(t)) return 80;

  const qTokens = q.split(/[\s-]+/).filter(Boolean);
  if (qTokens.length === 0) return 0;
  const tTokenSet = new Set(t.split(/[\s-]+/).filter(Boolean));
  const overlap = qTokens.filter((tok) => tTokenSet.has(tok)).length;
  if (overlap === 0) return 0;
  return Math.round((overlap / qTokens.length) * 60);
}

const STOCK_SELECT = {
  qty: true,
  avgCost: true,
  updatedAt: true,
  item: {
    select: {
      id: true,
      sku: true,
      name: true,
      kind: true,
      unit: { select: { code: true } },
    },
  },
  location: { select: { id: true, code: true, name: true } },
} as const;

function mapStockRow(r: any) {
  return {
    itemId: r.item.id,
    sku: r.item.sku,
    name: r.item.name,
    kind: r.item.kind as ItemKind,
    unit: r.item.unit?.code || "",
    qty: r.qty?.toString?.() ?? String(r.qty),
    avgCost: r.avgCost?.toString?.() ?? String(r.avgCost),
    location: r.location,
    updatedAt: r.updatedAt?.toISOString?.() ?? String(r.updatedAt),
  };
}

export async function searchStock(params: SearchStockParams) {
  const {
    sku,
    name,
    locationCode,
    onlyPositive = false,
    kind,
    limit = 50,
  } = params;

  const baseWhere: any = {
    ...(onlyPositive ? { qty: { gt: 0 } } : {}),
    ...(locationCode
      ? { location: { code: { equals: locationCode, mode: "insensitive" } } }
      : {}),
  };

  // ---------- Bước 1: thử match chính xác/gần đúng qua SQL (nhanh, tốt khi mã sạch) ----------
  const skuOr: any[] = [];
  if (sku) {
    for (const v of normalizeSkuVariantsForTools(sku)) {
      skuOr.push({ sku: { contains: v, mode: "insensitive" as const } });
    }
  }

  if (skuOr.length || name) {
    const exactRows = await prisma.stock.findMany({
      where: {
        ...baseWhere,
        item: {
          ...(skuOr.length ? { OR: skuOr } : {}),
          ...(name ? { name: { contains: name, mode: "insensitive" } } : {}),
          ...(kind ? { kind } : {}),
        },
      },
      select: STOCK_SELECT,
      orderBy: [{ updatedAt: "desc" }],
      take: limit,
    });

    if (exactRows.length > 0) {
      return exactRows.map(mapStockRow);
    }
  }

  // ---------- Bước 2: không ra kết quả (hoặc không có sku/name) -> fallback fuzzy ----------
  // Dữ liệu mã sản phẩm (SKU) bên kho này không đồng nhất: nhiều SKU trùng nhau giữa các
  // sản phẩm khác nhau, nhiều sản phẩm không có SKU. Nên khi tra chính xác thất bại, quét
  // rộng hơn (lọc theo kind/location như bình thường) rồi so khớp mờ theo tên/SKU đã bỏ dấu.
  const query = (name || sku || "").trim();
  if (!query) return [];

  const candidates = await prisma.stock.findMany({
    where: {
      ...baseWhere,
      ...(kind ? { item: { kind } } : {}),
    },
    select: STOCK_SELECT,
    take: 3000, // đủ lớn để quét hết danh mục, vẫn nhẹ vì chỉ select field cần thiết
  });

  const scored = candidates
    .map((r: any) => ({
      row: r,
      score: Math.max(
        scoreText(query, r.item?.name || ""),
        scoreText(query, r.item?.sku || "")
      ),
    }))
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);

  return scored.map((x) => mapStockRow(x.row));
}

export type ItemSalesParams = {
  itemQuery: string; // tên hoặc SKU sản phẩm, có thể không dấu/gõ tắt
  from?: string; // yyyy-mm-dd
  to?: string; // yyyy-mm-dd
};

/**
 * Doanh thu/số lượng đã bán của MỘT sản phẩm (theo tên hoặc SKU) trong khoảng thời gian.
 * Khác với searchInvoices (chỉ lọc theo mã hóa đơn/khách hàng), hàm này join thẳng
 * qua InvoiceLine để trả lời đúng câu hỏi "doanh thu của sản phẩm X".
 * Chỉ tính hóa đơn loại SALES đã APPROVED (không tính nháp/đã huỷ).
 */
export async function getItemSalesSummary(params: ItemSalesParams) {
  const { itemQuery, from, to } = params;
  if (!itemQuery?.trim()) {
    return { matched: [], totalRevenue: "0", totalQty: "0", note: "Thiếu tên/mã sản phẩm" };
  }

  // 1) Tìm các item khớp với câu hỏi (fuzzy, bỏ dấu) — có thể khớp nhiều item cùng lúc
  const allItems = await prisma.item.findMany({
    select: { id: true, sku: true, name: true, kind: true },
  });

  const scored = allItems
    .map((it) => ({
      item: it,
      score: Math.max(scoreText(itemQuery, it.name || ""), scoreText(itemQuery, it.sku || "")),
    }))
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 5);

  if (scored.length === 0) {
    return { matched: [], totalRevenue: "0", totalQty: "0", note: "Không tìm thấy sản phẩm phù hợp" };
  }

  const itemIds = scored.map((x) => x.item.id);
  const fromDate = from ? new Date(from + "T00:00:00.000Z") : undefined;
  const toDate = to ? new Date(to + "T23:59:59.999Z") : undefined;

  // 2) Cộng dồn doanh thu thật từ InvoiceLine (chỉ hóa đơn SALES đã APPROVED)
  const lines = await prisma.invoiceLine.findMany({
    where: {
      itemId: { in: itemIds },
      invoice: {
        type: "SALES" as any,
        status: "APPROVED" as any,
        ...(fromDate || toDate
          ? { issueDate: { ...(fromDate ? { gte: fromDate } : {}), ...(toDate ? { lte: toDate } : {}) } }
          : {}),
      },
    },
    select: { itemId: true, qty: true, amount: true },
  });

  const byItem = new Map<string, { qty: number; revenue: number }>();
  for (const l of lines) {
    const cur = byItem.get(l.itemId) || { qty: 0, revenue: 0 };
    cur.qty += Number(l.qty);
    cur.revenue += Number(l.amount);
    byItem.set(l.itemId, cur);
  }

  const matched = scored.map((x) => {
    const agg = byItem.get(x.item.id) || { qty: 0, revenue: 0 };
    return {
      itemId: x.item.id,
      sku: x.item.sku,
      name: x.item.name,
      kind: x.item.kind,
      qtySold: agg.qty,
      revenue: agg.revenue,
    };
  });

  const totalRevenue = matched.reduce((s, m) => s + m.revenue, 0);
  const totalQty = matched.reduce((s, m) => s + m.qtySold, 0);

  return {
    matched,
    totalRevenue: String(totalRevenue),
    totalQty: String(totalQty),
  };
}

export type StockCoverageParams = {
  itemQuery: string;
};

type CoverageItem = { id: string; sku: string; name: string; kind: string };

/**
 * Tính coverage (tồn / tốc độ bán / verdict) cho một danh sách item ĐÃ XÁC ĐỊNH
 * (không tự tìm kiếm item — dùng chung cho getStockCoverage và getItemFamilyReport
 * để đảm bảo 2 nơi luôn cho ra cùng 1 công thức đánh giá, không lệch nhau).
 */
async function computeCoverageForItems(items: CoverageItem[]) {
  if (items.length === 0) return [];

  const itemIds = items.map((i) => i.id);
  const now = new Date();
  const from30 = new Date(now.getTime() - 30 * 86400000);
  const from60 = new Date(now.getTime() - 60 * 86400000);

  const [stocks, sold30, sold60] = await Promise.all([
    prisma.stock.findMany({ where: { itemId: { in: itemIds } }, select: { itemId: true, qty: true } }),
    prisma.invoiceLine.groupBy({
      by: ["itemId"],
      where: { itemId: { in: itemIds }, invoice: { type: "SALES" as any, status: "APPROVED" as any, issueDate: { gte: from30 } } },
      _sum: { qty: true },
    }),
    prisma.invoiceLine.groupBy({
      by: ["itemId"],
      where: { itemId: { in: itemIds }, invoice: { type: "SALES" as any, status: "APPROVED" as any, issueDate: { gte: from60 } } },
      _sum: { qty: true },
    }),
  ]);

  const qtyMap = new Map<string, number>();
  for (const s of stocks) qtyMap.set(s.itemId, (qtyMap.get(s.itemId) || 0) + Number(s.qty));
  const sold30Map = new Map<string, number>();
  for (const s of sold30) sold30Map.set(s.itemId, Number(s._sum.qty ?? 0));
  const sold60Map = new Map<string, number>();
  for (const s of sold60) sold60Map.set(s.itemId, Number(s._sum.qty ?? 0));

  const MIN_SAMPLE = 3; // bán dưới ngưỡng này trong cả window thì coi là quá ít dữ liệu để tin số ngày-đủ-bán

  return items.map((it) => {
    const qty = qtyMap.get(it.id) || 0;
    const isMachine = it.kind === "MACHINE";
    const windowDays = isMachine ? 60 : 30;
    const soldNd = (isMachine ? sold60Map.get(it.id) : sold30Map.get(it.id)) || 0;
    const avgDaily = soldNd / windowDays;
    const daysCover = avgDaily > 0 ? Math.round((qty / avgDaily) * 10) / 10 : null;

    let verdict: string;
    let note: string | undefined;

    if (soldNd <= 0) {
      verdict = "KHONG_BAN_GAN_DAY";
    } else if (qty <= (isMachine ? 1 : 0)) {
      verdict = "THAP";
      note = "Tồn tuyệt đối gần như bằng 0 (không còn hàng dự phòng), bất kể tốc độ bán.";
    } else if (soldNd < MIN_SAMPLE) {
      verdict = "KHONG_DU_DU_LIEU";
      note = `Chỉ bán ${soldNd} cái trong ${windowDays} ngày qua — số liệu quá ít để đánh giá chính xác mức tồn.`;
    } else if (daysCover !== null && daysCover <= 7) {
      verdict = "THAP";
    } else if (daysCover !== null && daysCover <= 14) {
      verdict = "TRUNG_BINH";
    } else {
      verdict = "CAO";
    }

    return {
      itemId: it.id,
      sku: it.sku,
      name: it.name,
      kind: it.kind,
      qty,
      soldLastNDays: soldNd,
      windowDays,
      daysCover,
      verdict,
      note,
    };
  });
}

/**
 * Đánh giá tồn kho của MỘT sản phẩm là "nhiều" hay "ít" dựa trên TỐC ĐỘ BÁN THẬT,
 * không dựa vào số tuyệt đối (300 cái có thể là rất ít nếu bán 1500 cái/tháng, và
 * ngược lại rất nhiều nếu cả tháng chỉ bán 2 cái).
 * PART: dùng doanh số 30 ngày gần nhất. MACHINE: dùng doanh số 60 ngày gần nhất
 * (đồng nhất với logic đã dùng ở buildLowStockAlerts).
 */
export async function getStockCoverage(params: StockCoverageParams) {
  const { itemQuery } = params;
  if (!itemQuery?.trim()) {
    return { matched: [], note: "Thiếu tên/mã sản phẩm" };
  }

  const allItems = await prisma.item.findMany({
    select: { id: true, sku: true, name: true, kind: true },
  });

  const scored = allItems
    .map((it) => ({
      item: it,
      score: Math.max(scoreText(itemQuery, it.name || ""), scoreText(itemQuery, it.sku || "")),
    }))
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 5);

  if (scored.length === 0) {
    return { matched: [], note: "Không tìm thấy sản phẩm phù hợp" };
  }

  const matched = await computeCoverageForItems(scored.map((x) => x.item as CoverageItem));
  return { matched };
}

function verdictRank(v: string) {
  const order: Record<string, number> = {
    THAP: 0,
    KHONG_DU_DU_LIEU: 1,
    TRUNG_BINH: 2,
    CAO: 3,
    KHONG_BAN_GAN_DAY: 4,
  };
  return order[v] ?? 5;
}

/**
 * Báo cáo tồn kho CỐ ĐỊNH (không qua AI soạn câu) cho cả một "họ" sản phẩm — máy
 * chính + toàn bộ linh kiện liên quan. Dữ liệu kho này thường dùng CHUNG 1 mã cho
 * cả máy và các linh kiện của máy đó (vd nhiều linh kiện khác nhau đều mang mã
 * "JL-660"), nên gom theo đúng mã đó sẽ ra cả họ sản phẩm.
 * Dùng cho báo cáo/cảnh báo nhập hàng — luôn cho kết quả giống hệt nhau mỗi lần hỏi
 * cùng dữ liệu, không phụ thuộc vào cách AI diễn đạt.
 */
export async function getItemFamilyReport(itemQuery: string) {
  if (!itemQuery?.trim()) {
    return { machines: [], parts: [], note: "Thiếu tên/mã sản phẩm" };
  }

  const allItems = await prisma.item.findMany({
    select: { id: true, sku: true, name: true, kind: true },
  });

  const scored = allItems
    .map((it) => ({
      item: it,
      score: Math.max(scoreText(itemQuery, it.name || ""), scoreText(itemQuery, it.sku || "")),
    }))
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score);

  if (scored.length === 0) {
    return { machines: [], parts: [], note: "Không tìm thấy sản phẩm phù hợp" };
  }

  const topSku = (scored[0].item.sku || "").trim().toLowerCase();
  let familyItems = topSku
    ? allItems.filter((it) => (it.sku || "").trim().toLowerCase() === topSku)
    : [];

  // Không có sản phẩm nào khác cùng mã (vd sản phẩm không có mã) -> dùng top kết quả fuzzy
  if (familyItems.length <= 1) {
    familyItems = scored.slice(0, 10).map((x) => x.item);
  }

  const coverage = await computeCoverageForItems(familyItems as CoverageItem[]);

  const machines = coverage
    .filter((c) => c.kind === "MACHINE")
    .sort((a, b) => verdictRank(a.verdict) - verdictRank(b.verdict));
  const parts = coverage
    .filter((c) => c.kind === "PART")
    .sort((a, b) => verdictRank(a.verdict) - verdictRank(b.verdict));

  return { machines, parts, familyCode: scored[0].item.sku };
}

/**
 * Liệt kê các sản phẩm đang có tồn kho ÂM (qty < 0) — đây là dấu hiệu lỗi dữ liệu
 * thực sự (bán vượt quá số lượng tồn kho thực có), không phải câu hỏi tra cứu tên
 * sản phẩm thông thường.
 */
export async function getNegativeStockItems(params: { kind?: ItemKind } = {}) {
  const rows = await prisma.stock.findMany({
    where: {
      qty: { lt: 0 },
      ...(params.kind ? { item: { kind: params.kind } } : {}),
    },
    select: STOCK_SELECT,
    orderBy: [{ qty: "asc" }],
    take: 100,
  });

  return rows.map(mapStockRow);
}

export type CustomerInfoResult = {
  customerId: string;
  name: string;
  phone: string | null;
  totalSpent: number;
  totalDebt: number;
  recentInvoices: Array<{ code: string; date: string; total: number; paid: number }>;
  recentNotes: Array<{ content: string; date: string }>;
};

/**
 * Tra cứu khách hàng theo tên/SĐT/mã (fuzzy) — trả về tóm tắt: đã mua tổng bao
 * nhiêu, còn nợ bao nhiêu, các hóa đơn gần đây, và ghi chú CSKH gần đây (nếu có).
 * Dùng trước khi gọi điện/gặp khách để nắm nhanh lịch sử giao dịch.
 */
export async function getCustomerInfo(customerQuery: string): Promise<{
  matched: CustomerInfoResult[];
  note?: string;
}> {
  if (!customerQuery?.trim()) {
    return { matched: [], note: "Thiếu tên/SĐT khách hàng" };
  }

  const allPartners = await prisma.partner.findMany({
    select: { id: true, name: true, phone: true, code: true },
  });

  const scored = allPartners
    .map((p) => ({
      partner: p,
      score: Math.max(
        scoreText(customerQuery, p.name || ""),
        scoreText(customerQuery, p.phone || ""),
        scoreText(customerQuery, p.code || "")
      ),
    }))
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 3);

  if (scored.length === 0) {
    return { matched: [], note: "Không tìm thấy khách hàng phù hợp" };
  }

  const results: CustomerInfoResult[] = [];

  for (const s of scored) {
    const p = s.partner;

    const invoices = await prisma.invoice.findMany({
      where: { partnerId: p.id, type: "SALES" as any, status: "APPROVED" as any },
      orderBy: { issueDate: "desc" },
      select: {
        code: true,
        issueDate: true,
        total: true,
        paidAmount: true,
        netTotal: true,
      },
      take: 20,
    });

    let totalSpent = 0;
    let totalDebt = 0;
    for (const inv of invoices) {
      const total = Number(inv.total);
      const netTotal = Number(inv.netTotal) || total;
      const paid = Number(inv.paidAmount);
      totalSpent += total;
      totalDebt += Math.max(0, netTotal - paid);
    }

    const recentNotes = await prisma.customerNote.findMany({
      where: { partnerId: p.id },
      orderBy: { createdAt: "desc" },
      take: 5,
      select: { content: true, createdAt: true },
    });

    results.push({
      customerId: p.id,
      name: p.name,
      phone: p.phone,
      totalSpent,
      totalDebt,
      recentInvoices: invoices.slice(0, 10).map((i) => ({
        code: i.code,
        date: i.issueDate.toISOString().slice(0, 10),
        total: Number(i.total),
        paid: Number(i.paidAmount),
      })),
      recentNotes: recentNotes.map((n) => ({
        content: n.content,
        date: n.createdAt.toISOString().slice(0, 10),
      })),
    });
  }

  return { matched: results };
}

export type SearchInvoicesParams = {
  code?: string;
  partnerText?: string;
  from?: string; // yyyy-mm-dd
  to?: string; // yyyy-mm-dd
  limit?: number;
};

export async function searchInvoices(params: SearchInvoicesParams) {
  const { code, partnerText, from, to, limit = 50 } = params;

  const fromDate = from ? new Date(from + "T00:00:00.000Z") : undefined;
  const toDate = to ? new Date(to + "T23:59:59.999Z") : undefined;

  const rows = await prisma.invoice.findMany({
    where: {
      ...(code ? { code: { contains: code } } : {}),
      ...(partnerText
        ? {
            OR: [
              { partnerName: { contains: partnerText, mode: "insensitive" } },
              { partnerPhone: { contains: partnerText, mode: "insensitive" } },
              { partnerCode: { contains: partnerText, mode: "insensitive" } },
            ],
          }
        : {}),
      ...(fromDate || toDate
        ? {
            issueDate: {
              ...(fromDate ? { gte: fromDate } : {}),
              ...(toDate ? { lte: toDate } : {}),
            },
          }
        : {}),
    },
    select: {
      id: true,
      code: true,
      codeYear: true,
      type: true,
      issueDate: true,
      partnerName: true,
      total: true,
      status: true,
      paymentStatus: true,
      paidAmount: true,
    },
    orderBy: [{ issueDate: "desc" }],
    take: limit,
  });

  return rows.map((r) => ({
    ...r,
    issueDate: r.issueDate.toISOString(),
    total: r.total?.toString?.() ?? String(r.total),
    paidAmount: r.paidAmount?.toString?.() ?? String(r.paidAmount),
  }));
}