// src/services/me_sales_dashboard.service.ts
import { Prisma, PrismaClient, InvoiceStatus, InvoiceType } from "@prisma/client";

const prisma = new PrismaClient();

/** ---------------- helpers ---------------- **/
function n(x: any): number {
  if (x == null) return 0;
  if (typeof x === "number") return x;
  const v = Number(String(x));
  return Number.isFinite(v) ? v : 0;
}
function clamp0(x: number) {
  return x < 0 ? 0 : x;
}
function startEndOfMonthUTC(year: number, month: number) {
  // month: 1..12
  const start = new Date(Date.UTC(year, month - 1, 1, 0, 0, 0));
  const end = new Date(Date.UTC(year, month, 1, 0, 0, 0));
  return { start, end };
}
function formatDateVN(d: Date) {
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const yy = d.getUTCFullYear();
  return `${dd}/${mm}/${yy}`;
}
function truncKey(groupBy: "day" | "week" | "month") {
  return groupBy === "month" ? "month" : groupBy === "week" ? "week" : "day";
}
function revenueSign(t: InvoiceType) {
  if (t === "SALES") return 1;
  if (t === "SALES_RETURN") return -1;
  return 0;
}

/** ---------------- types ---------------- **/
export type MySalesDashboardResp = {
  period?: { month: number; year: number; from?: string; to?: string };
  summary: {
    revenue: number; // subtotal signed theo approvedAt
    collected: number; // tổng đã thu (paidAmount)
    normalOutstanding: number; // nợ thường (cần thu)
    holdOutstanding: number; // BH treo
    totalOutstanding: number; // tổng nợ = normal + hold
    orderCount: number;
  };
  trend: Array<{ date: string; revenue: number }>;
  debts: Array<{
    invoiceId: string;
    invoiceCode: string;
    customerName: string;
    date: string; // dd/MM/yyyy

    invoiceTotal: number; // total (đã VAT)
    paid: number; // paidAmount

    normalOutstanding: number;
    holdOutstanding: number;
    totalOutstanding: number;

    warrantyDueDate?: string | null;
  }>;
};

function calcHoldTotal(inv: {
  total: any;
  hasWarrantyHold: boolean;
  warrantyHoldPct: any;
  warrantyHoldAmount: any;
}) {
  const total = n(inv.total);
  const amt = n(inv.warrantyHoldAmount);
  if (amt > 0.0001) return clamp0(Math.min(total, amt));

  const pct = n(inv.warrantyHoldPct);
  if (inv.hasWarrantyHold && pct > 0.0001) {
    return clamp0((total * pct) / 100);
  }
  return 0;
}

export async function getMySalesDashboard(params: {
  saleUserId: string;
  month: number; // 1..12
  year: number;
  groupBy?: "day" | "week" | "month";
}): Promise<MySalesDashboardResp> {
  const { saleUserId, month, year } = params;
  const groupBy = params.groupBy ?? "day";
  const trunc = truncKey(groupBy);

  if (!Number.isFinite(month) || month < 1 || month > 12) {
    throw new Error("month không hợp lệ (1..12)");
  }
  if (!Number.isFinite(year) || year < 2000 || year > 2100) {
    throw new Error("year không hợp lệ");
  }

  const { start, end } = startEndOfMonthUTC(year, month);

  /** =========================
   * 1) Load invoices APPROVED in month for this SALE
   * - SALES & SALES_RETURN để tính revenue net (theo subtotal)
   * - include warrantyHold để biết status PAID/OPEN
   * ========================= */
  const invRows = await prisma.invoice.findMany({
    where: {
      status: InvoiceStatus.APPROVED,
      approvedAt: { not: null, gte: start, lt: end },
      type: { in: [InvoiceType.SALES, InvoiceType.SALES_RETURN] },
      saleUserId,
    },
    select: {
      id: true,
      code: true,
      type: true,
      approvedAt: true,
      issueDate: true,
      partnerName: true,

      subtotal: true,
      total: true,
      paidAmount: true,

      hasWarrantyHold: true,
      warrantyHoldPct: true,
      warrantyHoldAmount: true,
      warrantyDueDate: true,

      warrantyHold: {
        select: {
          status: true, // OPEN | PAID | VOID
        },
      },
    },
    orderBy: { approvedAt: "asc" },
  });

  // Revenue net theo subtotal signed
  let netRevenue = 0;
  for (const r of invRows) {
    netRevenue += revenueSign(r.type) * n(r.subtotal);
  }

  const debts: MySalesDashboardResp["debts"] = [];

  let sumCollected = 0;
  let sumNormalOutstanding = 0;
  let sumHoldOutstanding = 0;

  for (const inv of invRows as any[]) {
    // công nợ chỉ quan tâm SALES (return không list như khoản phải thu)
    if (inv.type !== "SALES") continue;

    const invoiceTotal = n(inv.total);
    const paid = clamp0(n(inv.paidAmount));
    const totalDebt = clamp0(invoiceTotal - paid);

    if (totalDebt <= 0.0001) continue;

    // HOLD total theo invoice (amount ưu tiên, fallback pct)
    const holdTotal = calcHoldTotal(inv);

    // HOLD còn treo:
    // - nếu warrantyHold.status = PAID/VOID => 0
    // - còn lại: min(holdTotal, totalDebt)
    const holdOutstanding =
      String(inv.warrantyHold?.status || "").toUpperCase() === "PAID" ||
      String(inv.warrantyHold?.status || "").toUpperCase() === "VOID"
        ? 0
        : Math.min(holdTotal, totalDebt);

    const normalOutstanding = clamp0(totalDebt - holdOutstanding);

    sumCollected += paid;
    sumNormalOutstanding += normalOutstanding;
    sumHoldOutstanding += holdOutstanding;

    debts.push({
      invoiceId: inv.id,
      invoiceCode: String(inv.code),
      customerName: String(inv.partnerName || ""),
      date: formatDateVN(new Date(inv.issueDate)),
      invoiceTotal,
      paid,
      normalOutstanding,
      holdOutstanding,
      totalOutstanding: normalOutstanding + holdOutstanding,
      warrantyDueDate: inv.warrantyDueDate
        ? new Date(inv.warrantyDueDate).toISOString().slice(0, 10)
        : null,
    });
  }

  // sort: nợ thường giảm dần rồi mới nhất (giữ cảm giác)
  debts.sort((a, b) => {
    if (b.normalOutstanding !== a.normalOutstanding)
      return b.normalOutstanding - a.normalOutstanding;
    return (b.date || "").localeCompare(a.date || "");
  });

  /** =========================
   * 2) Trend net revenue (approvedAt) by day/week/month
   * ========================= */
  const trendRaw: Array<{ t: any; revenue: any }> =
    invRows.length === 0
      ? []
      : await prisma.$queryRaw`
        SELECT
          date_trunc(${trunc}, i."approvedAt") AS t,
          COALESCE(SUM(
            CASE i."type"
              WHEN 'SALES' THEN COALESCE(i."subtotal",0)
              WHEN 'SALES_RETURN' THEN -COALESCE(i."subtotal",0)
              ELSE 0
            END
          ),0) AS revenue
        FROM "Invoice" i
        WHERE
          i."status" = 'APPROVED'
          AND i."type" IN ('SALES','SALES_RETURN')
          AND i."approvedAt" IS NOT NULL
          AND i."saleUserId" = ${saleUserId}
          AND i."approvedAt" >= ${start}
          AND i."approvedAt" < ${end}
        GROUP BY 1
        ORDER BY 1 ASC
      `;

  const trend = (trendRaw || []).map((r: any) => ({
    date: new Date(r.t).toISOString(),
    revenue: n(r.revenue),
  }));

  return {
    period: { month, year },
    summary: {
      revenue: netRevenue,
      collected: sumCollected,
      normalOutstanding: sumNormalOutstanding,
      holdOutstanding: sumHoldOutstanding,
      totalOutstanding: sumNormalOutstanding + sumHoldOutstanding,
      orderCount: invRows.length,
    },
    trend,
    debts,
  };
}
