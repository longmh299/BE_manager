// src/routes/me_sales_dashboard.routes.ts
import { Router } from "express";
import {
  PrismaClient,
  InvoiceStatus,
  InvoiceType,
  AllocationKind,
} from "@prisma/client";
import { requireAuth, getUser } from "../middlewares/auth";

const prisma = new PrismaClient();
const router = Router();

/** ================= Helpers ================= */

function num(v: any): number {
  if (v == null) return 0;
  if (typeof v === "number") return Number.isFinite(v) ? v : 0;
  if (typeof v === "string") {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  }
  if (typeof v?.toString === "function") {
    const n = Number(v.toString());
    return Number.isFinite(n) ? n : 0;
  }
  return 0;
}

function clamp0(x: number) {
  return x < 0 ? 0 : x;
}

function pad2(n: number) {
  return String(n).padStart(2, "0");
}

function formatDateVN(d: Date) {
  return `${pad2(d.getDate())}/${pad2(d.getMonth() + 1)}/${d.getFullYear()}`;
}

function ymd(d: Date) {
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
}

/**
 * ✅ UTC month range (best practice)
 * - start: inclusive
 * - endExclusive: exclusive (first day of next month)
 */
function monthRangeUTC(year: number, month: number) {
  const start = new Date(Date.UTC(year, month - 1, 1, 0, 0, 0, 0));
  const endExclusive = new Date(Date.UTC(year, month, 1, 0, 0, 0, 0));
  return { start, endExclusive };
}

// tính holdTotal: ưu tiên amount, fallback pct
function calcHoldTotal(inv: {
  total: any;
  hasWarrantyHold: boolean;
  warrantyHoldPct: any;
  warrantyHoldAmount: any;
}) {
  const total = clamp0(num(inv.total));
  const holdAmt = clamp0(num(inv.warrantyHoldAmount));
  if (holdAmt > 0.0001) return Math.min(total, holdAmt);

  const pct = clamp0(num(inv.warrantyHoldPct));
  if (inv.hasWarrantyHold && pct > 0.0001) {
    return clamp0((total * pct) / 100);
  }
  return 0;
}

/**
 * ✅ Quy đổi GROSS -> NET theo tỷ lệ subtotal/total
 * - mục tiêu: KPI "Đã thu" của staff không bị > "Doanh thu" (đều NET)
 */
function grossToNet(gross: number, subtotal: number, total: number) {
  const g = clamp0(num(gross));
  const sub = clamp0(num(subtotal));
  const tot = clamp0(num(total));
  if (g <= 0) return 0;
  if (tot <= 0) return 0;
  if (sub <= 0) return 0;
  return (g * sub) / tot;
}

type CustomerAgg = {
  customerKey: string; // partnerId || partnerName || "WALKIN"
  partnerId: string | null;
  name: string;
  phone: string | null;
  address: string | null;
  taxCode: string | null;
  email: string | null;

  // legacy
  outstanding: number;

  // ✅ NEW: split (GROSS - tiền thực tế khách còn phải trả)
  normalOutstanding: number;
  holdOutstanding: number;
  totalOutstanding: number;

  invoiceCount: number;
  avgOutstanding: number;
};

type InvoiceListRow = {
  invoiceId: string;
  invoiceCode: string;

  // ✅ giữ issueDate để hiển thị (dd/MM/yyyy)
  issueDate: string;

  partnerId: string | null;
  customerName: string;

  customerPhone: string | null;
  customerAddress: string | null;
  customerTaxCode: string | null;
  customerEmail: string | null;

  subtotal: number; // NET
  totalAmount: number; // GROSS

  /**
   * ✅ collected: NET (đã thu quy về chưa VAT) để hiển thị cùng doanh thu
   * (tránh "đã thu > doanh thu")
   */
  collected: number;

  /** optional: đối soát kế toán */
  collectedGross?: number;

  outstanding: number; // legacy (TOTAL outstanding) (GROSS)

  // ✅ NEW split (GROSS)
  normalOutstanding: number;
  holdOutstanding: number;
  totalOutstanding: number;

  paymentStatus: string;
  warrantyDueDate?: string | null;
};

type TrendRow = { date: string; revenue: number };

/**
 * GET /api/me/sales-dashboard?month=12&year=2025
 */
router.get("/sales-dashboard", requireAuth, async (req, res) => {
  const me = getUser(req);
  if (!me?.id) {
    return res
      .status(401)
      .json({ code: "UNAUTHORIZED", message: "Unauthorized" });
  }

  const now = new Date();
  const month = Math.min(
    12,
    Math.max(1, Number(req.query.month ?? now.getMonth() + 1) || now.getMonth() + 1)
  );
  const year = Math.max(
    2000,
    Math.min(2100, Number(req.query.year ?? now.getFullYear()) || now.getFullYear())
  );

  const { start, endExclusive } = monthRangeUTC(year, month);

  /**
   * ✅ IMPORTANT FIX
   * Dashboard theo "tháng chốt doanh thu" => lọc theo approvedAt
   * (tránh lệch TZ issueDate + đúng logic doanh thu)
   */
  const invoices = await prisma.invoice.findMany({
    where: {
      status: InvoiceStatus.APPROVED,
      type: InvoiceType.SALES,
      saleUserId: me.id,

      approvedAt: { not: null, gte: start, lt: endExclusive },
    },
    select: {
      id: true,
      code: true,

      approvedAt: true,
      issueDate: true,

      partnerId: true,
      partnerName: true,
      partnerPhone: true,
      partnerAddr: true,
      partnerTax: true,

      partner: {
        select: {
          id: true,
          name: true,
          phone: true,
          address: true,
          taxCode: true,
          email: true,
        },
      },

      subtotal: true,
      total: true,
      paidAmount: true,
      paymentStatus: true,

      // ✅ NEW: warranty fields
      hasWarrantyHold: true,
      warrantyHoldPct: true,
      warrantyHoldAmount: true,
      warrantyDueDate: true,
      warrantyHold: {
        select: { status: true }, // OPEN | PAID | VOID
      },
    },
    orderBy: { approvedAt: "desc" },
  });

  if (!invoices.length) {
    return res.json({
      period: {
        month,
        year,
        from: start.toISOString(),
        to: new Date(endExclusive.getTime() - 1).toISOString(),
      },
      summary: {
        revenue: 0,
        collected: 0,
        collectedGross: 0, // ✅ thêm để đối soát

        outstanding: 0,

        normalOutstanding: 0,
        holdOutstanding: 0,
        totalOutstanding: 0,

        orderCount: 0,
      },
      trend: [] as TrendRow[],
      invoices: [] as InvoiceListRow[],
      debts: [] as InvoiceListRow[],
      customers: [] as CustomerAgg[],
    });
  }

  const invoiceIds = invoices.map((i) => i.id);

  // 2) Allocations: group by invoiceId & kind (NORMAL + WARRANTY_HOLD)
  const allocAgg = await prisma.paymentAllocation.groupBy({
    by: ["invoiceId", "kind"],
    where: {
      invoiceId: { in: invoiceIds },
      kind: { in: [AllocationKind.NORMAL, AllocationKind.WARRANTY_HOLD] },
    },
    _sum: { amount: true },
  });

  const normalPaidByInvoice = new Map<string, number>();
  const holdPaidByInvoice = new Map<string, number>();

  for (const a of allocAgg) {
    const amt = clamp0(num(a._sum.amount));
    if (a.kind === AllocationKind.WARRANTY_HOLD) {
      holdPaidByInvoice.set(a.invoiceId, amt);
    } else {
      normalPaidByInvoice.set(a.invoiceId, amt);
    }
  }

  // 3) Build invoices rows
  let sumRevenue = 0; // NET (subtotal)
  let sumCollectedNet = 0; // ✅ NET
  let sumCollectedGross = 0; // optional đối soát

  let sumNormalOutstanding = 0; // GROSS
  let sumHoldOutstanding = 0; // GROSS
  let sumTotalOutstanding = 0; // GROSS

  const invoiceRows: InvoiceListRow[] = invoices.map((inv: any) => {
    const invSubtotal = num(inv.subtotal); // NET
    const invTotal = num(inv.total); // GROSS

    // paid split (GROSS)
    const normalPaidGross = clamp0(normalPaidByInvoice.get(inv.id) ?? 0);
    const holdPaidGross = clamp0(holdPaidByInvoice.get(inv.id) ?? 0);

    // ✅ collected NET để hiển thị cùng revenue
    const normalPaidNet = grossToNet(normalPaidGross, invSubtotal, invTotal);

    // hold total theo invoice (GROSS)
    const holdTotal = calcHoldTotal(inv);

    // holdOutstanding: nếu warrantyHold.status = PAID/VOID => 0
    const holdStatus = String(inv.warrantyHold?.status || "").toUpperCase();
    const holdOutstanding =
      holdStatus === "PAID" || holdStatus === "VOID"
        ? 0
        : clamp0(holdTotal - holdPaidGross);

    // normalOutstanding: phần còn lại (total - holdTotal) trừ NORMAL đã thu (GROSS)
    const normalBase = clamp0(invTotal - holdTotal);
    const normalOutstanding = clamp0(normalBase - normalPaidGross);

    const totalOutstanding = normalOutstanding + holdOutstanding;

    sumRevenue += invSubtotal;
    sumCollectedNet += normalPaidNet;
    sumCollectedGross += normalPaidGross;

    sumNormalOutstanding += normalOutstanding;
    sumHoldOutstanding += holdOutstanding;
    sumTotalOutstanding += totalOutstanding;

    // Resolve partner info: prefer partner relation, fallback snapshot on invoice
    const customerName = String(inv.partner?.name || inv.partnerName || "Khách lẻ");
    const phone =
      (inv.partner?.phone != null ? String(inv.partner.phone) : null) ??
      (inv.partnerPhone != null ? String(inv.partnerPhone) : null);
    const address =
      (inv.partner?.address != null ? String(inv.partner.address) : null) ??
      (inv.partnerAddr != null ? String(inv.partnerAddr) : null);
    const taxCode =
      (inv.partner?.taxCode != null ? String(inv.partner.taxCode) : null) ??
      (inv.partnerTax != null ? String(inv.partnerTax) : null);
    const email = inv.partner?.email != null ? String(inv.partner.email) : null;

    return {
      invoiceId: String(inv.id),
      invoiceCode: String(inv.code),

      // ✅ hiển thị theo issueDate (nếu null thì fallback approvedAt)
      issueDate: formatDateVN(new Date(inv.issueDate || inv.approvedAt)),

      partnerId: inv.partnerId ? String(inv.partnerId) : null,
      customerName,

      customerPhone: phone,
      customerAddress: address,
      customerTaxCode: taxCode,
      customerEmail: email,

      subtotal: Math.round(invSubtotal),
      totalAmount: Math.round(invTotal),

      // ✅ collected NET (đã thu chưa VAT)
      collected: Math.round(normalPaidNet),
      // optional: đối soát kế toán
      collectedGross: Math.round(normalPaidGross),

      // legacy fields
      outstanding: Math.round(totalOutstanding),

      // ✅ NEW split (GROSS)
      normalOutstanding: Math.round(normalOutstanding),
      holdOutstanding: Math.round(holdOutstanding),
      totalOutstanding: Math.round(totalOutstanding),

      paymentStatus: String(inv.paymentStatus || ""),
      warrantyDueDate: inv.warrantyDueDate
        ? new Date(inv.warrantyDueDate).toISOString().slice(0, 10)
        : null,
    };
  });

  const debts = invoiceRows
    .filter((x) => x.totalOutstanding > 0)
    .sort((a, b) => {
      const pa = a.issueDate.split("/").reverse().join("-");
      const pb = b.issueDate.split("/").reverse().join("-");
      return pa < pb ? 1 : -1;
    });

  // 4) Trend by approvedAt (sum subtotal per day) ✅ NET (giữ nhất quán với filter)
  const trendMap = new Map<string, number>();
  for (const inv of invoices as any[]) {
    const k = ymd(new Date(inv.approvedAt));
    trendMap.set(k, (trendMap.get(k) || 0) + num(inv.subtotal));
  }

  const trend: TrendRow[] = Array.from(trendMap.entries())
    .sort(([a], [b]) => (a < b ? -1 : 1))
    .map(([k, v]) => {
      const [, mm, dd] = k.split("-").map((x) => Number(x));
      return { date: `${pad2(dd)}/${pad2(mm)}`, revenue: Math.round(v) };
    });

  // 5) Customers aggregation (from debts) — split normal/hold (GROSS)
  const customerMap = new Map<string, CustomerAgg>();

  for (const d of debts) {
    const key = d.partnerId || d.customerName || "WALKIN";
    const cur = customerMap.get(key) || {
      customerKey: key,
      partnerId: d.partnerId,
      name: d.customerName,
      phone: d.customerPhone,
      address: d.customerAddress,
      taxCode: d.customerTaxCode,
      email: d.customerEmail,

      outstanding: 0,

      normalOutstanding: 0,
      holdOutstanding: 0,
      totalOutstanding: 0,

      invoiceCount: 0,
      avgOutstanding: 0,
    };

    cur.normalOutstanding += num(d.normalOutstanding);
    cur.holdOutstanding += num(d.holdOutstanding);
    cur.totalOutstanding += num(d.totalOutstanding);

    // legacy
    cur.outstanding += num(d.outstanding);

    cur.invoiceCount += 1;

    if (!cur.phone && d.customerPhone) cur.phone = d.customerPhone;
    if (!cur.address && d.customerAddress) cur.address = d.customerAddress;
    if (!cur.taxCode && d.customerTaxCode) cur.taxCode = d.customerTaxCode;
    if (!cur.email && d.customerEmail) cur.email = d.customerEmail;

    customerMap.set(key, cur);
  }

  const customers = Array.from(customerMap.values())
    .map((c) => ({
      ...c,
      outstanding: Math.round(c.totalOutstanding), // legacy = total (GROSS)
      normalOutstanding: Math.round(c.normalOutstanding),
      holdOutstanding: Math.round(c.holdOutstanding),
      totalOutstanding: Math.round(c.totalOutstanding),
      avgOutstanding: c.invoiceCount > 0 ? Math.round(c.totalOutstanding / c.invoiceCount) : 0,
    }))
    .sort((a, b) => b.totalOutstanding - a.totalOutstanding);

  const summary = {
    revenue: Math.round(sumRevenue), // NET
    collected: Math.round(sumCollectedNet), // ✅ NET (đã thu chưa VAT)
    collectedGross: Math.round(sumCollectedGross), // optional đối soát

    // legacy
    outstanding: Math.round(sumTotalOutstanding), // GROSS

    // ✅ NEW split (GROSS)
    normalOutstanding: Math.round(sumNormalOutstanding),
    holdOutstanding: Math.round(sumHoldOutstanding),
    totalOutstanding: Math.round(sumTotalOutstanding),

    orderCount: invoices.length,
  };

  return res.json({
    period: {
      month,
      year,
      from: start.toISOString(),
      to: new Date(endExclusive.getTime() - 1).toISOString(),
    },
    summary,
    trend,
    invoices: invoiceRows,
    debts,
    customers,
  });
});

export default router;
