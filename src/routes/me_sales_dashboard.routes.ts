// src/routes/me_sales_dashboard.routes.ts
import { Router } from "express";
import {
  Prisma,
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
 * ✅ UTC month range
 * - start: inclusive
 * - endExclusive: exclusive (first day of next month)
 */
function monthRangeUTC(year: number, month: number) {
  const start = new Date(Date.UTC(year, month - 1, 1, 0, 0, 0, 0));
  const endExclusive = new Date(Date.UTC(year, month, 1, 0, 0, 0, 0));
  return { start, endExclusive };
}

/**
 * ✅ basis NET sau trả hàng
 * - revenue dùng netSubtotal
 * - công nợ dùng netTotal
 */
function getNetBasis(inv: any) {
  const netSubtotal = num(inv.netSubtotal ?? inv.subtotal ?? 0);
  const netTotal = num(inv.netTotal ?? inv.total ?? 0);
  return {
    netSubtotal: clamp0(netSubtotal),
    netTotal: clamp0(netTotal),
  };
}

// ✅ HOLD tính theo NET total (ưu tiên amount, fallback pct)
function calcHoldTotalNet(inv: {
  netTotal: any;
  hasWarrantyHold: boolean;
  warrantyHoldPct: any;
  warrantyHoldAmount: any;
}) {
  const netTotal = clamp0(num(inv.netTotal));
  if (!inv.hasWarrantyHold) return 0;

  const holdAmt = clamp0(num(inv.warrantyHoldAmount));
  if (holdAmt > 0.0001) return Math.min(netTotal, holdAmt);

  const pct = clamp0(num(inv.warrantyHoldPct));
  if (pct > 0.0001) {
    return clamp0((netTotal * pct) / 100);
  }
  return 0;
}

/**
 * ✅ Quy đổi GROSS -> NET theo tỷ lệ netSubtotal/netTotal
 * (để KPI "Đã thu (NET)" không bị lệch)
 */
function grossToNetByNetBasis(gross: number, netSubtotal: number, netTotal: number) {
  const g = num(gross);
  const sub = num(netSubtotal);
  const tot = num(netTotal);
  if (!Number.isFinite(g) || g === 0) return 0;
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

  // split
  normalOutstanding: number;
  holdOutstanding: number;
  totalOutstanding: number;

  invoiceCount: number;
  avgOutstanding: number;
};

type InvoiceListRow = {
  invoiceId: string;
  invoiceCode: string;
  issueDate: string;

  partnerId: string | null;
  customerName: string;

  customerPhone: string | null;
  customerAddress: string | null;
  customerTaxCode: string | null;
  customerEmail: string | null;

  subtotal: number; // ✅ NET after return (netSubtotal)
  totalAmount: number; // ✅ NET after return (netTotal)

  collected: number; // ✅ NET
  collectedGross?: number; // signed gross for reconciliation

  outstanding: number; // legacy = totalOutstanding

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
    return res.status(401).json({ code: "UNAUTHORIZED", message: "Unauthorized" });
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
   * ✅ FIX: lấy SALES approved trong tháng của NV
   * ✅ NEW: select netSubtotal/netTotal để trừ hàng trả
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

      // gross
      subtotal: true,
      total: true,

      // ✅ NET after return
      netSubtotal: true,
      netTotal: true,

      paidAmount: true,
      paymentStatus: true,

      hasWarrantyHold: true,
      warrantyHoldPct: true,
      warrantyHoldAmount: true,
      warrantyDueDate: true,
      warrantyHold: { select: { status: true } }, // OPEN | PAID | VOID
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
        collectedGross: 0,
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

  /**
   * ✅ FIX SIGN + cắt theo tháng:
   * - RECEIPT => +ABS(amount)
   * - PAYMENT => -ABS(amount)
   * - payment.date < endExclusive (chốt trong tháng)
   */
  const allocGroups: Array<{ invoice_id: string; kind: AllocationKind; sum_amt: any }> =
    await prisma.$queryRaw`
      SELECT
        pa."invoiceId" AS invoice_id,
        pa."kind"      AS kind,
        COALESCE(SUM(
          CASE
            WHEN p."type"::text = 'PAYMENT' THEN -ABS(COALESCE(pa."amount",0))
            WHEN p."type"::text = 'RECEIPT' THEN  ABS(COALESCE(pa."amount",0))
            ELSE COALESCE(pa."amount",0)
          END
        ),0) AS sum_amt
      FROM "PaymentAllocation" pa
      JOIN "Payment" p ON p."id" = pa."paymentId"
      WHERE
        pa."invoiceId" IN (${Prisma.join(invoiceIds)})
        AND pa."kind" IN ('NORMAL','WARRANTY_HOLD')
        AND p."date" >= ${start}
        AND p."date" <  ${endExclusive}
      GROUP BY 1,2
    `;

  const normalPaidByInvoice = new Map<string, number>();
  const holdPaidByInvoice = new Map<string, number>();

  for (const g of allocGroups) {
    const signed = num(g.sum_amt); // signed already
    if (g.kind === AllocationKind.WARRANTY_HOLD) {
      holdPaidByInvoice.set(g.invoice_id, signed);
    } else {
      normalPaidByInvoice.set(g.invoice_id, signed);
    }
  }

  // ===== Build invoice rows =====
  let sumRevenue = 0; // ✅ NET revenue (netSubtotal)
  let sumCollectedNet = 0; // ✅ NET collected
  let sumCollectedGross = 0; // signed gross (đối soát)

  let sumNormalOutstanding = 0;
  let sumHoldOutstanding = 0;
  let sumTotalOutstanding = 0;

  const invoiceRows: InvoiceListRow[] = invoices.map((inv: any) => {
    const { netSubtotal, netTotal } = getNetBasis(inv);

    // ✅ ignore FULL RETURN (netTotal <= 0) => coi như không phát sinh phải thu
    // (vẫn trả invoices list để FE nhìn thấy, nhưng nợ = 0)
    const isFullyReturned = netTotal <= 0.0001;

    // paid split (signed gross)
    const paidNormalSignedGross = num(normalPaidByInvoice.get(inv.id) ?? 0);
    const paidHoldSignedGross = num(holdPaidByInvoice.get(inv.id) ?? 0);

    // ✅ không cho paid âm làm "nợ tăng ảo"
    const paidNormalGross = clamp0(paidNormalSignedGross);
    const paidHoldGross = clamp0(paidHoldSignedGross);

    // collected NET để hiển thị cùng revenue (dùng tỷ lệ netSubtotal/netTotal)
    const paidNormalNet = grossToNetByNetBasis(paidNormalGross, netSubtotal, netTotal);

    // hold total theo NET
    const holdTotal = calcHoldTotalNet({
      netTotal,
      hasWarrantyHold: Boolean(inv.hasWarrantyHold),
      warrantyHoldPct: inv.warrantyHoldPct,
      warrantyHoldAmount: inv.warrantyHoldAmount,
    });

    // holdOutstanding:
    // - nếu warrantyHold.status = PAID/VOID => 0
    // - còn lại: max(0, holdTotal - paidHoldGross)
    const holdStatus = String(inv.warrantyHold?.status || "").toUpperCase();
    const holdOutstanding =
      holdStatus === "PAID" || holdStatus === "VOID" || isFullyReturned
        ? 0
        : clamp0(holdTotal - paidHoldGross);

    // ✅ tổng phải thu trong tháng này theo NET (sau trả hàng)
    const totalDebt = isFullyReturned ? 0 : clamp0(netTotal - paidNormalGross);

    // normalOutstanding = totalDebt - holdOutstanding (không âm)
    const normalOutstanding = clamp0(totalDebt - holdOutstanding);

    const totalOutstanding = normalOutstanding + holdOutstanding;

    sumRevenue += netSubtotal;
    sumCollectedNet += paidNormalNet;
    sumCollectedGross += paidNormalSignedGross; // giữ signed để đối soát

    sumNormalOutstanding += normalOutstanding;
    sumHoldOutstanding += holdOutstanding;
    sumTotalOutstanding += totalOutstanding;

    // Resolve partner info
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

      issueDate: formatDateVN(new Date(inv.issueDate || inv.approvedAt)),

      partnerId: inv.partnerId ? String(inv.partnerId) : null,
      customerName,

      customerPhone: phone,
      customerAddress: address,
      customerTaxCode: taxCode,
      customerEmail: email,

      // ✅ NET after return
      subtotal: Math.round(netSubtotal),
      totalAmount: Math.round(netTotal),

      collected: Math.round(paidNormalNet),
      collectedGross: Math.round(paidNormalSignedGross),

      outstanding: Math.round(totalOutstanding),

      normalOutstanding: Math.round(normalOutstanding),
      holdOutstanding: Math.round(holdOutstanding),
      totalOutstanding: Math.round(totalOutstanding),

      paymentStatus: String(inv.paymentStatus || ""),
      warrantyDueDate: inv.warrantyDueDate
        ? new Date(inv.warrantyDueDate).toISOString().slice(0, 10)
        : null,
    };
  });

  // debts: chỉ lấy invoice còn nợ
  const debts = invoiceRows
    .filter((x) => x.totalOutstanding > 0)
    .sort((a, b) => {
      const pa = a.issueDate.split("/").reverse().join("-");
      const pb = b.issueDate.split("/").reverse().join("-");
      return pa < pb ? 1 : -1;
    });

  // Trend theo approvedAt (NET revenue = netSubtotal)
  const trendMap = new Map<string, number>();
  for (const inv of invoices as any[]) {
    const k = ymd(new Date(inv.approvedAt));
    const netSub = num(inv.netSubtotal ?? inv.subtotal ?? 0);
    trendMap.set(k, (trendMap.get(k) || 0) + netSub);
  }

  const trend: TrendRow[] = Array.from(trendMap.entries())
    .sort(([a], [b]) => (a < b ? -1 : 1))
    .map(([k, v]) => {
      const [, mm, dd] = k.split("-").map((x) => Number(x));
      return { date: `${pad2(dd)}/${pad2(mm)}`, revenue: Math.round(v) };
    });

  // Customers aggregation (from debts)
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
      outstanding: Math.round(c.totalOutstanding),
      normalOutstanding: Math.round(c.normalOutstanding),
      holdOutstanding: Math.round(c.holdOutstanding),
      totalOutstanding: Math.round(c.totalOutstanding),
      avgOutstanding: c.invoiceCount > 0 ? Math.round(c.totalOutstanding / c.invoiceCount) : 0,
    }))
    .sort((a, b) => b.totalOutstanding - a.totalOutstanding);

  const summary = {
    revenue: Math.round(sumRevenue), // ✅ NET (after return)
    collected: Math.round(sumCollectedNet), // ✅ NET
    collectedGross: Math.round(sumCollectedGross), // signed gross for reconciliation

    outstanding: Math.round(sumTotalOutstanding),

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
