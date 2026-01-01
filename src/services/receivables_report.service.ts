// src/services/receivables_report.service.ts
import {
  AllocationKind,
  InvoiceStatus,
  InvoiceType,
  Prisma,
  PrismaClient,
} from "@prisma/client";

const prisma = new PrismaClient();

type Money = Prisma.Decimal;

function D(v: any): Money {
  if (v == null) return new Prisma.Decimal(0);
  if (typeof v === "object" && typeof (v as any).toString === "function") {
    return new Prisma.Decimal((v as any).toString());
  }
  const n = Number(v);
  if (!Number.isFinite(n)) return new Prisma.Decimal(0);
  return new Prisma.Decimal(n);
}

function max0(x: Money): Money {
  return x.lessThan(0) ? new Prisma.Decimal(0) : x;
}

function pad2(n: number) {
  return String(n).padStart(2, "0");
}

function toDateOnlyLocal(d: Date) {
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
}

/**
 * ✅ asOf date-only = chốt cuối ngày theo LOCAL time (23:59:59.999)
 * Lý do: báo cáo công nợ "chốt ngày", cần tính đủ các khoản thu trong ngày đó.
 */
function parseAsOf(asOf?: string) {
  if (!asOf) return new Date();

  // yyyy-mm-dd
  if (/^\d{4}-\d{2}-\d{2}$/.test(asOf)) {
    const [yy, mm, dd] = asOf.split("-").map((x) => Number(x));
    // LOCAL end-of-day
    return new Date(yy, mm - 1, dd, 23, 59, 59, 999);
  }

  const d = new Date(asOf);
  if (Number.isNaN(d.getTime())) return new Date();
  return d;
}

/**
 * ✅ Start of month theo LOCAL time để rule "qua tháng mới lên công nợ" đúng với VN.
 */
function startOfMonthLocal(d: Date) {
  return new Date(d.getFullYear(), d.getMonth(), 1, 0, 0, 0, 0);
}

function toNum(v: any) {
  if (v == null) return 0;
  const n = typeof v === "number" ? v : Number(String(v));
  return Number.isFinite(n) ? n : 0;
}

function roundMoney(n: number) {
  return Math.round((n + Number.EPSILON) * 100) / 100;
}

/**
 * ✅ IMPORTANT FIX:
 * Schema netTotal là Decimal NOT NULL => không filter "netTotal: null".
 * => chỉ cần: netTotal > 0 là đã loại FULL RETURN (netTotal <= 0)
 */
function buildExcludeFullyReturnedWhere(): Prisma.InvoiceWhereInput {
  return {
    netTotal: { gt: 0 },
  };
}

/**
 * ✅ basis theo NET sau trả hàng + trừ BH treo
 */
function computeBasis(inv: any) {
  const netSubtotal =
    inv.netSubtotal != null
      ? roundMoney(toNum(inv.netSubtotal))
      : roundMoney(toNum(inv.subtotal));

  const netTax =
    inv.netTax != null ? roundMoney(toNum(inv.netTax)) : roundMoney(toNum(inv.tax));

  const netTotal =
    inv.netTotal != null ? roundMoney(toNum(inv.netTotal)) : roundMoney(toNum(inv.total));

  const hasHold = inv.hasWarrantyHold === true;

  let holdAmount = hasHold ? roundMoney(Math.max(0, toNum(inv.warrantyHoldAmount))) : 0;
  if (holdAmount > netSubtotal) holdAmount = netSubtotal;

  const collectible = Math.max(0, roundMoney(netTotal - holdAmount));

  return { netSubtotal, netTax, netTotal, hasHold, holdAmount, collectible };
}

/**
 * Fallback allocation logic cho data cũ:
 * - paidAmount (invoice) coi như NORMAL net
 * - WARRANTY_HOLD = 0
 */
function fallbackSplitPaidNet(paidAmount: Money) {
  return { normalPaid: max0(paidAmount), warrantyPaid: new Prisma.Decimal(0) };
}

export type PaymentHistoryRow = {
  paymentId: string;
  paymentDate: string; // ISO yyyy-mm-dd
  paymentType: string; // PaymentType
  refNo: string | null;
  allocationKind: AllocationKind;
  amount: number;
  note: string | null;
  accountName?: string | null;
  accountCode?: string | null;
};

export type ReceivableInvoiceRow = {
  invoiceId: string;
  code: string;
  issueDate: string; // yyyy-MM-dd
  partnerId: string | null;
  partnerName: string;

  saleUserId?: string | null;
  saleName?: string | null;

  netTotal: number;
  netSubtotal: number;
  netTax: number;

  hasWarrantyHold: boolean;
  warrantyHoldAmount: number;
  warrantyDueDate: string | null;

  paidTotal: number; // invoice.paidAmount (NET NORMAL)
  paidNormal: number;
  paidWarranty: number;

  normalOutstanding: number;
  warrantyOutstanding: number;

  warrantyHoldNotDue: number;
  warrantyHoldDue: number;

  totalOutstanding: number;

  paymentHistory?: PaymentHistoryRow[];
};

export type ReceivablesByPartnerRow = {
  partnerId: string | null;
  partnerName: string;

  normalOutstanding: number;
  warrantyHoldNotDue: number;
  warrantyHoldDue: number;

  totalOutstanding: number;
  invoiceCount: number;
};

export async function getReceivablesReport(params: { asOf?: string; includeRows?: boolean }) {
  const asOf = parseAsOf(params.asOf);
  const includeRows = params.includeRows !== false;

  const asOfDateOnly = params.asOf && /^\d{4}-\d{2}-\d{2}$/.test(params.asOf)
    ? params.asOf
    : toDateOnlyLocal(asOf);

  // ✅ Rule: hóa đơn qua tháng mới lên công nợ
  const periodStart = startOfMonthLocal(asOf);

  // ✅ chỉ lấy SALES APPROVED, loại FULL RETURN bằng netTotal > 0
  const invoices = await prisma.invoice.findMany({
    where: {
      type: "SALES" as InvoiceType,
      status: "APPROVED" as InvoiceStatus,
      issueDate: { lt: periodStart },
      ...buildExcludeFullyReturnedWhere(),
    },
    select: {
      id: true,
      code: true,
      issueDate: true,

      partnerId: true,
      partnerName: true,

      saleUserId: true,
      saleUserName: true,
      saleUser: { select: { id: true, username: true } },

      subtotal: true,
      tax: true,
      total: true,
      netSubtotal: true,
      netTax: true,
      netTotal: true,

      paidAmount: true,

      hasWarrantyHold: true,
      warrantyHoldAmount: true,
      warrantyDueDate: true,
    },
    orderBy: { issueDate: "desc" },
  });

  if (invoices.length === 0) {
    return {
      ok: true,
      data: {
        asOf: asOfDateOnly,
        byPartner: [] as ReceivablesByPartnerRow[],
        rows: [] as ReceivableInvoiceRow[],
        summary: {
          normalOutstanding: 0,
          warrantyHoldNotDue: 0,
          warrantyHoldDue: 0,
          totalOutstanding: 0,
          invoiceCount: 0,
        },
      },
    };
  }

  const invoiceIds = invoices.map((x) => x.id);

  /**
   * ✅ allocations theo invoiceId + kind, nhưng phải cắt theo asOf:
   * chỉ lấy Payment.date <= asOf (chốt công nợ)
   */
  const allocGroups = await prisma.paymentAllocation.groupBy({
    by: ["invoiceId", "kind"],
    where: {
      invoiceId: { in: invoiceIds },
      kind: { in: ["NORMAL" as AllocationKind, "WARRANTY_HOLD" as AllocationKind] },
      payment: { date: { lte: asOf } },
    },
    _sum: { amount: true },
  });

  const allocMap = new Map<string, { normal: Money; warranty: Money }>();
  for (const g of allocGroups) {
    const cur =
      allocMap.get(g.invoiceId) ?? {
        normal: new Prisma.Decimal(0),
        warranty: new Prisma.Decimal(0),
      };

    const sumAmt = D(g._sum.amount);

    if (g.kind === ("NORMAL" as AllocationKind)) cur.normal = cur.normal.plus(sumAmt);
    if (g.kind === ("WARRANTY_HOLD" as AllocationKind)) cur.warranty = cur.warranty.plus(sumAmt);

    allocMap.set(g.invoiceId, cur);
  }

  // ✅ payment history (chi tiết allocations) để hiển thị trong dialog thu tiền (cũng cắt theo asOf)
  const historyMap = new Map<string, PaymentHistoryRow[]>();
  if (includeRows && invoiceIds.length) {
    const allocs = await prisma.paymentAllocation.findMany({
      where: {
        invoiceId: { in: invoiceIds },
        kind: { in: ["NORMAL" as AllocationKind, "WARRANTY_HOLD" as AllocationKind] },
        payment: { date: { lte: asOf } },
      },
      select: {
        invoiceId: true,
        kind: true,
        amount: true,
        payment: {
          select: {
            id: true,
            date: true,
            type: true,
            refNo: true,
            note: true,
            account: { select: { code: true, name: true } },
          },
        },
      },
      orderBy: [{ payment: { date: "asc" } }, { createdAt: "asc" }],
    });

    for (const a of allocs as any[]) {
      const invId = a.invoiceId as string;
      const list = historyMap.get(invId) ?? [];
      list.push({
        paymentId: a.payment.id,
        paymentDate: (a.payment.date as Date).toISOString().slice(0, 10),
        paymentType: String(a.payment.type),
        refNo: a.payment.refNo ?? null,
        allocationKind: a.kind as AllocationKind,
        amount: roundMoney(toNum(a.amount)),
        note: a.payment.note ?? null,
        accountName: a.payment.account?.name ?? null,
        accountCode: a.payment.account?.code ?? null,
      });
      historyMap.set(invId, list);
    }
  }

  const byPartnerMap = new Map<string, ReceivablesByPartnerRow>();

  let sumNormal = 0;
  let sumHoldNotDue = 0;
  let sumHoldDue = 0;
  let sumTotal = 0;

  let includedInvoiceCount = 0;

  const rows: ReceivableInvoiceRow[] = [];

  for (const inv of invoices as any[]) {
    const basis = computeBasis(inv);
    if (basis.collectible <= 0.0001) continue;

    const paidTotal = D(inv.paidAmount); // fallback cho data cũ

    const holdAmount = D(basis.holdAmount);
    const hasWarrantyHold = inv.hasWarrantyHold === true && holdAmount.greaterThan(0);

    const dueDate: Date | null = inv.warrantyDueDate ?? null;

    const alloc = allocMap.get(inv.id);
    const hasAnyAllocation = alloc != null;

    let paidNormal = new Prisma.Decimal(0);
    let paidWarranty = new Prisma.Decimal(0);

    if (hasAnyAllocation) {
      paidNormal = max0(D(alloc!.normal));
      paidWarranty = max0(D(alloc!.warranty));
    } else {
      // data cũ không có allocations => không thể reconstruct theo asOf
      const split = fallbackSplitPaidNet(paidTotal);
      paidNormal = split.normalPaid;
      paidWarranty = split.warrantyPaid;
    }

    const collectibleDec = D(basis.collectible);
    const normalOutstanding = max0(collectibleDec.minus(paidNormal));
    const warrantyOutstanding = hasWarrantyHold
      ? max0(holdAmount.minus(paidWarranty))
      : new Prisma.Decimal(0);

    const matured = dueDate ? asOf.getTime() >= dueDate.getTime() : false;

    const warrantyHoldDue = matured ? warrantyOutstanding : new Prisma.Decimal(0);
    const warrantyHoldNotDue = matured ? new Prisma.Decimal(0) : warrantyOutstanding;

    const totalOutstanding = normalOutstanding.plus(warrantyOutstanding);

    // ✅ nếu invoice đã hết nợ tại asOf thì bỏ qua
    if (totalOutstanding.lessThanOrEqualTo(0)) continue;

    includedInvoiceCount += 1;

    const partnerKey = inv.partnerId ?? `__NO_PARTNER__:${inv.partnerName ?? ""}`;
    const partnerName = inv.partnerName ?? "(Không rõ đối tác)";

    const cur =
      byPartnerMap.get(partnerKey) ??
      ({
        partnerId: inv.partnerId ?? null,
        partnerName,
        normalOutstanding: 0,
        warrantyHoldNotDue: 0,
        warrantyHoldDue: 0,
        totalOutstanding: 0,
        invoiceCount: 0,
      } as ReceivablesByPartnerRow);

    const n = Number(normalOutstanding);
    const h = Number(warrantyHoldNotDue);
    const d = Number(warrantyHoldDue);
    const t = Number(totalOutstanding);

    cur.normalOutstanding += n;
    cur.warrantyHoldNotDue += h;
    cur.warrantyHoldDue += d;
    cur.totalOutstanding += t;
    cur.invoiceCount += 1;

    byPartnerMap.set(partnerKey, cur);

    sumNormal += n;
    sumHoldNotDue += h;
    sumHoldDue += d;
    sumTotal += t;

    if (includeRows) {
      const saleName = (inv.saleUserName || inv.saleUser?.username || null) as string | null;

      rows.push({
        invoiceId: inv.id,
        code: inv.code,
        issueDate: inv.issueDate.toISOString().slice(0, 10),
        partnerId: inv.partnerId ?? null,
        partnerName,

        saleUserId: inv.saleUserId ?? null,
        saleName,

        netTotal: basis.netTotal,
        netSubtotal: basis.netSubtotal,
        netTax: basis.netTax,

        hasWarrantyHold,
        warrantyHoldAmount: Number(holdAmount),
        warrantyDueDate: dueDate ? dueDate.toISOString().slice(0, 10) : null,

        paidTotal: Number(paidTotal),
        paidNormal: Number(paidNormal),
        paidWarranty: Number(paidWarranty),

        normalOutstanding: n,
        warrantyOutstanding: Number(warrantyOutstanding),

        warrantyHoldNotDue: h,
        warrantyHoldDue: d,

        totalOutstanding: t,
        paymentHistory: historyMap.get(inv.id) || [],
      });
    }
  }

  const byPartner = Array.from(byPartnerMap.values()).sort(
    (a, b) => b.totalOutstanding - a.totalOutstanding
  );

  return {
    ok: true,
    data: {
      asOf: asOfDateOnly,
      summary: {
        normalOutstanding: sumNormal,
        warrantyHoldNotDue: sumHoldNotDue,
        warrantyHoldDue: sumHoldDue,
        totalOutstanding: sumTotal,
        invoiceCount: includedInvoiceCount,
      },
      byPartner,
      rows,
    },
  };
}
