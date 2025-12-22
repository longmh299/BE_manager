// src/services/debts.service.ts
import { InvoiceStatus, InvoiceType, PrismaClient } from "@prisma/client";
import { auditLog, type AuditCtx } from "./audit.service";

const prisma = new PrismaClient();

export type DebtsBySaleParams = {
  from?: string;
  to?: string;
  saleUserId?: string;

  // ✅ tuỳ chọn: mặc định chỉ lấy APPROVED để công nợ “chốt”
  statuses?: InvoiceStatus[];
};

function toEndOfDay(d: Date) {
  const x = new Date(d);
  x.setHours(23, 59, 59, 999);
  return x;
}

function toIsoDateOnly(d: Date) {
  return d.toISOString().slice(0, 10);
}

function toNum(v: any) {
  if (v == null) return 0;
  const n = typeof v === "number" ? v : Number(String(v));
  return Number.isFinite(n) ? n : 0;
}

function calcInvoiceTotalWithTax(inv: any, subtotalFallback: number, taxFallback: number) {
  // ưu tiên total nếu đã lưu sẵn
  const candidates = [inv.totalWithTax, inv.totalAmount, inv.total, inv.grandTotal].filter(
    (x) => x != null
  );
  if (candidates.length) return toNum(candidates[0]);

  const subtotal = inv.subtotal != null ? toNum(inv.subtotal) : subtotalFallback;
  const tax = inv.tax != null ? toNum(inv.tax) : taxFallback;
  return subtotal + tax;
}

/** =========================================================
 *  CHI TIẾT CÔNG NỢ THEO SALE
 *  (READ-ONLY → KHÔNG audit)
 * ========================================================= */
export async function getDebtsBySale(params: DebtsBySaleParams) {
  const { from, to, saleUserId } = params;

  const whereInvoice: any = {
    type: InvoiceType.SALES,
    status: { in: params.statuses?.length ? params.statuses : (["APPROVED"] as InvoiceStatus[]) },
  };

  if (from || to) {
    whereInvoice.issueDate = {};
    if (from) whereInvoice.issueDate.gte = new Date(from);
    if (to) whereInvoice.issueDate.lte = toEndOfDay(new Date(to));
  }

  if (saleUserId) whereInvoice.saleUserId = saleUserId;

  const invoices = await prisma.invoice.findMany({
    where: whereInvoice,
    orderBy: { issueDate: "asc" },
    include: { partner: true, saleUser: true, lines: true },
  });

  const rows: any[] = [];

  for (const inv of invoices as any[]) {
    const paidTotal = toNum(inv.paidAmount);

    // subtotal (tiền hàng chưa VAT) - fallback sum line.amount
    let subtotal = inv.subtotal != null ? toNum(inv.subtotal) : 0;
    if (!subtotal) {
      subtotal = (inv.lines || []).reduce((sum: number, line: any) => {
        const qty = toNum(line.qty);
        const price = toNum(line.price);
        const amount = line.amount != null ? toNum(line.amount) : qty * price;
        return sum + amount;
      }, 0);
    }

    const tax = inv.tax != null ? toNum(inv.tax) : 0;
    const totalWithTax = calcInvoiceTotalWithTax(inv, subtotal, tax);

    const debtTotal = Math.max(0, totalWithTax - paidTotal);

    const lines = Array.isArray(inv.lines) ? inv.lines : [];
    if (lines.length === 0) {
      rows.push({
        invoiceId: inv.id,
        invoiceCode: inv.code,
        date: inv.issueDate ? toIsoDateOnly(inv.issueDate) : "",
        customerCode: inv.partner?.code ?? "",
        customerName: inv.partner?.name ?? inv.partnerName ?? "",
        itemName: "",
        qty: 0,
        unitPrice: 0,
        amount: subtotal,
        paid: paidTotal,
        debt: debtTotal,
        note: inv.note ?? "",
        saleUserId: inv.saleUserId ?? null,
        saleUserName: inv.saleUser?.username ?? inv.saleUserName ?? "(Chưa gán)",
        invoiceSubtotal: subtotal,
        invoiceTax: tax,
        invoiceTotal: totalWithTax,
      });
      continue;
    }

    // ✅ paid/debt chỉ nằm ở dòng đầu tiên
    lines.forEach((line: any, idx: number) => {
      const qty = toNum(line.qty);
      const price = toNum(line.price);
      const amount = line.amount != null ? toNum(line.amount) : qty * price;

      rows.push({
        invoiceId: inv.id,
        invoiceCode: inv.code,
        date: inv.issueDate ? toIsoDateOnly(inv.issueDate) : "",
        customerCode: inv.partner?.code ?? "",
        customerName: inv.partner?.name ?? inv.partnerName ?? "",
        itemName: line.itemName ?? "",
        qty,
        unitPrice: price,
        amount,
        paid: idx === 0 ? paidTotal : 0,
        debt: idx === 0 ? debtTotal : 0,
        note: inv.note ?? "",
        saleUserId: inv.saleUserId ?? null,
        saleUserName: inv.saleUser?.username ?? inv.saleUserName ?? "(Chưa gán)",
        invoiceSubtotal: subtotal,
        invoiceTax: tax,
        invoiceTotal: totalWithTax,
      });
    });
  }

  return rows;
}

/** =========================================================
 *  TỔNG HỢP CÔNG NỢ THEO SALE
 *  (READ-ONLY → KHÔNG audit)
 * ========================================================= */
export type DebtsSummaryBySaleParams = {
  from?: string;
  to?: string;
  statuses?: InvoiceStatus[];
};

export async function getDebtsSummaryBySale(params: DebtsSummaryBySaleParams) {
  const { from, to } = params;

  const whereInvoice: any = {
    type: InvoiceType.SALES,
    status: { in: params.statuses?.length ? params.statuses : (["APPROVED"] as InvoiceStatus[]) },
  };

  if (from || to) {
    whereInvoice.issueDate = {};
    if (from) whereInvoice.issueDate.gte = new Date(from);
    if (to) whereInvoice.issueDate.lte = toEndOfDay(new Date(to));
  }

  const invoices = await prisma.invoice.findMany({
    where: whereInvoice,
    include: { saleUser: true, lines: true },
  });

  const map: Record<
    string,
    {
      saleUserId: string | null;
      saleUserName: string;
      totalAmount: number;
      totalPaid: number;
      totalDebt: number;
    }
  > = {};

  for (const inv of invoices as any[]) {
    const key = inv.saleUserId ?? "NO_SALE";
    if (!map[key]) {
      map[key] = {
        saleUserId: inv.saleUserId ?? null,
        saleUserName: inv.saleUser?.username ?? inv.saleUserName ?? "(Chưa gán sale)",
        totalAmount: 0,
        totalPaid: 0,
        totalDebt: 0,
      };
    }

    let subtotal = inv.subtotal != null ? toNum(inv.subtotal) : 0;
    if (!subtotal) {
      subtotal = (inv.lines || []).reduce((sum: number, line: any) => {
        const qty = toNum(line.qty);
        const price = toNum(line.price);
        const amount = line.amount != null ? toNum(line.amount) : qty * price;
        return sum + amount;
      }, 0);
    }

    const tax = inv.tax != null ? toNum(inv.tax) : 0;
    const totalWithTax = calcInvoiceTotalWithTax(inv, subtotal, tax);

    const paid = toNum(inv.paidAmount);
    const debt = Math.max(0, totalWithTax - paid);

    map[key].totalAmount += totalWithTax;
    map[key].totalPaid += paid;
    map[key].totalDebt += debt;
  }

  return Object.values(map);
}

/** =========================================================
 *  UPDATE GHI CHÚ CÔNG NỢ
 *  (MUTATE → CÓ AUDIT)
 * ========================================================= */
export async function updateDebtNote(invoiceId: string, note: string, auditCtx?: AuditCtx) {
  return prisma.$transaction(async (tx) => {
    const before = await tx.invoice.findUnique({
      where: { id: invoiceId },
      select: { id: true, note: true },
    });

    if (!before) throw new Error("Invoice not found");

    const after = await tx.invoice.update({
      where: { id: invoiceId },
      data: { note },
      select: { id: true, note: true },
    });

    await auditLog(tx, {
      userId: auditCtx?.userId,
      userRole: auditCtx?.userRole,

      action: "DEBT_NOTE_UPDATE",
      entity: "Invoice",
      entityId: invoiceId,

      before: { note: before.note },
      after: { note: after.note },

      meta: auditCtx?.meta,
    });

    return after;
  });
}
