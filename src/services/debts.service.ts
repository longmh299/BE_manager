// src/services/debts.service.ts
import { InvoiceType, PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

export type DebtsBySaleParams = {
  from?: string;
  to?: string;
  saleUserId?: string;
};

export async function getDebtsBySale(params: DebtsBySaleParams) {
  const { from, to, saleUserId } = params;

  // ✅ chỉ lấy hóa đơn bán hàng đã lưu tồn (có ít nhất 1 movement)
  const whereInvoice: any = {
    type: InvoiceType.SALES,
    movements: { some: {} },
  };

  if (from || to) {
    whereInvoice.issueDate = {};
    if (from) whereInvoice.issueDate.gte = new Date(from);
    if (to) {
      const toDate = new Date(to);
      toDate.setHours(23, 59, 59, 999);
      whereInvoice.issueDate.lte = toDate;
    }
  }

  if (saleUserId) {
    whereInvoice.saleUserId = saleUserId;
  }

  const invoices = await prisma.invoice.findMany({
    where: whereInvoice,
    orderBy: { issueDate: "asc" },
    include: {
      partner: true,
      saleUser: true,
      lines: true,
      // không cần include movements, chỉ cần dùng trong where là đủ
      // movements: true,
    },
  });

  const rows: any[] = [];

  for (const inv of invoices) {
    const paidTotal = Number(inv.paidAmount ?? 0);
    // tổng tiền đã thu của HĐ, sẽ phân bổ dần cho từng dòng
    let remainingPaid = paidTotal;

    for (const line of inv.lines) {
      const qty = Number(line.qty);
      const price = Number(line.price ?? 0);
      const amount = Number(
        line.amount != null ? line.amount : qty * price
      );

      // phân bổ tiền đã thanh toán cho dòng này (FIFO)
      let linePaid = 0;
      if (remainingPaid > 0 && amount > 0) {
        linePaid = Math.min(amount, remainingPaid);
        remainingPaid -= linePaid;
      }
      const lineDebt = amount - linePaid;

      rows.push({
        invoiceId: inv.id,
        date: inv.issueDate.toISOString().slice(0, 10),
        customerCode: inv.partner?.code ?? "",
        customerName: inv.partner?.name ?? inv.partnerName ?? "",
        itemName: line.itemName ?? "",
        qty,
        unitPrice: price,
        amount,
        paid: linePaid, // ✅ đã thanh toán cho từng dòng
        debt: lineDebt, // ✅ nợ riêng từng dòng
        note: inv.note ?? "",
        saleUserId: inv.saleUserId ?? null,
        saleUserName:
          inv.saleUser?.username ?? inv.saleUserName ?? "(Chưa gán)",
        invoiceCode: inv.code,
      });
    }
  }

  return rows;
}

export type DebtsSummaryBySaleParams = {
  from?: string;
  to?: string;
};

export async function getDebtsSummaryBySale(
  params: DebtsSummaryBySaleParams
) {
  const { from, to } = params;

  // ✅ tổng hợp cũng chỉ lấy hóa đơn đã lưu tồn
  const whereInvoice: any = {
    type: InvoiceType.SALES,
    movements: { some: {} },
  };

  if (from || to) {
    whereInvoice.issueDate = {};
    if (from) whereInvoice.issueDate.gte = new Date(from);
    if (to) {
      const toDate = new Date(to);
      toDate.setHours(23, 59, 59, 999);
      whereInvoice.issueDate.lte = toDate;
    }
  }

  const invoices = await prisma.invoice.findMany({
    where: whereInvoice,
    include: {
      saleUser: true,
    },
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

  for (const inv of invoices) {
    const key = inv.saleUserId ?? "NO_SALE";
    if (!map[key]) {
      map[key] = {
        saleUserId: inv.saleUserId ?? null,
        saleUserName:
          inv.saleUser?.username ?? inv.saleUserName ?? "(Chưa gán sale)",
        totalAmount: 0,
        totalPaid: 0,
        totalDebt: 0,
      };
    }

    const total = Number(inv.total ?? 0);
    const paid = Number(inv.paidAmount ?? 0);
    const debt = total - paid;

    map[key].totalAmount += total;
    map[key].totalPaid += paid;
    map[key].totalDebt += debt;
  }

  return Object.values(map);
}

// ✅ update ghi chú công nợ (lưu vào invoice.note)
export async function updateDebtNote(invoiceId: string, note: string) {
  const inv = await prisma.invoice.update({
    where: { id: invoiceId },
    data: { note },
    select: { id: true, note: true },
  });

  return inv; // { id, note }
}
