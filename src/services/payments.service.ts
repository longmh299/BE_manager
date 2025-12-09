// src/services/payments.service.ts
import { PrismaClient, PaymentStatus, PaymentType } from "@prisma/client";

const prisma = new PrismaClient();

export type CreatePaymentInput = {
  date: string; // YYYY-MM-DD
  partnerId: string;
  type: PaymentType | "RECEIPT" | "PAYMENT";
  amount: number;
  method?: string;
  refNo?: string;
  note?: string;
  createdById?: string;
  allocations?: { invoiceId: string; amount: number }[];
};

export async function createPaymentWithAllocations(input: CreatePaymentInput) {
  const {
    date,
    partnerId,
    type,
    amount,
    method,
    refNo,
    note,
    createdById,
    allocations,
  } = input;

  if (!date || !partnerId || !type || !amount) {
    throw new Error("Thiếu dữ liệu bắt buộc");
  }

  if (type !== "RECEIPT" && type !== "PAYMENT") {
    throw new Error("Loại phiếu không hợp lệ");
  }

  if (Number(amount) <= 0) {
    throw new Error("Số tiền phải > 0");
  }

  const result = await prisma.$transaction(async (tx) => {
    // 1. Tạo phiếu thu/chi
    const payment = await tx.payment.create({
      data: {
        date: new Date(date),
        partnerId,
        type: type as PaymentType,
        amount,
        method,
        refNo,
        note,
        createdById,
      },
    });

    const allocs = allocations?.filter(
      (a) => a.invoiceId && Number(a.amount) > 0
    );

    // 2. Tạo allocations + cập nhật hóa đơn (paidAmount / paymentStatus)
    if (allocs && allocs.length > 0) {
      for (const a of allocs) {
        await tx.paymentAllocation.create({
          data: {
            paymentId: payment.id,
            invoiceId: a.invoiceId,
            amount: a.amount,
          },
        });
      }

      const invoiceIds = Array.from(new Set(allocs.map((a) => a.invoiceId)));

      for (const invoiceId of invoiceIds) {
        const inv = await tx.invoice.findUnique({
          where: { id: invoiceId },
        });
        if (!inv) continue;

        const agg = await tx.paymentAllocation.aggregate({
          where: { invoiceId },
          _sum: { amount: true },
        });

        const paidAmount = Number(agg._sum.amount ?? 0);
        const total = Number(inv.total);

        let paymentStatus: PaymentStatus = "UNPAID";
        if (paidAmount <= 0) paymentStatus = "UNPAID";
        else if (paidAmount < total) paymentStatus = "PARTIAL";
        else paymentStatus = "PAID";

        await tx.invoice.update({
          where: { id: invoiceId },
          data: {
            paidAmount,
            paymentStatus,
          },
        });
      }
    }

    return payment;
  });

  return result;
}

export type ListPaymentsParams = {
  partnerId?: string;
  type?: "RECEIPT" | "PAYMENT";
  from?: string;
  to?: string;
};

export async function listPayments(params: ListPaymentsParams) {
  const { partnerId, type, from, to } = params;

  const where: any = {};
  if (partnerId) where.partnerId = partnerId;
  if (type && (type === "RECEIPT" || type === "PAYMENT")) where.type = type;

  if (from || to) {
    where.date = {};
    if (from) where.date.gte = new Date(from);
    if (to) {
      const toDate = new Date(to);
      toDate.setHours(23, 59, 59, 999);
      where.date.lte = toDate;
    }
  }

  const payments = await prisma.payment.findMany({
    where,
    orderBy: { date: "desc" },
    include: {
      partner: true,
      createdBy: true,
      allocations: {
        include: {
          invoice: {
            select: { id: true, code: true, issueDate: true, total: true },
          },
        },
      },
    },
  });

  return payments;
}

export async function getPaymentById(id: string) {
  const payment = await prisma.payment.findUnique({
    where: { id },
    include: {
      partner: true,
      createdBy: true,
      allocations: {
        include: {
          invoice: {
            select: { id: true, code: true, issueDate: true, total: true },
          },
        },
      },
    },
  });

  return payment;
}
