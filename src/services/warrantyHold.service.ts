// src/services/warrantyHold.service.ts
import { PrismaClient, Prisma, AllocationKind, InvoiceStatus } from "@prisma/client";
import { auditLog, type AuditCtx } from "./audit.service";

/**
 * ===============================
 * Warranty Hold Service
 * ===============================
 * - Quản lý công nợ bảo hành treo (WarrantyHold)
 * - Không làm gãy nghiệp vụ chính
 * - Có audit log đầy đủ (before/after + meta)
 *
 * Quy ước:
 * - WarrantyHold gắn 1-1 với Invoice (invoiceId unique)
 * - status: OPEN | PAID | VOID
 * - PAID khi tổng allocations kind=WARRANTY_HOLD >= amount
 */

/** helpers */
function num(x: any) {
  const v = Number(x ?? 0);
  return Number.isFinite(v) ? v : 0;
}

function iso(d: any) {
  if (!d) return null;
  const x = new Date(d);
  if (Number.isNaN(x.getTime())) return null;
  return x.toISOString();
}

function snapshotWarrantyHold(wh: any) {
  if (!wh) return null;
  return {
    id: wh.id,
    invoiceId: wh.invoiceId,
    amount: num(wh.amount),
    dueDate: iso(wh.dueDate),
    status: wh.status,
    paidAt: wh.paidAt ? iso(wh.paidAt) : null,
    note: wh.note ?? null,
    createdAt: wh.createdAt ? iso(wh.createdAt) : null,
    updatedAt: wh.updatedAt ? iso(wh.updatedAt) : null,
  };
}

function mergeMeta(a: any, b: any) {
  if (!a && !b) return undefined;
  if (!a) return b;
  if (!b) return a;
  return { ...a, ...b };
}

/**
 * Tạo / cập nhật WarrantyHold khi invoice được APPROVED
 *
 * Gọi trong transaction approveInvoice
 */
export async function ensureWarrantyHoldOnApprove(
  tx: PrismaClient | Prisma.TransactionClient,
  invoiceId: string,
  audit?: AuditCtx
) {
  const inv = await tx.invoice.findUnique({
    where: { id: invoiceId },
    select: {
      id: true,
      code: true,
      status: true,
      approvedAt: true,

      type: true,
      issueDate: true,

      hasWarrantyHold: true,
      warrantyHoldPct: true,
      warrantyHoldAmount: true,
      warrantyDueDate: true,
    },
  });

  if (!inv) return;

  // Chỉ phát sinh công nợ BH khi hoá đơn đã APPROVED
  if (inv.status !== InvoiceStatus.APPROVED || !inv.approvedAt) return;

  // Chỉ SALES mới có BH treo (đúng mô hình của bạn)
  // Nếu sau này muốn mở rộng thì bỏ check type này.
  if (String(inv.type) !== "SALES") {
    // nếu có record BH cũ thì VOID cho sạch
    const existing = await tx.warrantyHold.findUnique({ where: { invoiceId: inv.id } });
    if (existing && existing.status !== "VOID") {
      const before = snapshotWarrantyHold(existing);

      const afterRow = await tx.warrantyHold.update({
        where: { invoiceId: inv.id },
        data: {
          status: "VOID",
          paidAt: null,
          note: "VOID: không áp dụng bảo hành treo cho loại hoá đơn này",
        },
      });

      await auditLog(tx as any, {
        userId: audit?.userId,
        userRole: audit?.userRole,
        action: "WARRANTY_HOLD_VOID",
        entity: "WarrantyHold",
        entityId: afterRow.id,
        before,
        after: snapshotWarrantyHold(afterRow),
        meta: mergeMeta(audit?.meta, { reason: "invoiceTypeNotSales", invoiceCode: inv.code }),
      });
    }
    return;
  }

  const holdAmt = num(inv.warrantyHoldAmount);
  const hasHold = inv.hasWarrantyHold === true && holdAmt > 0 && !!inv.warrantyDueDate;

  const existing = await tx.warrantyHold.findUnique({
    where: { invoiceId: inv.id },
  });

  // ===============================
  // ❌ Không còn BH => VOID (không delete để giữ lịch sử)
  // ===============================
  if (!hasHold) {
    if (!existing) return;

    // Nếu đang PAID/VOID thì không cần làm gì thêm
    if (existing.status === "VOID") return;

    const before = snapshotWarrantyHold(existing);

    const afterRow = await tx.warrantyHold.update({
      where: { invoiceId: inv.id },
      data: {
        status: "VOID",
        paidAt: null,
        note: "Huỷ bảo hành treo khi duyệt hoá đơn (không còn BH)",
      },
    });

    await auditLog(tx as any, {
      userId: audit?.userId,
      userRole: audit?.userRole,
      action: "WARRANTY_HOLD_VOID",
      entity: "WarrantyHold",
      entityId: afterRow.id,
      before,
      after: snapshotWarrantyHold(afterRow),
      meta: mergeMeta(audit?.meta, {
        invoiceId: inv.id,
        invoiceCode: inv.code,
        hasWarrantyHold: inv.hasWarrantyHold,
        warrantyHoldAmount: num(inv.warrantyHoldAmount),
        warrantyDueDate: inv.warrantyDueDate ? iso(inv.warrantyDueDate) : null,
      }),
    });

    return;
  }

  // ===============================
  // ✅ Có BH => UPSERT OPEN (reset paidAt)
  // ===============================
  const before = snapshotWarrantyHold(existing);

  const afterRow = await tx.warrantyHold.upsert({
    where: { invoiceId: inv.id },
    create: {
      invoiceId: inv.id,
      amount: inv.warrantyHoldAmount!,
      dueDate: inv.warrantyDueDate!,
      status: "OPEN",
      paidAt: null,
      note: "Giữ lại bảo hành",
    },
    update: {
      amount: inv.warrantyHoldAmount!,
      dueDate: inv.warrantyDueDate!,
      status: "OPEN",
      paidAt: null,
      note: existing?.note ?? "Giữ lại bảo hành",
    },
  });

  await auditLog(tx as any, {
    userId: audit?.userId,
    userRole: audit?.userRole,
    action: existing ? "WARRANTY_HOLD_UPDATE" : "WARRANTY_HOLD_CREATE",
    entity: "WarrantyHold",
    entityId: afterRow.id,
    before,
    after: snapshotWarrantyHold(afterRow),
    meta: mergeMeta(audit?.meta, {
      invoiceId: inv.id,
      invoiceCode: inv.code,
      approvedAt: inv.approvedAt ? iso(inv.approvedAt) : null,
      issueDate: inv.issueDate ? iso(inv.issueDate) : null,
      pct: num(inv.warrantyHoldPct),
    }),
  });
}

/**
 * Đóng WarrantyHold khi đã thu đủ tiền bảo hành
 *
 * Gọi sau khi create payment allocations (kind = WARRANTY_HOLD)
 *
 * NOTE:
 * - Hàm này an toàn, không throw nếu không đủ điều kiện
 * - Nhưng nếu bạn đã xử lý cập nhật OPEN/PAID trong payments.service.ts rồi
 *   thì có thể coi hàm này là "phòng hờ" cho các luồng legacy.
 */
export async function closeWarrantyHoldIfPaid(
  tx: PrismaClient | Prisma.TransactionClient,
  invoiceId: string,
  audit?: AuditCtx
) {
  const wh = await tx.warrantyHold.findUnique({
    where: { invoiceId },
  });

  if (!wh || wh.status !== "OPEN") return;

  const agg = await tx.paymentAllocation.aggregate({
    where: { invoiceId, kind: AllocationKind.WARRANTY_HOLD },
    _sum: { amount: true },
  });

  const paid = num(agg._sum.amount);
  const need = num(wh.amount);

  if (!(need > 0) || paid + 0.0001 < need) return;

  const lastPay = await tx.paymentAllocation.findFirst({
    where: { invoiceId, kind: AllocationKind.WARRANTY_HOLD },
    orderBy: { createdAt: "desc" },
    select: { payment: { select: { id: true, date: true } } },
  });

  const before = snapshotWarrantyHold(wh);

  const afterRow = await tx.warrantyHold.update({
    where: { id: wh.id },
    data: {
      status: "PAID",
      paidAt: lastPay?.payment?.date ?? new Date(),
    },
  });

  await auditLog(tx as any, {
    userId: audit?.userId,
    userRole: audit?.userRole,
    action: "WARRANTY_HOLD_PAID",
    entity: "WarrantyHold",
    entityId: afterRow.id,
    before,
    after: snapshotWarrantyHold(afterRow),
    meta: mergeMeta(audit?.meta, {
      invoiceId,
      paymentId: lastPay?.payment?.id ?? null,
      paid,
      need,
    }),
  });
}

/**
 * Lấy số tiền bảo hành còn phải thu
 * (Dùng để validate FE / BE)
 */
export async function getWarrantyHoldRemaining(
  tx: PrismaClient | Prisma.TransactionClient,
  invoiceId: string
): Promise<number> {
  const wh = await tx.warrantyHold.findUnique({
    where: { invoiceId },
    select: { amount: true, status: true },
  });

  if (!wh || wh.status !== "OPEN") return 0;

  const agg = await tx.paymentAllocation.aggregate({
    where: { invoiceId, kind: AllocationKind.WARRANTY_HOLD },
    _sum: { amount: true },
  });

  const paid = num(agg._sum.amount);
  const need = num(wh.amount);
  const remain = need - paid;

  return remain > 0 ? remain : 0;
}
