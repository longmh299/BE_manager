// src/services/payments.service.ts
import {
  AllocationKind,
  Prisma,
  PrismaClient,
  PaymentStatus,
  PaymentType,
  InvoiceType,
  InvoiceStatus,
} from "@prisma/client";
import { auditLog, type AuditCtx } from "./audit.service";

const prisma = new PrismaClient();

/** ======================= helpers ======================= **/

function mergeMeta(a: any, b: any) {
  if (!a && !b) return undefined;
  if (!a) return b;
  if (!b) return a;
  return { ...a, ...b };
}

export type CreatePaymentInput = {
  date: string; // YYYY-MM-DD (hoặc ISO)
  partnerId: string;
  type: PaymentType | "RECEIPT" | "PAYMENT";

  // ✅ tiền thực thu/chi (luôn dương)
  amount: number;

  // tài khoản nhận/chi tiền
  accountId?: string;

  method?: string;
  refNo?: string;
  note?: string;
  createdById?: string;

  // ✅ allocation.amount: NORMAL signed (receipt + / refund -), HOLD signed (receipt + / refund -)
  allocations?: {
    invoiceId: string;
    amount: number;
    kind?: AllocationKind | "NORMAL" | "HOLD" | "WARRANTY_HOLD";
  }[];
};

function toDec(n: any) {
  const v = Number(n ?? 0);
  return new Prisma.Decimal(isNaN(v) ? 0 : v);
}

function num(n: any): number {
  const v = Number(n ?? 0);
  return Number.isFinite(v) ? v : 0;
}

function isHoldKind(k: any) {
  const x = String(k ?? "").toUpperCase();
  return x === "WARRANTY_HOLD" || x === "HOLD";
}

/**
 * Normalize kind:
 * - Accept legacy "HOLD" and normalize to "WARRANTY_HOLD" (new name)
 * - Default: "NORMAL"
 */
function kindOf(x: any): AllocationKind | "NORMAL" | "WARRANTY_HOLD" {
  const k = String(x ?? "NORMAL").toUpperCase();
  if (k === "HOLD") return "WARRANTY_HOLD";
  if (k === "WARRANTY_HOLD") return "WARRANTY_HOLD";
  return "NORMAL";
}

function nearlyEqual(a: number, b: number, eps = 0.0001) {
  return Math.abs(a - b) <= eps;
}

function clamp(n: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, n));
}

function sumBy<T>(arr: T[], fn: (x: T) => number) {
  return arr.reduce((s, x) => s + fn(x), 0);
}

function toIsoDateOnly(d: any) {
  try {
    const x = new Date(d);
    if (Number.isNaN(x.getTime())) return String(d ?? "");
    return x.toISOString();
  } catch {
    return String(d ?? "");
  }
}

function roundMoney(n: number) {
  return Math.round((n + Number.EPSILON) * 100) / 100;
}

/** ======================= NET helpers (VERY IMPORTANT) ======================= **/

function getInvoiceNetBase(inv: {
  subtotal?: any;
  total?: any;
  netSubtotal?: any;
  netTotal?: any;
}) {
  const total = roundMoney(num(inv.total));
  const subtotal = roundMoney(num(inv.subtotal));

  const netTotal = roundMoney(num((inv as any).netTotal));
  const netSubtotal = roundMoney(num((inv as any).netSubtotal));

  const baseTotal = Number.isFinite(netTotal) && netTotal >= 0 ? netTotal : total;
  const baseSubtotal = Number.isFinite(netSubtotal) && netSubtotal >= 0 ? netSubtotal : subtotal;

  return { total, subtotal, baseTotal, baseSubtotal };
}

/**
 * Legacy fallback (chỉ dùng nếu invoice cũ chưa có warrantyHoldAmount)
 * - KHÔNG phải logic chính nữa.
 *
 * ✅ NEW RULE:
 * - warrantyHoldPct (nếu dùng legacy) áp trên SUBTOTAL (tạm tính), không phải total (gross)
 */
function legacyDerivedHold(params: { subtotal: number; hasHold: boolean; pct: number }) {
  const subtotal = roundMoney(params.subtotal || 0);
  if (!params.hasHold) return { pct: 0, holdAmount: 0 };
  const pct = Number.isFinite(params.pct) && params.pct > 0 ? params.pct : 0;
  const holdAmount = pct > 0 ? roundMoney((subtotal * pct) / 100) : 0;
  return { pct, holdAmount };
}

/**
 * Hold = số tiền treo nhập trực tiếp (warrantyHoldAmount) nếu hasWarrantyHold=true.
 * Fallback legacy:
 * - nếu hasWarrantyHold=true mà warrantyHoldAmount=0 và warrantyHoldPct>0 => derive theo pct (trên subtotal/netSubtotal)
 *
 * ✅ IMPORTANT:
 * - collectible NORMAL = baseTotal (NET after return, still includes VAT) - holdAmount
 * - holdAmount cap theo baseSubtotal
 */
function computeHoldFromInvoice(inv: {
  subtotal?: any;
  total?: any;
  netSubtotal?: any;
  netTotal?: any;
  hasWarrantyHold: any;
  warrantyHoldAmount: any;
  warrantyHoldPct: any;
}) {
  const { baseTotal, baseSubtotal } = getInvoiceNetBase(inv);

  const hasHold = inv.hasWarrantyHold === true;
  if (!hasHold) {
    return { hasHold: false, holdAmount: 0, collectible: baseTotal, source: "NONE" as const };
  }

  const holdDb = roundMoney(Math.max(0, num(inv.warrantyHoldAmount)));
  if (holdDb > 0) {
    const holdAmount = Math.min(holdDb, baseSubtotal);
    return {
      hasHold: true,
      holdAmount,
      collectible: Math.max(0, roundMoney(baseTotal - holdAmount)),
      source: "AMOUNT" as const,
    };
  }

  // legacy fallback (pct on baseSubtotal)
  const pct = num(inv.warrantyHoldPct);
  const legacy = legacyDerivedHold({ subtotal: baseSubtotal, hasHold: true, pct });
  const holdAmount = Math.min(Math.max(0, legacy.holdAmount), baseSubtotal);

  return {
    hasHold: true,
    holdAmount,
    collectible: Math.max(0, roundMoney(baseTotal - holdAmount)),
    source: holdAmount > 0 ? ("PERCENT_LEGACY" as const) : ("ZERO" as const),
  };
}

/** ======================= main service ======================= **/

/**
 * Create payment + allocations and update invoices
 * ✅ supports auditCtx for logging (userId/userRole/meta)
 *
 * Business rules (Option A upgraded):
 * - RECEIPT: NORMAL >= 0, HOLD >= 0
 * - PAYMENT (refund): NORMAL <= 0, HOLD <= 0   (để hoàn cả HOLD nếu đã thu)
 *
 * - Invoice.paidAmount = NET NORMAL (không cộng HOLD)
 * - WarrantyHold net = sum allocations kind HOLD (có thể giảm khi refund HOLD)
 */
export async function createPaymentWithAllocations(input: CreatePaymentInput, auditCtx?: AuditCtx) {
  const {
    date,
    partnerId,
    type,
    amount,
    accountId,
    method,
    refNo,
    note,
    createdById,
    allocations,
  } = input;

  if (!date || !partnerId || !type || amount == null) {
    throw new Error("Thiếu dữ liệu bắt buộc");
  }
  if (type !== "RECEIPT" && type !== "PAYMENT") {
    throw new Error("Loại phiếu không hợp lệ");
  }
  if (!Number.isFinite(Number(amount)) || Number(amount) <= 0) {
    throw new Error("Số tiền phiếu phải > 0");
  }

  const rawAllocs = (allocations || [])
    .filter((a) => a?.invoiceId && Number.isFinite(num(a.amount)) && num(a.amount) !== 0)
    .map((a) => ({
      invoiceId: String(a.invoiceId),
      amount: num(a.amount), // signed
      kind: kindOf(a.kind),
    }));

  // ========= validate allocation sign rules =========
  for (const a of rawAllocs) {
    if (isHoldKind(a.kind)) {
      // ✅ RECEIPT chỉ dương; PAYMENT chỉ âm (refund hold)
      if (type === "RECEIPT" && a.amount <= 0) {
        throw new Error("Phiếu THU (RECEIPT): phân bổ HOLD phải là số dương.");
      }
      if (type === "PAYMENT" && a.amount >= 0) {
        throw new Error("Phiếu CHI (PAYMENT): phân bổ HOLD phải là số âm (hoàn HOLD).");
      }
    } else {
      // NORMAL
      if (type === "RECEIPT" && a.amount < 0) {
        throw new Error("Phiếu THU (RECEIPT) không được có phân bổ NORMAL âm.");
      }
      if (type === "PAYMENT" && a.amount > 0) {
        throw new Error("Phiếu CHI (PAYMENT) không được có phân bổ NORMAL dương.");
      }
    }
  }

  // ========= validate total = cash amount =========
  if (rawAllocs.length > 0) {
    if (type === "RECEIPT") {
      const expected = rawAllocs.reduce((s, x) => s + num(x.amount), 0);
      if (!nearlyEqual(expected, num(amount))) {
        throw new Error(
          `Tổng phân bổ (signed) = ${expected} phải bằng số tiền phiếu = ${num(amount)}.`
        );
      }
    } else {
      const expected = rawAllocs.reduce((s, x) => s + Math.abs(num(x.amount)), 0);
      if (!nearlyEqual(expected, num(amount))) {
        throw new Error(
          `Tổng phân bổ (sum abs allocations) = ${expected} phải bằng số tiền phiếu = ${num(amount)}.`
        );
      }
    }
  }

  const paymentId = await prisma.$transaction(
    async (tx) => {
      // Validate accountId nếu có
      if (accountId) {
        const acc = await tx.paymentAccount.findUnique({
          where: { id: accountId },
          select: { id: true, isActive: true },
        });
        if (!acc) throw new Error("Tài khoản nhận/chi không tồn tại");
        if (!acc.isActive) throw new Error("Tài khoản nhận/chi đang bị khóa");
      }

      const allocSummary = {
        invoiceCount: Array.from(new Set(rawAllocs.map((a) => a.invoiceId))).length,
        normalSigned: sumBy(
          rawAllocs.filter((a) => !isHoldKind(a.kind)),
          (a) => a.amount
        ),
        holdSigned: sumBy(
          rawAllocs.filter((a) => isHoldKind(a.kind)),
          (a) => a.amount
        ),
        normalAbs: sumBy(
          rawAllocs.filter((a) => !isHoldKind(a.kind)),
          (a) => Math.abs(a.amount)
        ),
        holdAbs: sumBy(
          rawAllocs.filter((a) => isHoldKind(a.kind)),
          (a) => Math.abs(a.amount)
        ),
      };

      // 1) Create payment (amount luôn dương)
      const payment = await tx.payment.create({
        data: {
          date: new Date(date),
          partnerId,
          type: type as PaymentType,
          amount: toDec(amount),
          accountId: accountId ?? null,
          method: method ?? null,
          refNo: refNo ?? null,
          note: note ?? null,
          createdById: createdById ?? null,
        },
        select: {
          id: true,
          date: true,
          partnerId: true,
          type: true,
          amount: true,
          accountId: true,
          method: true,
          refNo: true,
          note: true,
        },
      });

      // ✅ AUDIT: payment created (before allocations)
      await auditLog(tx, {
        userId: auditCtx?.userId ?? createdById,
        userRole: auditCtx?.userRole,
        action: "PAYMENT_CREATE",
        entity: "Payment",
        entityId: payment.id,
        before: null,
        after: {
          id: payment.id,
          date: toIsoDateOnly(payment.date),
          partnerId: payment.partnerId,
          type: payment.type,
          amount: num(payment.amount),
          accountId: payment.accountId,
          method: payment.method,
          refNo: payment.refNo,
          note: payment.note,
        },
        meta: mergeMeta(auditCtx?.meta, {
          allocations: {
            ...allocSummary,
            preview: rawAllocs.slice(0, 30).map((a) => ({
              invoiceId: a.invoiceId,
              kind: a.kind,
              amount: a.amount,
            })),
          },
        }),
      });

      // 2) Allocations + update invoice (paidAmount/paymentStatus) + update WarrantyHold status (OPEN/PAID)
      if (rawAllocs.length > 0) {
        const invoiceIds = Array.from(new Set(rawAllocs.map((a) => a.invoiceId)));

        const invs = await tx.invoice.findMany({
          where: { id: { in: invoiceIds } },
          select: {
            id: true,
            code: true,
            type: true,
            status: true,
            partnerId: true,

            subtotal: true,
            total: true,
            netSubtotal: true,
            netTotal: true,

            paidAmount: true,
            paymentStatus: true,

            hasWarrantyHold: true,
            warrantyHoldPct: true,
            warrantyHoldAmount: true,
            warrantyDueDate: true,

            refInvoiceId: true,
          },
        });

        if (invs.length !== invoiceIds.length) {
          const existIds = new Set(invs.map((i) => i.id));
          const missing = invoiceIds.filter((id) => !existIds.has(id));
          throw new Error(`Invoice không tồn tại: ${missing.join(", ")}`);
        }

        // ✅ invoice must be APPROVED + not CANCELLED
        for (const inv of invs) {
          if (inv.status !== ("APPROVED" as InvoiceStatus)) {
            throw new Error(`Hoá đơn ${inv.code || inv.id} chưa DUYỆT nên không thể thu/chi.`);
          }
          if (String(inv.status) === "CANCELLED") {
            throw new Error(`Hoá đơn ${inv.code || inv.id} đã HỦY nên không thể thu/chi.`);
          }
        }

        // ✅ partner must match
        for (const inv of invs) {
          if (inv.partnerId && inv.partnerId !== partnerId) {
            throw new Error(`Partner của phiếu không khớp với hoá đơn ${inv.code || inv.id}.`);
          }
        }

        // ✅ Option A: allocations chỉ áp vào SALES gốc
        for (const a of rawAllocs) {
          const inv = invs.find((x) => x.id === a.invoiceId)!;

          if (inv.type !== "SALES") {
            throw new Error("Option A: phân bổ chỉ được áp vào hoá đơn SALES gốc.");
          }

          if (isHoldKind(a.kind)) {
            if (inv.hasWarrantyHold !== true) {
              throw new Error(
                `Hoá đơn ${inv.code || inv.id} không có BH treo (hasWarrantyHold=false), không thể phân bổ HOLD.`
              );
            }
          }
        }

        // Existing sums by invoiceId & kind (signed net)
        const existing = await tx.paymentAllocation.groupBy({
          by: ["invoiceId", "kind"],
          where: { invoiceId: { in: invoiceIds } },
          _sum: { amount: true },
        });

        const existingMap = new Map<string, { normal: number; hold: number }>();
        for (const e of existing) {
          const cur = existingMap.get(e.invoiceId) || { normal: 0, hold: 0 };
          const s = num(e._sum.amount);

          if (isHoldKind(e.kind)) cur.hold += s; // signed net hold
          else cur.normal += s; // signed net normal

          existingMap.set(e.invoiceId, cur);
        }

        // Map invoice meta (NET base + hold on netSubtotal)
        const invMap = new Map(
          invs.map((i) => {
            const base = getInvoiceNetBase(i);
            const holdInfo = computeHoldFromInvoice(i);

            const ex = existingMap.get(i.id) || { normal: 0, hold: 0 };
            const existingHoldNet = roundMoney(Math.max(0, ex.hold));
            const holdCap = roundMoney(Math.max(holdInfo.holdAmount, existingHoldNet));

            // collectible should always use NET baseTotal
            const collectible = Math.max(0, roundMoney(base.baseTotal - holdInfo.holdAmount));

            return [
              i.id,
              {
                code: i.code,
                type: i.type as InvoiceType,

                baseSubtotal: base.baseSubtotal,
                baseTotal: base.baseTotal,

                hasHold: (i.hasWarrantyHold === true) || existingHoldNet > 0.0001,
                holdAmount: holdCap, // cap for HOLD net
                collectible, // cap for NORMAL net
                holdSource:
                  holdCap > holdInfo.holdAmount
                    ? ("EXISTING_HOLD_NET_CAP" as const)
                    : (holdInfo.source as any),
              },
            ] as const;
          })
        );

        // ✅ Snapshot invoice BEFORE for audit
        const invBeforeForAudit = invs.map((inv) => {
          const meta = invMap.get(inv.id)!;
          return {
            invoiceId: inv.id,
            code: inv.code,
            baseTotal: meta.baseTotal,
            baseSubtotal: meta.baseSubtotal,
            hasHold: meta.hasHold,
            holdAmount: meta.holdAmount,
            collectible: meta.collectible,
            paidAmount: num(inv.paidAmount),
            paymentStatus: String(inv.paymentStatus || ""),
            holdSource: meta.holdSource,
          };
        });

        // New sums in request
        const newMap = new Map<string, { normal: number; hold: number }>();
        for (const a of rawAllocs) {
          const cur = newMap.get(a.invoiceId) || { normal: 0, hold: 0 };
          if (isHoldKind(a.kind)) cur.hold += a.amount; // signed
          else cur.normal += a.amount; // signed
          newMap.set(a.invoiceId, cur);
        }

        /**
         * ✅ Validate caps per invoice:
         * - For RECEIPT: enforce upper bound (không thu vượt)
         * - For PAYMENT (refund): chỉ cần không âm (không hoàn quá số đã thu)
         */
        for (const invoiceId of invoiceIds) {
          const meta = invMap.get(invoiceId);
          if (!meta) throw new Error(`Invoice ${invoiceId} không tồn tại`);

          const ex = existingMap.get(invoiceId) || { normal: 0, hold: 0 };
          const nw = newMap.get(invoiceId) || { normal: 0, hold: 0 };

          const nextNormal = ex.normal + nw.normal; // net signed
          const nextHold = ex.hold + nw.hold; // net signed

          if (nextNormal < -0.0001) {
            throw new Error(
              `Hoàn tiền NORMAL vượt quá số đã thu. Sau giao dịch, NORMAL net = ${nextNormal} (< 0).`
            );
          }
          if (nextHold < -0.0001) {
            throw new Error(
              `Hoàn tiền HOLD vượt quá số đã thu HOLD. Sau giao dịch, HOLD net = ${nextHold} (< 0).`
            );
          }

          if (type === "RECEIPT") {
            if (nextNormal > meta.collectible + 0.0001) {
              throw new Error(
                `Số tiền NORMAL net vượt quá số cần thu. Tối đa ${meta.collectible}, hiện tại sẽ thành ${nextNormal}.`
              );
            }
            if (nextHold > meta.holdAmount + 0.0001) {
              throw new Error(
                `Số tiền HOLD net vượt quá mức cho phép. Tối đa ${meta.holdAmount}, hiện tại sẽ thành ${nextHold}.`
              );
            }
          }

          if (nextHold > 0 && !meta.hasHold) {
            throw new Error("Hoá đơn không có bảo hành, không thể có phân bổ HOLD.");
          }
        }

        // Persist allocations (signed)
        await tx.paymentAllocation.createMany({
          data: rawAllocs.map((a) => ({
            paymentId: payment.id,
            invoiceId: a.invoiceId,
            amount: toDec(a.amount),
            kind: a.kind as any,
          })),
        });

        // Recompute sums again (authoritative) and update invoices
        const sums2 = await tx.paymentAllocation.groupBy({
          by: ["invoiceId", "kind"],
          where: { invoiceId: { in: invoiceIds } },
          _sum: { amount: true },
        });

        const sum2Map = new Map<string, { normal: Prisma.Decimal; hold: Prisma.Decimal }>();
        for (const s of sums2) {
          const cur = sum2Map.get(s.invoiceId) || {
            normal: new Prisma.Decimal(0),
            hold: new Prisma.Decimal(0),
          };
          if (isHoldKind(s.kind)) {
            cur.hold = (cur.hold ?? new Prisma.Decimal(0)).add(s._sum.amount ?? new Prisma.Decimal(0));
          } else {
            cur.normal = (cur.normal ?? new Prisma.Decimal(0)).add(
              s._sum.amount ?? new Prisma.Decimal(0)
            );
          }
          sum2Map.set(s.invoiceId, cur);
        }

        const invAfterForAudit: any[] = [];

        for (const inv of invs) {
          const meta = invMap.get(inv.id)!;

          const sums = sum2Map.get(inv.id) || {
            normal: new Prisma.Decimal(0),
            hold: new Prisma.Decimal(0),
          };

          const paidNormalNet = num(sums.normal); // net >= 0
          const paidHoldNet = num(sums.hold); // net >= 0

          const paidNormalClamped = clamp(paidNormalNet, 0, meta.collectible);
          const paidHoldClamped = clamp(paidHoldNet, 0, meta.holdAmount);

          // ✅ invoice.paidAmount = NET NORMAL (clamped)
          const invoicePaidAmount = paidNormalClamped;

          let paymentStatus: PaymentStatus = "UNPAID";
          if (paidNormalClamped <= 0) paymentStatus = "UNPAID";
          else if (paidNormalClamped + 0.0001 < meta.collectible) paymentStatus = "PARTIAL";
          else paymentStatus = "PAID";
          if (meta.collectible <= 0.0001) {
            paymentStatus = "PAID";
          }

          await tx.invoice.update({
            where: { id: inv.id },
            data: {
              paidAmount: toDec(invoicePaidAmount),
              paymentStatus,
            },
          });

          // ✅ WarrantyHold row sync (OPEN/PAID) based on HOLD NET
          if (meta.hasHold) {
            const holdBefore = await tx.warrantyHold.findUnique({
              where: { invoiceId: inv.id },
              select: {
                id: true,
                invoiceId: true,
                amount: true,
                dueDate: true,
                status: true,
                paidAt: true,
                note: true,
                createdAt: true,
                updatedAt: true,
              },
            });

            const holdRow =
              holdBefore ??
              (await tx.warrantyHold.create({
                data: {
                  invoiceId: inv.id,
                  amount: toDec(meta.holdAmount),
                  dueDate: inv.warrantyDueDate ?? new Date(payment.date),
                  status: "OPEN",
                  note: "Giữ lại bảo hành",
                },
                select: {
                  id: true,
                  invoiceId: true,
                  amount: true,
                  dueDate: true,
                  status: true,
                  paidAt: true,
                  note: true,
                  createdAt: true,
                  updatedAt: true,
                },
              }));

            const shouldPaid = meta.holdAmount > 0 && paidHoldClamped + 0.0001 >= meta.holdAmount;

            await tx.warrantyHold.update({
              where: { invoiceId: inv.id },
              data: {
                amount: toDec(meta.holdAmount),
                dueDate: inv.warrantyDueDate ?? holdRow.dueDate,
                status: shouldPaid ? "PAID" : "OPEN",
                paidAt: shouldPaid ? payment.date : null,
              } as any,
            });

            const holdAfter = await tx.warrantyHold.findUnique({
              where: { invoiceId: inv.id },
              select: {
                id: true,
                invoiceId: true,
                amount: true,
                dueDate: true,
                status: true,
                paidAt: true,
                note: true,
                createdAt: true,
                updatedAt: true,
              },
            });

            await auditLog(tx, {
              userId: auditCtx?.userId ?? createdById,
              userRole: auditCtx?.userRole,
              action: "WARRANTY_HOLD_UPDATE_FROM_PAYMENT",
              entity: "WarrantyHold",
              entityId: holdRow.id,
              before: holdBefore
                ? {
                    ...holdBefore,
                    amount: num(holdBefore.amount),
                    dueDate: toIsoDateOnly(holdBefore.dueDate),
                    paidAt: holdBefore.paidAt ? toIsoDateOnly(holdBefore.paidAt) : null,
                    createdAt: toIsoDateOnly(holdBefore.createdAt),
                    updatedAt: toIsoDateOnly(holdBefore.updatedAt),
                  }
                : null,
              after: holdAfter
                ? {
                    ...holdAfter,
                    amount: num(holdAfter.amount),
                    dueDate: toIsoDateOnly(holdAfter.dueDate),
                    paidAt: holdAfter.paidAt ? toIsoDateOnly(holdAfter.paidAt) : null,
                    createdAt: toIsoDateOnly(holdAfter.createdAt),
                    updatedAt: toIsoDateOnly(holdAfter.updatedAt),
                  }
                : null,
              meta: mergeMeta(auditCtx?.meta, {
                invoiceId: inv.id,
                invoiceCode: meta.code,
                paymentId: payment.id,
                paymentDate: toIsoDateOnly(payment.date),
                paidHoldNet: paidHoldClamped,
                holdAmount: meta.holdAmount,
                holdSource: meta.holdSource,
              }),
            });
          }

          invAfterForAudit.push({
            invoiceId: inv.id,
            code: meta.code,
            baseTotal: meta.baseTotal,
            baseSubtotal: meta.baseSubtotal,
            hasHold: meta.hasHold,
            holdAmount: meta.holdAmount,
            collectible: meta.collectible,
            paidNormalNet: paidNormalClamped,
            paidHoldNet: paidHoldClamped,
            invoicePaidAmount,
            paymentStatus,
          });
        }

        // ✅ AUDIT: allocations applied + invoices updated
        await auditLog(tx, {
          userId: auditCtx?.userId ?? createdById,
          userRole: auditCtx?.userRole,
          action: "PAYMENT_APPLY_ALLOCATIONS",
          entity: "Payment",
          entityId: payment.id,
          before: { invoices: invBeforeForAudit },
          after: { invoices: invAfterForAudit },
          meta: mergeMeta(auditCtx?.meta, {
            payment: {
              id: payment.id,
              type: payment.type,
              amount: num(payment.amount),
              date: toIsoDateOnly(payment.date),
              partnerId: payment.partnerId,
            },
            allocationSummary: allocSummary,
          }),
        });
      }

      return payment.id;
    },
    { timeout: 20000, maxWait: 5000 }
  );

  // Fetch OUTSIDE transaction
  return prisma.payment.findUnique({
    where: { id: paymentId },
    include: {
      partner: true,
      account: true,
      createdBy: true,
      allocations: {
        include: {
          invoice: {
            select: {
              id: true,
              code: true,
              issueDate: true,
              type: true,
              total: true,
              netTotal: true,
              hasWarrantyHold: true,
              warrantyHoldPct: true,
              warrantyHoldAmount: true,
              warrantyDueDate: true,
              refInvoiceId: true,
              status: true,
            },
          },
        },
      },
    },
  });
}

export type ListPaymentsParams = {
  partnerId?: string;
  type?: "RECEIPT" | "PAYMENT";
  accountId?: string;
  from?: string;
  to?: string;
};

export async function listPayments(params: ListPaymentsParams) {
  const { partnerId, type, accountId, from, to } = params;

  const where: any = {};
  if (partnerId) where.partnerId = partnerId;
  if (accountId) where.accountId = accountId;
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

  return prisma.payment.findMany({
    where,
    orderBy: { date: "desc" },
    include: {
      partner: true,
      account: true,
      createdBy: true,
      allocations: {
        include: {
          invoice: {
            select: {
              id: true,
              code: true,
              issueDate: true,
              type: true,
              total: true,
              netTotal: true,
              hasWarrantyHold: true,
              warrantyHoldPct: true,
              warrantyHoldAmount: true,
              warrantyDueDate: true,
              refInvoiceId: true,
              status: true,
            },
          },
        },
      },
    },
  });
}

export async function getPaymentById(id: string) {
  return prisma.payment.findUnique({
    where: { id },
    include: {
      partner: true,
      account: true,
      createdBy: true,
      allocations: {
        include: {
          invoice: {
            select: {
              id: true,
              code: true,
              issueDate: true,
              type: true,
              total: true,
              netTotal: true,
              hasWarrantyHold: true,
              warrantyHoldPct: true,
              warrantyHoldAmount: true,
              warrantyDueDate: true,
              refInvoiceId: true,
              status: true,
            },
          },
        },
      },
    },
  });
}
