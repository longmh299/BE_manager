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

  // ✅ allocation.amount: signed (RECEIPT + / PAYMENT -)
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
 * - Accept legacy "HOLD" and normalize to "WARRANTY_HOLD"
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
 * Hold (chỉ cho SALES)
 * - collectible NORMAL = baseTotal - holdAmount
 */
function computeHoldForSales(inv: {
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

  // legacy pct on baseSubtotal
  const pct = Math.max(0, num(inv.warrantyHoldPct));
  const holdAmount = pct > 0 ? Math.min(roundMoney((baseSubtotal * pct) / 100), baseSubtotal) : 0;

  return {
    hasHold: true,
    holdAmount,
    collectible: Math.max(0, roundMoney(baseTotal - holdAmount)),
    source: holdAmount > 0 ? ("PERCENT_LEGACY" as const) : ("ZERO" as const),
  };
}

function computePaidNormalFromSigned(invType: InvoiceType, normalSigned: number) {
  // ✅ Convention:
  // - SALES: receipt allocations + ; refund allocations - => paid = max(0, signed)
  // - PURCHASE: payments allocations - ; refund allocations + => paid = max(0, -signed)
  if (invType === "PURCHASE") return Math.max(0, roundMoney(-normalSigned));
  return Math.max(0, roundMoney(normalSigned));
}

function computePaidHoldFromSigned(holdSigned: number) {
  // hold only makes sense as net >= 0 (refund reduces)
  return Math.max(0, roundMoney(holdSigned));
}

/**
 * ✅ FIX legacy: PURCHASE allocations NORMAL bị lưu dương (sai quy ước)
 * Nếu thấy NORMAL allocations của PURCHASE "toàn dương" => flip sang âm 1 lần.
 */
async function fixLegacyPurchaseNormalSigns(
  tx: Prisma.TransactionClient,
  invs: Array<{ id: string; type: InvoiceType }>
) {
  const purchaseIds = invs.filter((x) => x.type === "PURCHASE").map((x) => x.id);
  if (purchaseIds.length === 0) return;

  const rows = await tx.paymentAllocation.findMany({
    where: { invoiceId: { in: purchaseIds }, kind: "NORMAL" as any },
    select: { id: true, invoiceId: true, amount: true },
  });

  const byInv = new Map<string, { pos: number; neg: number; ids: string[] }>();
  for (const r of rows) {
    const v = num(r.amount);
    const cur = byInv.get(r.invoiceId) || { pos: 0, neg: 0, ids: [] as string[] };
    if (v > 0) cur.pos += 1;
    if (v < 0) cur.neg += 1;
    cur.ids.push(r.id);
    byInv.set(r.invoiceId, cur);
  }

  const needFlipIds: string[] = [];
  for (const [invoiceId, s] of byInv.entries()) {
    // chỉ flip khi "toàn dương" (pos>0 && neg==0)
    if (s.pos > 0 && s.neg === 0) {
      needFlipIds.push(invoiceId);
    }
  }
  if (needFlipIds.length === 0) return;

  const toFlip = rows.filter((r) => needFlipIds.includes(r.invoiceId));
  for (const r of toFlip) {
    const v = num(r.amount);
    if (v === 0) continue;
    await tx.paymentAllocation.update({
      where: { id: r.id },
      data: { amount: toDec(-Math.abs(v)) },
    });
  }
}

/** ======================= main service ======================= **/

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
      if (type === "RECEIPT" && a.amount <= 0) {
        throw new Error("Phiếu THU (RECEIPT): phân bổ HOLD phải là số dương.");
      }
      if (type === "PAYMENT" && a.amount >= 0) {
        throw new Error("Phiếu CHI (PAYMENT): phân bổ HOLD phải là số âm (hoàn HOLD).");
      }
    } else {
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
        throw new Error(`Tổng phân bổ (signed) = ${expected} phải bằng số tiền phiếu = ${num(amount)}.`);
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
        normalSigned: sumBy(rawAllocs.filter((a) => !isHoldKind(a.kind)), (a) => a.amount),
        holdSigned: sumBy(rawAllocs.filter((a) => isHoldKind(a.kind)), (a) => a.amount),
        normalAbs: sumBy(rawAllocs.filter((a) => !isHoldKind(a.kind)), (a) => Math.abs(a.amount)),
        holdAbs: sumBy(rawAllocs.filter((a) => isHoldKind(a.kind)), (a) => Math.abs(a.amount)),
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

        // invoice must be APPROVED + not CANCELLED
        for (const inv of invs) {
          if (inv.status !== ("APPROVED" as InvoiceStatus)) {
            throw new Error(`Hoá đơn ${inv.code || inv.id} chưa DUYỆT nên không thể thu/chi.`);
          }
          if (String(inv.status) === "CANCELLED") {
            throw new Error(`Hoá đơn ${inv.code || inv.id} đã HỦY nên không thể thu/chi.`);
          }
        }

        // partner must match
        for (const inv of invs) {
          if (inv.partnerId && inv.partnerId !== partnerId) {
            throw new Error(`Partner của phiếu không khớp với hoá đơn ${inv.code || inv.id}.`);
          }
        }

        // ✅ allow allocations for SALES + PURCHASE only (block returns)
        for (const a of rawAllocs) {
          const inv = invs.find((x) => x.id === a.invoiceId)!;
          const invType = inv.type as InvoiceType;

          if (invType !== "SALES" && invType !== "PURCHASE") {
            throw new Error("Chỉ thu/chi trực tiếp cho hoá đơn SALES/PURCHASE (không áp cho phiếu trả).");
          }

          if (isHoldKind(a.kind)) {
            // HOLD only for SALES
            if (invType !== "SALES") {
              throw new Error(`Hoá đơn ${inv.code || inv.id} không hỗ trợ HOLD (chỉ SALES).`);
            }
            if (inv.hasWarrantyHold !== true) {
              throw new Error(
                `Hoá đơn ${inv.code || inv.id} không có BH treo (hasWarrantyHold=false), không thể phân bổ HOLD.`
              );
            }
          }
        }

        // ✅ FIX legacy purchase sign (flip NORMAL positive -> negative)
        await fixLegacyPurchaseNormalSigns(
          tx,
          invs.map((x) => ({ id: x.id, type: x.type as InvoiceType }))
        );

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

          if (isHoldKind(e.kind)) cur.hold += s;
          else cur.normal += s;

          existingMap.set(e.invoiceId, cur);
        }

        // Build invoice meta
        const invMap = new Map<
          string,
          {
            code: string | null;
            invType: InvoiceType;
            collectible: number;
            holdAmount: number;
            hasHold: boolean;
            baseTotal: number;
            baseSubtotal: number;
          }
        >();

        for (const i of invs) {
          const invType = i.type as InvoiceType;
          const base = getInvoiceNetBase(i);

          if (invType === "SALES") {
            const holdInfo = computeHoldForSales({
              subtotal: i.subtotal,
              total: i.total,
              netSubtotal: i.netSubtotal,
              netTotal: i.netTotal,
              hasWarrantyHold: i.hasWarrantyHold,
              warrantyHoldAmount: i.warrantyHoldAmount,
              warrantyHoldPct: i.warrantyHoldPct,
            });

            invMap.set(i.id, {
              code: i.code,
              invType,
              baseTotal: base.baseTotal,
              baseSubtotal: base.baseSubtotal,
              hasHold: holdInfo.hasHold,
              holdAmount: holdInfo.holdAmount,
              collectible: holdInfo.collectible,
            });
          } else {
            // PURCHASE: no hold
            invMap.set(i.id, {
              code: i.code,
              invType,
              baseTotal: base.baseTotal,
              baseSubtotal: base.baseSubtotal,
              hasHold: false,
              holdAmount: 0,
              collectible: Math.max(0, roundMoney(base.baseTotal)),
            });
          }
        }

        // ✅ Pre-sync invoice fields from existing allocations (fix UI “0đ”)
        for (const inv of invs) {
          const meta = invMap.get(inv.id)!;
          const ex = existingMap.get(inv.id) || { normal: 0, hold: 0 };

          const paidNormal = clamp(
            computePaidNormalFromSigned(meta.invType, ex.normal),
            0,
            meta.collectible
          );

          let st: PaymentStatus = "UNPAID";
          if (meta.collectible <= 0.0001) st = "PAID";
          else if (paidNormal <= 0) st = "UNPAID";
          else if (paidNormal + 0.0001 < meta.collectible) st = "PARTIAL";
          else st = "PAID";

          const paidDb = num(inv.paidAmount);
          const stDb = String(inv.paymentStatus || "");

          if (!nearlyEqual(paidDb, paidNormal) || stDb !== st) {
            await tx.invoice.update({
              where: { id: inv.id },
              data: { paidAmount: toDec(paidNormal), paymentStatus: st },
            });
          }
        }

        // New sums in request
        const newMap = new Map<string, { normal: number; hold: number }>();
        for (const a of rawAllocs) {
          const cur = newMap.get(a.invoiceId) || { normal: 0, hold: 0 };
          if (isHoldKind(a.kind)) cur.hold += a.amount;
          else cur.normal += a.amount;
          newMap.set(a.invoiceId, cur);
        }

        // ✅ Validate caps per invoice using invoice type semantics
        for (const inv of invs) {
          const meta = invMap.get(inv.id)!;
          const ex = existingMap.get(inv.id) || { normal: 0, hold: 0 };
          const nw = newMap.get(inv.id) || { normal: 0, hold: 0 };

          const nextNormalSigned = ex.normal + nw.normal;
          const nextHoldSigned = ex.hold + nw.hold;

          // forbid direction flip (over-refund)
          if (meta.invType === "SALES") {
            if (nextNormalSigned < -0.0001) {
              throw new Error("Hoàn tiền vượt quá số đã thu (NORMAL).");
            }
          } else if (meta.invType === "PURCHASE") {
            if (nextNormalSigned > 0.0001) {
              throw new Error("Hoàn tiền vượt quá số đã chi (NORMAL).");
            }
          }

          const nextPaidNormal = clamp(
            computePaidNormalFromSigned(meta.invType, nextNormalSigned),
            0,
            meta.collectible
          );

          if (nextPaidNormal > meta.collectible + 0.0001) {
            const verb = meta.invType === "PURCHASE" ? "đã chi" : "đã thu";
            throw new Error(
              `Số tiền ${verb} vượt quá giá trị hoá đơn. Tối đa ${meta.collectible}, hiện tại sẽ thành ${nextPaidNormal}.`
            );
          }

          if (meta.invType === "SALES" && meta.hasHold) {
            const nextPaidHold = clamp(computePaidHoldFromSigned(nextHoldSigned), 0, meta.holdAmount);
            if (nextPaidHold > meta.holdAmount + 0.0001) {
              throw new Error(
                `Số tiền HOLD net vượt quá mức cho phép. Tối đa ${meta.holdAmount}, hiện tại sẽ thành ${nextPaidHold}.`
              );
            }
          } else {
            // non-sales: no hold
            if (Math.abs(nextHoldSigned) > 0.0001) {
              throw new Error("Hoá đơn này không hỗ trợ HOLD.");
            }
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

        // Recompute sums again and update invoices
        const sums2 = await tx.paymentAllocation.groupBy({
          by: ["invoiceId", "kind"],
          where: { invoiceId: { in: invoiceIds } },
          _sum: { amount: true },
        });

        const sum2Map = new Map<string, { normal: number; hold: number }>();
        for (const s of sums2) {
          const cur = sum2Map.get(s.invoiceId) || { normal: 0, hold: 0 };
          const v = num(s._sum.amount);
          if (isHoldKind(s.kind)) cur.hold += v;
          else cur.normal += v;
          sum2Map.set(s.invoiceId, cur);
        }

        const invAfterForAudit: any[] = [];

        for (const inv of invs) {
          const meta = invMap.get(inv.id)!;
          const sums = sum2Map.get(inv.id) || { normal: 0, hold: 0 };

          const paidNormal = clamp(
            computePaidNormalFromSigned(meta.invType, sums.normal),
            0,
            meta.collectible
          );

          let paymentStatus: PaymentStatus = "UNPAID";
          if (meta.collectible <= 0.0001) paymentStatus = "PAID";
          else if (paidNormal <= 0) paymentStatus = "UNPAID";
          else if (paidNormal + 0.0001 < meta.collectible) paymentStatus = "PARTIAL";
          else paymentStatus = "PAID";

          await tx.invoice.update({
            where: { id: inv.id },
            data: {
              paidAmount: toDec(paidNormal),
              paymentStatus,
            },
          });

          // WarrantyHold sync only for SALES
          if (meta.invType === "SALES" && meta.hasHold) {
            const paidHold = clamp(computePaidHoldFromSigned(sums.hold), 0, meta.holdAmount);

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

            const shouldPaid = meta.holdAmount > 0 && paidHold + 0.0001 >= meta.holdAmount;

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
                paidHoldNet: paidHold,
                holdAmount: meta.holdAmount,
              }),
            });
          }

          invAfterForAudit.push({
            invoiceId: inv.id,
            code: meta.code,
            invType: meta.invType,
            collectible: meta.collectible,
            paidAmount: paidNormal,
            paymentStatus,
          });
        }

        await auditLog(tx, {
          userId: auditCtx?.userId ?? createdById,
          userRole: auditCtx?.userRole,
          action: "PAYMENT_APPLY_ALLOCATIONS",
          entity: "Payment",
          entityId: payment.id,
          before: null,
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
