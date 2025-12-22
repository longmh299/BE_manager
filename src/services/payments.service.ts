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

  // ✅ allocation.amount: NORMAL có thể âm (refund), WARRANTY_HOLD chỉ dương
  allocations?: {
    invoiceId: string;
    amount: number;
    kind?: AllocationKind | "NORMAL" | "WARRANTY_HOLD";
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

function kindOf(x: any): AllocationKind {
  const k = String(x ?? "NORMAL").toUpperCase();
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

function computeDerivedHold(params: { total: number; hasHold: boolean; pct: number }) {
  const total = roundMoney(params.total || 0);
  if (!params.hasHold) return { pct: 0, holdAmount: 0, collectible: total };
  const pct =
    Number.isFinite(params.pct) && params.pct > 0 ? params.pct : 5; // default 5%
  const holdAmount = roundMoney((total * pct) / 100);
  const collectible = Math.max(0, roundMoney(total - holdAmount));
  return { pct, holdAmount, collectible };
}

/**
 * Create payment + allocations and update invoices
 * ✅ supports auditCtx for logging (userId/userRole/meta)
 */
export async function createPaymentWithAllocations(
  input: CreatePaymentInput,
  auditCtx?: AuditCtx
) {
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
      amount: num(a.amount), // ✅ signed for NORMAL; HOLD always positive
      kind: kindOf(a.kind),
    }));

  // ========= validate allocation sign rules =========
  for (const a of rawAllocs) {
    if (a.kind === "WARRANTY_HOLD") {
      if (a.amount <= 0) throw new Error("Phân bổ WARRANTY_HOLD phải là số dương.");
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
    const holdSum = rawAllocs
      .filter((x) => x.kind === "WARRANTY_HOLD")
      .reduce((s, x) => s + Math.abs(x.amount), 0);

    if (type === "RECEIPT") {
      const normalSum = rawAllocs
        .filter((x) => x.kind === "NORMAL")
        .reduce((s, x) => s + x.amount, 0); // >=0
      const expected = normalSum + holdSum;
      if (!nearlyEqual(expected, num(amount))) {
        throw new Error(
          `Tổng phân bổ (NORMAL + HOLD) = ${expected} phải bằng số tiền phiếu = ${num(amount)}.`
        );
      }
    } else {
      // PAYMENT: normal âm => cash = abs(normal)
      const normalAbs = rawAllocs
        .filter((x) => x.kind === "NORMAL")
        .reduce((s, x) => s + Math.abs(x.amount), 0);
      const expected = normalAbs + holdSum;
      if (!nearlyEqual(expected, num(amount))) {
        throw new Error(
          `Tổng phân bổ (abs(NORMAL) + HOLD) = ${expected} phải bằng số tiền phiếu = ${num(amount)}.`
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

      // Snapshot audit input summary (small)
      const allocSummary = {
        invoiceCount: Array.from(new Set(rawAllocs.map((a) => a.invoiceId))).length,
        normalSigned: sumBy(
          rawAllocs.filter((a) => a.kind === "NORMAL"),
          (a) => a.amount
        ),
        hold: sumBy(
          rawAllocs.filter((a) => a.kind === "WARRANTY_HOLD"),
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
            preview: rawAllocs.slice(0, 20).map((a) => ({
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

        // Load invoices (total + warrantyHold + partner + type + status)
        const invs = await tx.invoice.findMany({
          where: { id: { in: invoiceIds } },
          select: {
            id: true,
            code: true,
            type: true,
            status: true,
            partnerId: true,
            total: true,

            paidAmount: true,
            paymentStatus: true,

            hasWarrantyHold: true,
            warrantyHoldPct: true,
            warrantyHoldAmount: true,
            warrantyDueDate: true,
          },
        });

        if (invs.length !== invoiceIds.length) {
          const existIds = new Set(invs.map((i) => i.id));
          const missing = invoiceIds.filter((id) => !existIds.has(id));
          throw new Error(`Invoice không tồn tại: ${missing.join(", ")}`);
        }

        // ✅ invoice phải APPROVED mới cho thu/chi phân bổ
        for (const inv of invs) {
          if (inv.status !== ("APPROVED" as InvoiceStatus)) {
            throw new Error(`Hoá đơn ${inv.code || inv.id} chưa DUYỆT nên không thể thu/chi.`);
          }
        }

        // ✅ partner must match
        for (const inv of invs) {
          if (inv.partnerId && inv.partnerId !== partnerId) {
            throw new Error(`Partner của phiếu không khớp với hoá đơn ${inv.code || inv.id}.`);
          }
        }

        // ✅ Option A: allocation NORMAL âm chỉ được gắn vào invoice SALES (gốc)
        for (const a of rawAllocs) {
          if (a.kind === "NORMAL" && a.amount < 0) {
            const inv = invs.find((x) => x.id === a.invoiceId)!;
            if (inv.type !== "SALES") {
              throw new Error(
                "Hoàn tiền (NORMAL âm) theo phương án A chỉ được phân bổ vào hoá đơn SALES gốc."
              );
            }
          }
        }

        // ✅ WARRANTY_HOLD chỉ áp cho hoá đơn SALES có hasWarrantyHold=true
        for (const a of rawAllocs) {
          if (a.kind === "WARRANTY_HOLD") {
            const inv = invs.find((x) => x.id === a.invoiceId)!;
            if (inv.type !== "SALES") {
              throw new Error("WARRANTY_HOLD chỉ áp dụng cho hoá đơn BÁN (SALES).");
            }
            if (inv.hasWarrantyHold !== true) {
              throw new Error(
                `Hoá đơn ${inv.code || inv.id} không có bảo hành, không thể phân bổ WARRANTY_HOLD.`
              );
            }
          }
        }

        // Map invoice meta (derive hold nếu legacy bị 0)
        const invMap = new Map(
          invs.map((i) => {
            const total = num(i.total);
            const hasHold = i.hasWarrantyHold === true;
            const holdDb = num(i.warrantyHoldAmount);
            const pct = num(i.warrantyHoldPct);
            const derived = computeDerivedHold({ total, hasHold, pct });
            const holdAmount = hasHold ? (holdDb > 0 ? holdDb : derived.holdAmount) : 0;
            const collectible = Math.max(0, roundMoney(total - holdAmount));
            return [
              i.id,
              {
                code: i.code,
                type: i.type as InvoiceType,
                total,
                hasHold,
                pct: hasHold ? (pct > 0 ? pct : 5) : 0,
                holdAmount,
                collectible,
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
            total: meta.total,
            hasHold: meta.hasHold,
            holdAmount: meta.holdAmount,
            collectible: meta.collectible,
            paidAmount: num(inv.paidAmount),
            paymentStatus: String(inv.paymentStatus || ""),
          };
        });

        // Existing sums by invoiceId & kind
        const existing = await tx.paymentAllocation.groupBy({
          by: ["invoiceId", "kind"],
          where: { invoiceId: { in: invoiceIds } },
          _sum: { amount: true },
        });

        const existingMap = new Map<string, { normal: number; hold: number }>();
        for (const e of existing) {
          const cur = existingMap.get(e.invoiceId) || { normal: 0, hold: 0 };
          const s = num(e._sum.amount);
          if (e.kind === "WARRANTY_HOLD") cur.hold = s;
          else cur.normal = s; // ✅ can be signed sum
          existingMap.set(e.invoiceId, cur);
        }

        // New sums in request
        const newMap = new Map<string, { normal: number; hold: number }>();
        for (const a of rawAllocs) {
          const cur = newMap.get(a.invoiceId) || { normal: 0, hold: 0 };
          if (a.kind === "WARRANTY_HOLD") cur.hold += a.amount; // always positive
          else cur.normal += a.amount; // signed
          newMap.set(a.invoiceId, cur);
        }

        // Validate caps per invoice (allow refund => normal giảm, nhưng không < 0)
        for (const invoiceId of invoiceIds) {
          const meta = invMap.get(invoiceId);
          if (!meta) throw new Error(`Invoice ${invoiceId} không tồn tại`);

          const ex = existingMap.get(invoiceId) || { normal: 0, hold: 0 };
          const nw = newMap.get(invoiceId) || { normal: 0, hold: 0 };

          const nextNormal = ex.normal + nw.normal; // ✅ can decrease
          const nextHold = ex.hold + nw.hold;

          // ✅ NORMAL không được < 0 (refund không vượt số đã thu)
          if (nextNormal < -0.0001) {
            throw new Error(
              `Hoàn tiền vượt quá số đã thu NORMAL. Sau giao dịch, NORMAL sẽ thành ${nextNormal} (< 0).`
            );
          }

          // ✅ NORMAL không vượt collectible
          if (nextNormal > meta.collectible + 0.0001) {
            throw new Error(
              `Số tiền phân bổ (NORMAL) vượt quá số cần thu. Được thu tối đa ${meta.collectible}, hiện tại sẽ thành ${nextNormal}.`
            );
          }

          // ✅ HOLD chỉ khi invoice có hold + không vượt holdAmount
          if (nextHold > 0 && !meta.hasHold) {
            throw new Error("Hoá đơn không có bảo hành, không thể phân bổ khoản WARRANTY_HOLD.");
          }
          if (meta.hasHold) {
            if (nextHold > meta.holdAmount + 0.0001) {
              throw new Error(
                `Số tiền phân bổ (WARRANTY_HOLD) vượt quá số bảo hành treo. Tối đa ${meta.holdAmount}, hiện tại sẽ thành ${nextHold}.`
              );
            }
          }
        }

        // Persist allocations (allow signed)
        await tx.paymentAllocation.createMany({
          data: rawAllocs.map((a) => ({
            paymentId: payment.id,
            invoiceId: a.invoiceId,
            amount: toDec(a.amount), // ✅ signed
            kind: a.kind,
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
          if (s.kind === "WARRANTY_HOLD") cur.hold = s._sum.amount ?? new Prisma.Decimal(0);
          else cur.normal = s._sum.amount ?? new Prisma.Decimal(0);
          sum2Map.set(s.invoiceId, cur);
        }

        const invAfterForAudit: any[] = [];

        for (const inv of invs) {
          const meta = invMap.get(inv.id)!;

          const sums = sum2Map.get(inv.id) || {
            normal: new Prisma.Decimal(0),
            hold: new Prisma.Decimal(0),
          };

          const paidNormalNum = num(sums.normal); // signed sum, validated >= 0 overall
          const paidHoldNum = num(sums.hold); // >= 0

          // ✅ clamp normal/hold trong range (an toàn)
          const paidNormalClamped = clamp(paidNormalNum, 0, meta.collectible);
          const paidHoldClamped = clamp(paidHoldNum, 0, meta.holdAmount);

          // ✅ invoice.paidAmount = NORMAL đã thu (không cộng HOLD)
          const invoicePaidAmount = paidNormalClamped;

          let paymentStatus: PaymentStatus = "UNPAID";
          if (paidNormalClamped <= 0) paymentStatus = "UNPAID";
          else if (paidNormalClamped + 0.0001 < meta.collectible) paymentStatus = "PARTIAL";
          else paymentStatus = "PAID";

          await tx.invoice.update({
            where: { id: inv.id },
            data: {
              paidAmount: toDec(invoicePaidAmount),
              paymentStatus,
            },
          });

          // ✅ Update WarrantyHold row + audit (nếu có hold)
          if (meta.hasHold) {
            // invoiceId đang unique trong WarrantyHold model
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

            // nếu legacy chưa có row thì tạo (robust)
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
              },
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

            // ✅ AUDIT WarrantyHold change (chỉ log khi userId có)
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
                paidHoldClamped,
                holdAmount: meta.holdAmount,
              }),
            });
          }

          invAfterForAudit.push({
            invoiceId: inv.id,
            code: meta.code,
            total: meta.total,
            hasHold: meta.hasHold,
            holdAmount: meta.holdAmount,
            collectible: meta.collectible,
            paidNormal: paidNormalClamped,
            paidHold: paidHoldClamped,
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
          before: {
            invoices: invBeforeForAudit,
          },
          after: {
            invoices: invAfterForAudit,
          },
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
              hasWarrantyHold: true,
              warrantyHoldPct: true,
              warrantyHoldAmount: true,
              warrantyDueDate: true,
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
              hasWarrantyHold: true,
              warrantyHoldPct: true,
              warrantyHoldAmount: true,
              warrantyDueDate: true,
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
              hasWarrantyHold: true,
              warrantyHoldPct: true,
              warrantyHoldAmount: true,
              warrantyDueDate: true,
            },
          },
        },
      },
    },
  });
}
