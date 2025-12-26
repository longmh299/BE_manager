// src/services/invoices.service.ts
import {
  PrismaClient,
  InvoiceType,
  Prisma,
  PaymentStatus,
  InvoiceStatus,
  MovementType,
} from "@prisma/client";
import { auditLog, type AuditCtx } from "./audit.service";
import { ensureWarrantyHoldOnApprove } from "./warrantyHold.service";

const prisma = new PrismaClient();

function mergeMeta(a: any, b: any) {
  if (!a && !b) return undefined;
  if (!a) return b;
  if (!b) return a;
  return { ...a, ...b };
}

/** ========================= Helpers ========================= **/

function httpError(status: number, message: string) {
  const err: any = new Error(message);
  err.statusCode = status;
  return err;
}

function handleUniqueInvoiceError(e: unknown) {
  if (e instanceof Prisma.PrismaClientKnownRequestError && e.code === "P2002") {
    const target = (e.meta as any)?.target;
    const targetStr = Array.isArray(target) ? target.join(",") : String(target || "");
    if (targetStr.includes("code")) {
      throw httpError(400, "Mã hoá đơn đã tồn tại");
    }
  }
  throw e;
}

async function ensureWarehouse(warehouseId?: string) {
  if (warehouseId) {
    const w = await prisma.location.findUnique({ where: { id: warehouseId } });
    if (!w) throw new Error("Warehouse not found");
    return w;
  }
  const warehouses = await prisma.location.findMany({ where: { kind: "warehouse" } });
  if (warehouses.length === 0) throw new Error("No warehouse found");
  if (warehouses.length > 1)
    throw new Error("Multiple warehouses detected. Please specify warehouseId.");
  return warehouses[0];
}

function toNum(d: Prisma.Decimal | number | string | null | undefined): number {
  if (d == null) return 0;
  if (typeof d === "number") return d;
  return Number(d.toString());
}

/** round to 2 decimals for money */
function roundMoney(n: number) {
  return Math.round((n + Number.EPSILON) * 100) / 100;
}

/** round pct to 2 decimals */
function roundPct(n: number) {
  return Math.round((n + Number.EPSILON) * 100) / 100;
}

/** parse optional number from body (accept string/number, ignore empty) */
function parseOptionalNumber(x: any): number | undefined {
  if (x === undefined || x === null) return undefined;
  if (typeof x === "string" && x.trim() === "") return undefined;
  const n = Number(x);
  if (!Number.isFinite(n)) return undefined;
  return n;
}

/**
 * tax:
 * - ưu tiên taxPercent
 * - nếu không có taxPercent thì dùng tax
 * - none => 0
 */
function calcTaxFromBody(subtotal: number, body: any): number {
  if (body) {
    const rawPercent =
      body.taxPercent !== undefined && body.taxPercent !== null ? body.taxPercent : undefined;

    if (rawPercent !== undefined && rawPercent !== "") {
      const p = Number(rawPercent);
      if (!isNaN(p) && p > 0) return Math.round((subtotal * p) / 100);
    }

    const rawTax = body.tax !== undefined && body.tax !== null ? body.tax : undefined;
    if (rawTax !== undefined && rawTax !== "") {
      const t = Number(rawTax);
      if (!isNaN(t)) return t;
    }
  }
  return 0;
}

/**
 * ✅ helper: tính holdAmount + collectible (tiền cần thu ngay)
 *
 * ✅ CHỐT:
 * - warrantyHoldAmount tính trên subtotal (không VAT)
 * - collectibleNow = total - holdAmount
 *
 * ✅ NEW:
 * - ưu tiên nhập trực tiếp warrantyHoldAmount
 * - nếu không có amount thì dùng pct
 * - nếu không có cả 2 mà hasWarrantyHold=true => fallback legacyPct (mặc định 5)
 */
function computeWarrantyHoldAndCollectible(params: {
  subtotal: number;
  total: number;
  hasWarrantyHold: boolean;

  // optional inputs
  warrantyHoldPct?: number;
  warrantyHoldAmount?: number;

  // legacy fallback
  legacyPct?: number;
}) {
  const subtotal = roundMoney(params.subtotal || 0);
  const total = roundMoney(params.total || 0);

  if (!params.hasWarrantyHold) {
    return { pct: 0, holdAmount: 0, collectible: roundMoney(total) };
  }

  const legacyPct = Number.isFinite(params.legacyPct ?? NaN) ? (params.legacyPct as number) : 5;

  const rawAmount =
    Number.isFinite(params.warrantyHoldAmount ?? NaN) && (params.warrantyHoldAmount as number) >= 0
      ? (params.warrantyHoldAmount as number)
      : undefined;

  const rawPct =
    Number.isFinite(params.warrantyHoldPct ?? NaN) && (params.warrantyHoldPct as number) >= 0
      ? (params.warrantyHoldPct as number)
      : undefined;

  let holdAmount = 0;
  let pct = 0;

  // ✅ ưu tiên nhập amount
  if (rawAmount !== undefined) {
    holdAmount = roundMoney(Math.max(0, rawAmount));
    if (holdAmount > subtotal + 0.0001) {
      throw httpError(400, `Số tiền BH treo không được vượt quá subtotal (${subtotal}).`);
    }
    pct = subtotal > 0 ? roundPct((holdAmount / subtotal) * 100) : 0;
  } else if (rawPct !== undefined && rawPct > 0) {
    if (rawPct > 100) throw httpError(400, "warrantyHoldPct không hợp lệ (0..100).");
    pct = rawPct;
    holdAmount = roundMoney((subtotal * pct) / 100);
  } else {
    // legacy fallback nếu bật treo mà không nhập gì
    pct = legacyPct;
    holdAmount = roundMoney((subtotal * pct) / 100);
  }

  const collectible = Math.max(0, roundMoney(total - holdAmount));
  return { pct, holdAmount, collectible };
}

/**
 * ✅ VAT return fallback: nếu SALES_RETURN bị lưu thiếu VAT (tax=0),
 * thì tự suy ra VAT theo tỷ lệ VAT của hóa đơn gốc.
 *
 * returnTax = returnSubtotal * (originTax / originSubtotal)
 */
function computeReturnTaxFromOrigin(params: {
  originSubtotal: number;
  originTax: number;
  returnSubtotal: number;
}) {
  const oSub = Math.max(0, roundMoney(params.originSubtotal || 0));
  const oTax = Math.max(0, roundMoney(params.originTax || 0));
  const rSub = Math.max(0, roundMoney(params.returnSubtotal || 0));

  if (oSub <= 0.0001 || oTax <= 0.0001 || rSub <= 0.0001) return 0;

  const rate = oTax / oSub;
  let rTax = roundMoney(rSub * rate);

  // cap không vượt VAT gốc
  if (rTax > oTax) rTax = oTax;
  if (rTax < 0) rTax = 0;

  return rTax;
}

/**
 * Chuẩn hoá payment (legacy: khi tạo invoice cho phép set paidAmount)
 *
 * ⚠️ IMPORTANT:
 * - Chỉ dùng cho PURCHASE / SALES (không dùng cho return types)
 * - Return types (SALES_RETURN/PURCHASE_RETURN) phải đi qua /payments theo Option A
 */
function normalizePayment(subtotal: number, tax: number, body: any) {
  const total = subtotal + tax;

  const status = (body?.paymentStatus as PaymentStatus | undefined) ?? undefined;

  const rawPaid =
    body?.paidAmount !== undefined && body?.paidAmount !== null && body?.paidAmount !== ""
      ? Number(body.paidAmount) || 0
      : undefined;

  let paidAmount = 0;
  let paymentStatus: PaymentStatus = "UNPAID";

  if (status === "UNPAID") {
    paidAmount = 0;
    paymentStatus = "UNPAID";
  } else if (status === "PAID") {
    paidAmount = total;
    paymentStatus = "PAID";
  } else if (status === "PARTIAL") {
    if (rawPaid === undefined) {
      throw httpError(400, "Chọn thanh toán một phần thì phải nhập số tiền đã trả.");
    }
    paidAmount = rawPaid;
    paymentStatus = "PARTIAL";
  } else {
    const inferredPaid = rawPaid ?? 0;
    if (inferredPaid <= 0) {
      paidAmount = 0;
      paymentStatus = "UNPAID";
    } else if (inferredPaid >= total) {
      paidAmount = total;
      paymentStatus = "PAID";
    } else {
      paidAmount = inferredPaid;
      paymentStatus = "PARTIAL";
    }
  }

  if (paidAmount < 0) throw httpError(400, "Số tiền đã trả không hợp lệ.");
  if (paidAmount > total) {
    throw httpError(400, `Số tiền đã trả (${paidAmount}) không được vượt quá tổng tiền (${total}).`);
  }

  return { total, paidAmount, paymentStatus };
}

async function validateReceiveAccountId(
  tx: Prisma.TransactionClient | PrismaClient,
  receiveAccountId?: string | null
) {
  if (receiveAccountId == null) return null;
  const id = String(receiveAccountId).trim();
  if (!id) return null;

  const acc = await tx.paymentAccount.findFirst({
    where: { id, isActive: true },
    select: { id: true },
  });
  if (!acc) throw httpError(400, "Tài khoản nhận tiền không hợp lệ hoặc đã bị khóa.");
  return acc.id;
}

/**
 * Validate & load invoice gốc cho SALES_RETURN
 */
async function requireValidRefInvoiceForSalesReturn(
  tx: Prisma.TransactionClient | PrismaClient,
  refInvoiceIdRaw: any
) {
  const refInvoiceId = String(refInvoiceIdRaw || "").trim();
  if (!refInvoiceId) {
    throw httpError(
      400,
      "Phiếu KHÁCH TRẢ HÀNG (SALES_RETURN) bắt buộc phải chọn 'Hóa đơn gốc' (refInvoiceId)."
    );
  }

  const origin = await tx.invoice.findUnique({
    where: { id: refInvoiceId },
    select: {
      id: true,
      type: true,
      status: true,
      approvedAt: true,

      partnerId: true,
      partnerName: true,
      partnerPhone: true,
      partnerTax: true,
      partnerAddr: true,

      saleUserId: true,
      saleUserName: true,
      techUserId: true,
      techUserName: true,

      receiveAccountId: true,
      code: true,
      issueDate: true,

      subtotal: true,
      tax: true,
      total: true,

      returnedSubtotal: true,
      returnedTax: true,
      returnedTotal: true,

      netSubtotal: true,
      netTax: true,
      netTotal: true,

      hasWarrantyHold: true,
      warrantyHoldPct: true,
      warrantyHoldAmount: true,

      cancelledAt: true,
    },
  });

  if (!origin) throw httpError(400, "Không tìm thấy hóa đơn gốc (refInvoiceId).");

  if (origin.type !== "SALES") {
    throw httpError(400, "Hóa đơn gốc của phiếu trả hàng phải là hóa đơn BÁN (SALES).");
  }

  if (origin.status !== "APPROVED" && origin.status !== "CANCELLED") {
    throw httpError(400, "Hóa đơn gốc chưa được DUYỆT nên chưa thể tạo/duyệt phiếu trả hàng.");
  }

  if (origin.status === "CANCELLED") {
    throw httpError(409, "Hóa đơn gốc đã bị HỦY (CANCELLED), không thể tạo thêm phiếu trả hàng.");
  }

  return origin;
}

/**
 * Compute collectible cho SALES dựa trên NET sau trả
 */
function computeCollectibleForSalesWithNet(inv: {
  subtotal: number;
  tax: number;
  total: number;
  netSubtotal?: number;
  netTotal?: number;
  hasWarrantyHold: boolean;
  warrantyHoldPct: number;
  warrantyHoldAmount: number;
}) {
  const baseTotal =
    Number.isFinite(inv.netTotal ?? NaN) && (inv.netTotal as number) >= 0
      ? roundMoney(inv.netTotal as number)
      : roundMoney(inv.total);

  const baseSubtotal =
    Number.isFinite(inv.netSubtotal ?? NaN) && (inv.netSubtotal as number) >= 0
      ? roundMoney(inv.netSubtotal as number)
      : roundMoney(inv.subtotal);

  const hasHold = inv.hasWarrantyHold === true;

  if (!hasHold) {
    return { baseSubtotal, baseTotal, pct: 0, holdAmount: 0, collectible: baseTotal };
  }

  const pct =
    Number.isFinite(inv.warrantyHoldPct) && inv.warrantyHoldPct > 0 ? inv.warrantyHoldPct : 5;

  let holdAmount =
    Number.isFinite(inv.warrantyHoldAmount) && inv.warrantyHoldAmount > 0
      ? roundMoney(inv.warrantyHoldAmount)
      : roundMoney((baseSubtotal * pct) / 100);

  if (holdAmount > baseSubtotal) holdAmount = baseSubtotal;

  const collectible = Math.max(0, roundMoney(baseTotal - holdAmount));
  return { baseSubtotal, baseTotal, pct: roundPct(pct), holdAmount, collectible };
}

/**
 * ✅ Sync invoice.paidAmount/paymentStatus từ allocations (NORMAL)
 *
 * QUY ƯỚC:
 * - invoice.paidAmount = tổng NORMAL net đã thu (>=0) cho invoice đó (refund làm giảm)
 * - với SALES: collectible dựa trên netTotal/netSubtotal sau trả (nếu có)
 * - HOLD không cộng vào paidAmount
 *
 * ✅ FIX LONG-TERM:
 * - Nếu SALES đã trả hàng FULL (netTotal <= 0) => coi như đã tất toán:
 *   paymentStatus = PAID, paidAmount = 0 (tránh bị hiện UNPAID sau refund full)
 *
 * ✅ IMPORTANT (Option A):
 * - SALES_RETURN/PURCHASE_RETURN không được dùng allocations để thể hiện refund.
 *   Refund phải apply vào SALES gốc qua /payments.
 *   => Vì vậy, return invoice sẽ luôn "UNPAID/0" (hoặc bạn có thể set PAID), nhưng không ảnh hưởng công nợ.
 */
async function syncInvoicePaidFromAllocations(tx: Prisma.TransactionClient, invoiceId: string) {
  const inv = await tx.invoice.findUnique({
    where: { id: invoiceId },
    select: {
      id: true,
      type: true,
      subtotal: true,
      tax: true,
      total: true,

      netSubtotal: true,
      netTax: true,
      netTotal: true,

      hasWarrantyHold: true,
      warrantyHoldPct: true,
      warrantyHoldAmount: true,

      status: true,
    },
  });
  if (!inv) throw httpError(404, "Invoice not found");

  // ✅ Option A: bỏ qua sync payment cho return invoice (tránh return invoice có paidAmount ảo)
  if (inv.type === "SALES_RETURN" || inv.type === "PURCHASE_RETURN") {
    await tx.invoice.update({
      where: { id: invoiceId },
      data: {
        paidAmount: new Prisma.Decimal(0),
        paymentStatus: "UNPAID",
      },
    });
    return;
  }

  const agg = await tx.paymentAllocation.aggregate({
    where: { invoiceId, kind: "NORMAL" },
    _sum: { amount: true },
  });

  const sumNormalSigned = toNum(agg._sum.amount); // signed sum (refund làm giảm)
  const paidNormalNet = Math.max(0, sumNormalSigned); // ✅ không dùng abs()

  const total = toNum(inv.total);
  const tax = toNum(inv.tax);
  const subtotal =
    toNum(inv.subtotal) > 0 ? toNum(inv.subtotal) : Math.max(0, roundMoney(total - tax));

  const netSubtotal = toNum((inv as any).netSubtotal);
  const netTotal = toNum((inv as any).netTotal);

  // ✅ FIX: FULL RETURN => netTotal = 0 => phải là PAID (đã tất toán), không phải UNPAID
  if (inv.type === "SALES" && netTotal <= 0.0001) {
    await tx.invoice.update({
      where: { id: invoiceId },
      data: {
        paidAmount: new Prisma.Decimal(0),
        paymentStatus: "PAID",
      },
    });
    return;
  }

  // ✅ SALES dùng NET để tính collectible
  let collectible = total;
  let holdPct = toNum(inv.warrantyHoldPct);
  let holdAmount = toNum(inv.warrantyHoldAmount);

  if (inv.type === "SALES") {
    const calc = computeCollectibleForSalesWithNet({
      subtotal,
      tax,
      total,
      netSubtotal: Number.isFinite(netSubtotal) ? netSubtotal : undefined,
      netTotal: Number.isFinite(netTotal) ? netTotal : undefined,
      hasWarrantyHold: inv.hasWarrantyHold === true,
      warrantyHoldPct: holdPct,
      warrantyHoldAmount: holdAmount,
    });
    collectible = calc.collectible;
    holdPct = calc.pct;
    holdAmount = calc.holdAmount;
  } else {
    collectible = total;
  }

  const paidClamped = Math.min(paidNormalNet, collectible);

  let paymentStatus: PaymentStatus = "UNPAID";
  if (paidClamped <= 0) paymentStatus = "UNPAID";
  else if (paidClamped + 0.0001 < collectible) paymentStatus = "PARTIAL";
  else paymentStatus = "PAID";

  if (collectible <= 0.0001) {
    paymentStatus = "PAID";
  }

  await tx.invoice.update({
    where: { id: invoiceId },
    data: {
      paidAmount: new Prisma.Decimal(paidClamped),
      paymentStatus,

      ...(inv.type === "SALES" && inv.hasWarrantyHold
        ? {
            warrantyHoldPct: new Prisma.Decimal(holdPct),
            warrantyHoldAmount: new Prisma.Decimal(holdAmount),
          }
        : {}),
    },
  });
}

/**
 * Nếu lúc tạo invoice có paidAmount > 0 => tạo Payment + Allocation
 *
 * ✅ FIX (Option A):
 * - KHÔNG tạo payment lúc tạo SALES_RETURN/PURCHASE_RETURN.
 * - Refund phải đi qua /payments và allocate âm vào SALES gốc.
 */
async function createInitialPaymentIfNeeded(
  tx: Prisma.TransactionClient,
  invoiceId: string,
  params: {
    paidAmount: number;
    issueDate: Date;
    partnerId?: string | null;
    receiveAccountId?: string | null;
    createdById?: string | null;
    note?: string;
  }
): Promise<number> {
  const paid = roundMoney(params.paidAmount || 0);
  if (paid <= 0) return 0;

  const inv = await tx.invoice.findUnique({
    where: { id: invoiceId },
    select: {
      type: true,
      code: true,
      subtotal: true,
      tax: true,
      total: true,

      netSubtotal: true,
      netTotal: true,

      hasWarrantyHold: true,
      warrantyHoldPct: true,
      warrantyHoldAmount: true,
    },
  });
  if (!inv) throw httpError(404, "Invoice not found");

  // ✅ block return types
  if (inv.type === "SALES_RETURN" || inv.type === "PURCHASE_RETURN") {
    return 0;
  }

  if (!params.partnerId) {
    throw httpError(400, "Hóa đơn có 'Đã thu' nhưng chưa chọn khách hàng (partner).");
  }

  const paymentType =
    inv.type === "PURCHASE" /* || inv.type === "SALES_RETURN" */ ? "PAYMENT" : "RECEIPT";

  const total = toNum(inv.total);
  const tax = toNum(inv.tax);
  const subtotal =
    toNum(inv.subtotal) > 0 ? toNum(inv.subtotal) : Math.max(0, roundMoney(total - tax));

  let collectible = total;

  if (inv.type === "SALES") {
    const calc = computeCollectibleForSalesWithNet({
      subtotal,
      tax,
      total,
      netSubtotal: toNum((inv as any).netSubtotal) || subtotal,
      netTotal: toNum((inv as any).netTotal) || total,
      hasWarrantyHold: inv.hasWarrantyHold === true,
      warrantyHoldPct: toNum(inv.warrantyHoldPct),
      warrantyHoldAmount: toNum(inv.warrantyHoldAmount),
    });
    collectible = calc.collectible;
  }

  const paidClamped = Math.min(paid, collectible);
  const allocAmount = paymentType === "PAYMENT" ? -paidClamped : paidClamped;

  await tx.payment.create({
    data: {
      date: params.issueDate,
      partnerId: params.partnerId,
      type: paymentType as any,
      amount: new Prisma.Decimal(paidClamped),
      accountId: params.receiveAccountId ?? null,
      note: params.note ?? `Thu/chi lúc tạo HĐ ${inv.code}`,
      createdById: params.createdById ?? null,
      allocations: {
        create: {
          invoiceId,
          amount: new Prisma.Decimal(allocAmount),
          kind: "NORMAL",
        },
      },
    },
  });

  return paidClamped;
}

/**
 * Recompute subtotal / total cho một invoice (DRAFT)
 */
async function recomputeInvoiceTotals(invoiceId: string) {
  const [invoice, lines] = await Promise.all([
    prisma.invoice.findUnique({
      where: { id: invoiceId },
      select: { tax: true, paymentStatus: true, paidAmount: true },
    }),
    prisma.invoiceLine.findMany({ where: { invoiceId } }),
  ]);

  if (!invoice) throw new Error("Invoice not found");

  const subtotal = lines.reduce((s, l) => s + toNum(l.amount), 0);
  const tax = toNum(invoice.tax);
  const total = subtotal + tax;

  let paidAmount = toNum(invoice.paidAmount);
  const st = invoice.paymentStatus;

  if (st === "UNPAID") paidAmount = 0;
  if (st === "PAID") paidAmount = total;
  if (st === "PARTIAL") {
    if (paidAmount < 0) paidAmount = 0;
    if (paidAmount > total) paidAmount = total;
  }

  return prisma.invoice.update({
    where: { id: invoiceId },
    data: {
      subtotal: new Prisma.Decimal(subtotal),
      total: new Prisma.Decimal(total),
      paidAmount: new Prisma.Decimal(paidAmount),
    },
  });
}

/**
 * CHỈ cho sửa khi DRAFT
 */
async function assertInvoiceEditable(tx: Prisma.TransactionClient | PrismaClient, invoiceId: string) {
  const inv = await tx.invoice.findUnique({
    where: { id: invoiceId },
    select: { status: true },
  });
  if (!inv) throw httpError(404, "Invoice not found");
  if (inv.status !== "DRAFT") {
    throw httpError(409, "Chỉ hoá đơn NHÁP (DRAFT) mới được chỉnh sửa.");
  }
}

/** rounding helper cho avgCost (4 decimals) */
function round4(n: number) {
  return Math.round(n * 10000) / 10000;
}

/** avgCost bình quân gia quyền */
function computeNewAvgCost(params: {
  curQty: number;
  curAvg: number;
  inQty: number;
  inUnitCost: number;
}) {
  const { curQty, curAvg, inQty, inUnitCost } = params;
  if (inQty <= 0) return curAvg;
  const totalQty = curQty + inQty;
  if (totalQty <= 0) return curAvg;
  const totalCost = curQty * curAvg + inQty * inUnitCost;
  return round4(totalCost / totalQty);
}

/** Gom qty theo itemId */
function sumQtyByItem(rows: Array<{ itemId: string; qty: number }>) {
  const map = new Map<string, number>();
  for (const r of rows) map.set(r.itemId, (map.get(r.itemId) || 0) + r.qty);
  return map;
}

/** helper: fetch invoice full (dùng tx) */
async function getInvoiceByIdTx(tx: Prisma.TransactionClient | PrismaClient, id: string) {
  return tx.invoice.findUnique({
    where: { id },
    include: {
      partner: true,
      saleUser: true,
      techUser: true,
      approvedBy: true,
      receiveAccount: true,
      lines: { include: { item: true } },
      movements: { include: { lines: true } },

      allocations: {
        include: {
          payment: {
            include: {
              account: true,
              createdBy: true,
              partner: true,
            },
          },
        },
        orderBy: { createdAt: "desc" },
      },

      warrantyHold: true,
    },
  });
}

/** helper: snapshot gọn cho audit */
async function getInvoiceAuditSnapshot(
  tx: Prisma.TransactionClient | PrismaClient,
  invoiceId: string
) {
  const inv = await tx.invoice.findUnique({
    where: { id: invoiceId },
    select: {
      id: true,
      code: true,
      type: true,
      status: true,
      issueDate: true,

      partnerId: true,
      partnerName: true,

      saleUserId: true,
      saleUserName: true,
      techUserId: true,
      techUserName: true,

      subtotal: true,
      tax: true,
      total: true,

      returnedSubtotal: true,
      returnedTax: true,
      returnedTotal: true,
      netSubtotal: true,
      netTax: true,
      netTotal: true,

      paymentStatus: true,
      paidAmount: true,

      hasWarrantyHold: true,
      warrantyHoldPct: true,
      warrantyHoldAmount: true,
      warrantyDueDate: true,

      receiveAccountId: true,
      refInvoiceId: true,

      totalCost: true,

      approvedById: true,
      approvedAt: true,
      note: true,
      createdAt: true,
      updatedAt: true,

      cancelledAt: true,
      cancelledById: true,
      cancelReason: true,
    },
  });

  if (!inv) return null;

  return {
    ...inv,
    issueDate: inv.issueDate ? inv.issueDate.toISOString() : null,
    warrantyDueDate: inv.warrantyDueDate ? inv.warrantyDueDate.toISOString() : null,
    approvedAt: inv.approvedAt ? inv.approvedAt.toISOString() : null,
    createdAt: inv.createdAt ? inv.createdAt.toISOString() : null,
    updatedAt: inv.updatedAt ? inv.updatedAt.toISOString() : null,
    cancelledAt: inv.cancelledAt ? inv.cancelledAt.toISOString() : null,

    subtotal: toNum(inv.subtotal),
    tax: toNum(inv.tax),
    total: toNum(inv.total),

    returnedSubtotal: toNum((inv as any).returnedSubtotal),
    returnedTax: toNum((inv as any).returnedTax),
    returnedTotal: toNum((inv as any).returnedTotal),

    netSubtotal: toNum((inv as any).netSubtotal),
    netTax: toNum((inv as any).netTax),
    netTotal: toNum((inv as any).netTotal),

    paidAmount: toNum(inv.paidAmount),
    warrantyHoldPct: toNum(inv.warrantyHoldPct),
    warrantyHoldAmount: toNum(inv.warrantyHoldAmount),
    totalCost: toNum(inv.totalCost),
  };
}

/**
 * Apply SALES_RETURN vào hóa đơn SALES gốc
 *
 * ✅ FIX theo rule bạn chốt:
 * - Trả hàng FULL phải trả cả VAT.
 * - Không tin tuyệt đối ret.tax (UI/BE có thể lưu 0), nếu (ret.total != ret.subtotal + ret.tax)
 *   => lấy VAT của phiếu trả = ret.total - ret.subtotal.
 * - Nếu phiếu trả bị lưu thiếu VAT (tax=0, total=subtotal) => tự tính VAT theo VAT của hóa đơn gốc.
 * - Không auto set CANCELLED nữa. "Tag đã trả" sẽ thể hiện bằng returnedTotal/netTotal ở FE.
 */
async function applySalesReturnToOrigin(
  tx: Prisma.TransactionClient,
  params: {
    returnInvoiceId: string;
    originInvoiceId: string;
    actorId: string;
    auditCtx?: AuditCtx;
  }
) {
  const ret = await tx.invoice.findUnique({
    where: { id: params.returnInvoiceId },
    select: {
      id: true,
      code: true,
      type: true,
      status: true,
      subtotal: true,
      tax: true,
      total: true,
      issueDate: true,
      approvedAt: true,
      refInvoiceId: true,
    },
  });
  if (!ret) throw httpError(404, "Không tìm thấy phiếu trả hàng");
  if (ret.type !== "SALES_RETURN") throw httpError(400, "Không phải phiếu SALES_RETURN");

  const origin = await tx.invoice.findUnique({
    where: { id: params.originInvoiceId },
    select: {
      id: true,
      code: true,
      type: true,
      status: true,

      subtotal: true,
      tax: true,
      total: true,

      returnedSubtotal: true,
      returnedTax: true,
      returnedTotal: true,

      netSubtotal: true,
      netTax: true,
      netTotal: true,

      hasWarrantyHold: true,
      warrantyHoldPct: true,
      warrantyHoldAmount: true,
      warrantyDueDate: true,

      paidAmount: true,

      cancelledAt: true,
    },
  });
  if (!origin) throw httpError(404, "Không tìm thấy hóa đơn gốc");
  if (origin.type !== "SALES") throw httpError(400, "Hóa đơn gốc không phải SALES");

  const originBefore = await getInvoiceAuditSnapshot(tx, origin.id);

  const oSubtotal = roundMoney(toNum(origin.subtotal));
  const oTax = roundMoney(toNum(origin.tax));
  const oTotal = roundMoney(toNum(origin.total));

  // --- ✅ normalize return amounts ---
  let rSubtotal = roundMoney(toNum(ret.subtotal));
  let rTax = roundMoney(toNum(ret.tax));
  let rTotal = roundMoney(toNum(ret.total));

  // fallback: nếu total bị 0 nhưng subtotal/tax có => tính lại
  if (rTotal <= 0 && (rSubtotal > 0 || rTax > 0)) {
    rTotal = roundMoney(rSubtotal + rTax);
  }

  // ✅ CORE FIX #1: nếu mismatch thì tin total+subtotal => derive tax = total - subtotal
  if (rTotal > 0) {
    const diff = roundMoney(rTotal - (rSubtotal + rTax));
    if (Math.abs(diff) > 0.01) {
      const derivedTax = roundMoney(rTotal - rSubtotal);
      rTax = Math.max(0, derivedTax);
    }
  }

  // ✅ CORE FIX #2: nếu phiếu trả bị lưu thiếu VAT (tax ~ 0, total ~ subtotal) nhưng hóa đơn gốc có VAT
  if (rSubtotal > 0.0001 && rTax <= 0.0001 && oTax > 0.0001 && oSubtotal > 0.0001) {
    rTax = computeReturnTaxFromOrigin({
      originSubtotal: oSubtotal,
      originTax: oTax,
      returnSubtotal: rSubtotal,
    });
    rTotal = roundMoney(rSubtotal + rTax);
  }

  // ensure non-negative
  rSubtotal = Math.max(0, rSubtotal);
  rTax = Math.max(0, rTax);
  rTotal = Math.max(0, roundMoney(rSubtotal + rTax));

  // Basic caps: không cho trả vượt tổng gốc
  const oldReturnedSubtotal = roundMoney(toNum((origin as any).returnedSubtotal));
  const oldReturnedTax = roundMoney(toNum((origin as any).returnedTax));
  const oldReturnedTotal = roundMoney(toNum((origin as any).returnedTotal));

  const nextReturnedSubtotal = Math.min(oSubtotal, roundMoney(oldReturnedSubtotal + rSubtotal));
  const nextReturnedTax = Math.min(oTax, roundMoney(oldReturnedTax + rTax));
  const nextReturnedTotal = Math.min(oTotal, roundMoney(oldReturnedTotal + rTotal));

  const nextNetSubtotal = Math.max(0, roundMoney(oSubtotal - nextReturnedSubtotal));
  const nextNetTax = Math.max(0, roundMoney(oTax - nextReturnedTax));
  const nextNetTotal = Math.max(0, roundMoney(oTotal - nextReturnedTotal));

  // recompute hold theo netSubtotal
  let nextHoldPct = roundPct(toNum(origin.warrantyHoldPct));
  let nextHoldAmount = roundMoney(toNum(origin.warrantyHoldAmount));

  if (origin.hasWarrantyHold === true) {
    if (!(nextHoldPct > 0)) nextHoldPct = 5;
    nextHoldAmount = roundMoney((nextNetSubtotal * nextHoldPct) / 100);
    if (nextHoldAmount > nextNetSubtotal) nextHoldAmount = nextNetSubtotal;
  } else {
    nextHoldPct = 0;
    nextHoldAmount = 0;
  }

  await tx.invoice.update({
    where: { id: origin.id },
    data: {
      returnedSubtotal: new Prisma.Decimal(nextReturnedSubtotal),
      returnedTax: new Prisma.Decimal(nextReturnedTax),
      returnedTotal: new Prisma.Decimal(nextReturnedTotal),

      netSubtotal: new Prisma.Decimal(nextNetSubtotal),
      netTax: new Prisma.Decimal(nextNetTax),
      netTotal: new Prisma.Decimal(nextNetTotal),

      ...(origin.hasWarrantyHold
        ? {
            warrantyHoldPct: new Prisma.Decimal(nextHoldPct),
            warrantyHoldAmount: new Prisma.Decimal(nextHoldAmount),
          }
        : {
            warrantyHoldPct: new Prisma.Decimal(0),
            warrantyHoldAmount: new Prisma.Decimal(0),
          }),
    },
  });

  // ✅ sync paid/status theo allocations NORMAL (chỉ trên SALES gốc)
  await syncInvoicePaidFromAllocations(tx, origin.id);

  const originAfter = await getInvoiceAuditSnapshot(tx, origin.id);

  await auditLog(tx, {
    userId: params.auditCtx?.userId ?? params.actorId,
    userRole: params.auditCtx?.userRole,
    action: "INVOICE_ORIGIN_APPLY_RETURN",
    entity: "Invoice",
    entityId: origin.id,
    before: originBefore,
    after: originAfter,
    meta: mergeMeta(params.auditCtx?.meta, {
      originInvoiceId: origin.id,
      originCode: origin.code,
      returnInvoiceId: ret.id,
      returnCode: ret.code,
      delta: { returnedSubtotal: rSubtotal, returnedTax: rTax, returnedTotal: rTotal },
      next: {
        returnedSubtotal: nextReturnedSubtotal,
        returnedTax: nextReturnedTax,
        returnedTotal: nextReturnedTotal,
        netSubtotal: nextNetSubtotal,
        netTax: nextNetTax,
        netTotal: nextNetTotal,
      },
      normalize: {
        retSubtotal: toNum(ret.subtotal),
        retTax: toNum(ret.tax),
        retTotal: toNum(ret.total),
        used: { rSubtotal, rTax, rTotal },
        originRate: oSubtotal > 0 ? oTax / oSubtotal : null,
      },
    }),
  });
}

/** ========================= Public APIs ========================= **/

export async function listInvoices(
  q: string | undefined,
  page: number,
  pageSize: number,
  filter: {
    type?: InvoiceType;
    excludeTypes?: InvoiceType[];
    saleUserId?: string;
    techUserId?: string;
    from?: Date;
    to?: Date;
    paymentStatus?: PaymentStatus;
    status?: InvoiceStatus;
    receiveAccountId?: string;
  }
) {
  const where: Prisma.InvoiceWhereInput = {};

  if (q) {
    Object.assign(where, {
      OR: [
        { code: { contains: q, mode: "insensitive" } },
        { partnerName: { contains: q, mode: "insensitive" } },
        { note: { contains: q, mode: "insensitive" } },
      ],
    });
  }

  if (filter?.type) where.type = filter.type;

  if (filter?.excludeTypes && filter.excludeTypes.length > 0) {
    where.NOT = { type: { in: filter.excludeTypes } };
  }

  if (filter?.saleUserId) where.saleUserId = filter.saleUserId as any;
  if (filter?.techUserId) where.techUserId = filter.techUserId as any;
  if (filter?.status) where.status = filter.status;
  if (filter?.receiveAccountId) where.receiveAccountId = filter.receiveAccountId;

  if (filter?.from || filter?.to) {
    where.issueDate = {};
    if (filter.from) (where.issueDate as any).gte = filter.from;
    if (filter.to) (where.issueDate as any).lte = filter.to;
  }
  if (filter?.paymentStatus) where.paymentStatus = filter.paymentStatus;

  const [total, rows] = await Promise.all([
    prisma.invoice.count({ where }),
    prisma.invoice.findMany({
      where,
      orderBy: { issueDate: "desc" },
      include: {
        partner: true,
        saleUser: true,
        techUser: true,
        approvedBy: true,
        receiveAccount: true,
        lines: { include: { item: true } },
        movements: true,
        warrantyHold: true,
      },
      skip: (page - 1) * pageSize,
      take: pageSize,
    }),
  ]);

  return { data: rows, total, page, pageSize };
}

export async function getInvoiceById(id: string) {
  return getInvoiceByIdTx(prisma, id);
}

export async function updateInvoiceNote(id: string, note: string, auditCtx?: AuditCtx) {
  await assertInvoiceEditable(prisma, id);

  const before = await getInvoiceAuditSnapshot(prisma, id);

  const updated = await prisma.invoice.update({
    where: { id },
    data: { note },
    select: { id: true, note: true },
  });

  const after = await getInvoiceAuditSnapshot(prisma, id);

  await auditLog(prisma, {
    userId: auditCtx?.userId,
    userRole: auditCtx?.userRole,
    action: "INVOICE_NOTE_UPDATE",
    entity: "Invoice",
    entityId: id,
    before,
    after,
    meta: mergeMeta(auditCtx?.meta, { noteLength: String(note || "").length }),
  });

  return updated;
}

/**
 * Create invoice
 *
 * ✅ FIX (Option A):
 * - SALES_RETURN/PURCHASE_RETURN: ignore paidAmount/paymentStatus on invoice create
 *   (refund không gắn vào return invoice)
 *
 * ✅ FIX VAT RETURN:
 * - SALES_RETURN: tax/total sẽ được suy ra từ VAT của hóa đơn gốc (tỷ lệ originTax/originSubtotal)
 *   để tránh trường hợp phiếu trả bị lưu thiếu VAT => origin còn thiếu VAT như bug bạn gặp.
 */
export async function createInvoice(body: any, auditCtx?: AuditCtx) {
  const issueDate = body.issueDate ? new Date(body.issueDate) : new Date();

  const safeCode =
    body.code && String(body.code).trim().length > 0 ? String(body.code).trim() : `INV-${Date.now()}`;

  const rawLines: any[] = Array.isArray(body.lines) ? body.lines : [];

  const validLines = rawLines
    .map((l) => ({ ...l, itemId: l.itemId, qty: Number(l.qty || 0), price: Number(l.price || 0) }))
    .filter((l) => !!l.itemId && l.qty > 0);

  if (!validLines.length) {
    throw httpError(400, "Hoá đơn phải có ít nhất 1 sản phẩm (hãy chọn sản phẩm từ danh sách).");
  }

  const type: InvoiceType = (body.type ?? "SALES") as InvoiceType;

  const subtotal = validLines.reduce((s, l) => s + l.qty * l.price, 0);
  const taxFromBody = calcTaxFromBody(subtotal, body);
  const totalFromBody = subtotal + taxFromBody;

  // ✅ Only SALES/PURCHASE allow legacy paidAmount-on-create
  const isReturnType = type === "SALES_RETURN" || type === "PURCHASE_RETURN";
  const normalized = !isReturnType
    ? normalizePayment(subtotal, taxFromBody, body)
    : { total: totalFromBody, paidAmount: 0, paymentStatus: "UNPAID" as PaymentStatus };
  const paidAmount = normalized.paidAmount;

  const hasWarrantyHold = body?.hasWarrantyHold === true;

  const inputHoldAmount = parseOptionalNumber(body?.warrantyHoldAmount);
  const inputHoldPct = parseOptionalNumber(body?.warrantyHoldPct);

  try {
    const created = await prisma.$transaction(
      async (tx) => {
        const receiveAccountId = await validateReceiveAccountId(tx, body.receiveAccountId);

        let origin: Awaited<ReturnType<typeof requireValidRefInvoiceForSalesReturn>> | null = null;

        // ✅ totals effective (đặc biệt cho SALES_RETURN cần suy VAT theo origin)
        let effectiveTax = taxFromBody;
        let effectiveTotal = totalFromBody;

        if (type === "SALES_RETURN") {
          origin = await requireValidRefInvoiceForSalesReturn(tx, body.refInvoiceId);

          // fill partner
          if (!body.partnerId) body.partnerId = origin.partnerId ?? null;
          if (!body.partnerName) body.partnerName = origin.partnerName ?? null;

          if (body.partnerPhone == null) body.partnerPhone = origin.partnerPhone ?? null;
          if (body.partnerTax == null) body.partnerTax = origin.partnerTax ?? null;
          if (body.partnerAddr == null) body.partnerAddr = origin.partnerAddr ?? null;

          // ✅ VAT return = theo tỷ lệ VAT hóa đơn gốc
          effectiveTax = computeReturnTaxFromOrigin({
            originSubtotal: toNum(origin.subtotal),
            originTax: toNum(origin.tax),
            returnSubtotal: subtotal,
          });
          effectiveTotal = roundMoney(subtotal + effectiveTax);
        }

        if (type === "SALES_RETURN" && !body.partnerId) {
          throw httpError(
            400,
            "Phiếu KHÁCH TRẢ HÀNG cần có khách hàng (partnerId). Hãy chọn hóa đơn gốc hoặc chọn khách hàng."
          );
        }

        const holdCalc = computeWarrantyHoldAndCollectible({
          subtotal,
          total: effectiveTotal,
          hasWarrantyHold: type === "SALES" && hasWarrantyHold,
          warrantyHoldPct: inputHoldPct,
          warrantyHoldAmount: inputHoldAmount,
          legacyPct: 5,
        });

        const due =
          type === "SALES" && hasWarrantyHold
            ? (() => {
                const d = new Date(issueDate);
                d.setFullYear(d.getFullYear() + 1);
                return d;
              })()
            : null;

        const inv = await tx.invoice.create({
          data: {
            code: safeCode,
            type,
            issueDate,

            partnerId: body.partnerId ?? null,

            saleUserId: body.saleUserId ?? null,
            techUserId: body.techUserId ?? null,

            refInvoiceId:
              type === "SALES_RETURN"
                ? String(body.refInvoiceId).trim()
                : body.refInvoiceId ?? null,

            receiveAccountId,
            note: body.note ?? "",

            partnerName: body.partnerName ?? null,
            partnerPhone: body.partnerPhone ?? null,
            partnerTax: body.partnerTax ?? null,
            partnerAddr: body.partnerAddr ?? null,

            currency: body.currency ?? "VND",

            subtotal: new Prisma.Decimal(subtotal),
            tax: new Prisma.Decimal(effectiveTax),
            total: new Prisma.Decimal(effectiveTotal),

            returnedSubtotal: new Prisma.Decimal(0),
            returnedTax: new Prisma.Decimal(0),
            returnedTotal: new Prisma.Decimal(0),

            netSubtotal: new Prisma.Decimal(subtotal),
            netTax: new Prisma.Decimal(effectiveTax),
            netTotal: new Prisma.Decimal(effectiveTotal),

            // ✅ return types always start unpaid/0 (refund goes via /payments to origin)
            paymentStatus: "UNPAID",
            paidAmount: new Prisma.Decimal(0),

            hasWarrantyHold: type === "SALES" ? hasWarrantyHold : false,
            warrantyHoldPct: new Prisma.Decimal(type === "SALES" ? holdCalc.pct : 0),
            warrantyHoldAmount: new Prisma.Decimal(type === "SALES" ? holdCalc.holdAmount : 0),
            warrantyDueDate: type === "SALES" ? due : null,

            status: "DRAFT",

            cancelledAt: null,
            cancelledById: null,
            cancelReason: null,
          } as any,
        });

        await tx.invoiceLine.createMany({
          data: validLines.map((l) => {
            const amount = l.qty * l.price;
            return {
              invoiceId: inv.id,
              itemId: l.itemId,
              qty: new Prisma.Decimal(l.qty),
              price: new Prisma.Decimal(l.price),
              amount: new Prisma.Decimal(amount),
              itemName: l.itemName || undefined,
              itemSku: l.itemSku || undefined,
            };
          }),
        });

        // ✅ Only non-return types can create initial payment
        let paidClamped = 0;
        if (!isReturnType && paidAmount > 0) {
          paidClamped = await createInitialPaymentIfNeeded(tx, inv.id, {
            paidAmount,
            issueDate,
            partnerId: inv.partnerId,
            receiveAccountId: inv.receiveAccountId,
            createdById: body.createdById ?? auditCtx?.userId ?? null,
            note: body.initialPaymentNote,
          });
        }

        if (paidClamped > 0) {
          const collectible = holdCalc.collectible;
          const st: PaymentStatus = paidClamped + 0.0001 >= collectible ? "PAID" : "PARTIAL";

          await tx.invoice.update({
            where: { id: inv.id },
            data: {
              paidAmount: new Prisma.Decimal(Math.min(paidClamped, collectible)),
              paymentStatus: st,
            },
          });
        }

        const after = await getInvoiceAuditSnapshot(tx, inv.id);
        await auditLog(tx, {
          userId: auditCtx?.userId ?? body.createdById,
          userRole: auditCtx?.userRole,
          action: "INVOICE_CREATE",
          entity: "Invoice",
          entityId: inv.id,
          before: null,
          after,
          meta: mergeMeta(auditCtx?.meta, {
            safeCode,
            originInvoiceId: origin?.id ?? null,
            lineCount: validLines.length,
            isReturnType,
            note: isReturnType
              ? "Return invoice does not create payments; refund must go via /payments to origin."
              : undefined,
          }),
        });

        return inv;
      },
      { timeout: 20000, maxWait: 5000 }
    );

    return getInvoiceById(created.id);
  } catch (e) {
    handleUniqueInvoiceError(e);
  }
}

/**
 * Update invoice + replace lines
 *
 * ✅ FIX: Nếu invoice không phải SALES => ép warrantyHold fields = 0 (tránh return bị set hold)
 * ✅ FIX VAT RETURN: nếu SALES_RETURN, tax/total được suy theo VAT hóa đơn gốc
 */
export async function updateInvoice(id: string, body: any, auditCtx?: AuditCtx) {
  const before = await getInvoiceAuditSnapshot(prisma, id);

  try {
    await prisma.$transaction(
      async (tx) => {
        await assertInvoiceEditable(tx, id);

        const current = await tx.invoice.findUnique({
          where: { id },
          select: {
            id: true,
            type: true,
            refInvoiceId: true,
            issueDate: true,
            subtotal: true,
            tax: true,
            total: true,
            hasWarrantyHold: true,
            warrantyHoldPct: true,
            warrantyHoldAmount: true,
          },
        });
        if (!current) throw httpError(404, "Invoice not found");

        const nextType: InvoiceType = (body.type ?? current.type) as InvoiceType;

        let originForReturn: Awaited<ReturnType<typeof requireValidRefInvoiceForSalesReturn>> | null =
          null;

        if (nextType === "SALES_RETURN") {
          const refId = body.refInvoiceId !== undefined ? body.refInvoiceId : current.refInvoiceId;
          originForReturn = await requireValidRefInvoiceForSalesReturn(tx, refId);

          if (body.partnerId == null) body.partnerId = originForReturn.partnerId ?? null;
          if (body.partnerName == null) body.partnerName = originForReturn.partnerName ?? null;
          if (body.partnerPhone == null) body.partnerPhone = originForReturn.partnerPhone ?? null;
          if (body.partnerTax == null) body.partnerTax = originForReturn.partnerTax ?? null;
          if (body.partnerAddr == null) body.partnerAddr = originForReturn.partnerAddr ?? null;

          if (!body.partnerId) {
            throw httpError(
              400,
              "Phiếu KHÁCH TRẢ HÀNG cần có khách hàng (partnerId). Hãy chọn hóa đơn gốc hoặc chọn khách hàng."
            );
          }
        }

        const data: any = {};

        if (body.code !== undefined) {
          const trimmed = String(body.code || "").trim();
          if (trimmed.length > 0) data.code = trimmed;
        }
        if (body.issueDate) data.issueDate = new Date(body.issueDate);
        if (body.type) data.type = body.type as InvoiceType;
        if (body.note !== undefined) data.note = body.note;

        if (body.partnerId !== undefined) data.partnerId = body.partnerId || null;

        if (body.partnerName !== undefined) data.partnerName = body.partnerName;
        if (body.partnerPhone !== undefined) data.partnerPhone = body.partnerPhone;
        if (body.partnerTax !== undefined) data.partnerTax = body.partnerTax;
        if (body.partnerAddr !== undefined) data.partnerAddr = body.partnerAddr;

        if (body.saleUserId !== undefined) data.saleUserId = body.saleUserId || null;
        if (body.techUserId !== undefined) data.techUserId = body.techUserId || null;

        if (body.receiveAccountId !== undefined) {
          data.receiveAccountId = await validateReceiveAccountId(tx, body.receiveAccountId);
        }

        if (body.refInvoiceId !== undefined) {
          data.refInvoiceId = body.refInvoiceId ? String(body.refInvoiceId).trim() : null;
        }

        // ✅ warrantyHold chỉ hợp lệ cho SALES
        if (nextType !== "SALES") {
          data.hasWarrantyHold = false;
          data.warrantyHoldPct = new Prisma.Decimal(0);
          data.warrantyHoldAmount = new Prisma.Decimal(0);
          data.warrantyDueDate = null;
        } else {
          if (body.hasWarrantyHold !== undefined) {
            data.hasWarrantyHold = body.hasWarrantyHold === true;
            if (data.hasWarrantyHold !== true) {
              data.warrantyHoldPct = new Prisma.Decimal(0);
              data.warrantyHoldAmount = new Prisma.Decimal(0);
              data.warrantyDueDate = null;
            }
          }

          if (body.warrantyHoldPct !== undefined) {
            const pct = Number(body.warrantyHoldPct);
            if (!Number.isFinite(pct) || pct < 0 || pct > 100) {
              throw httpError(400, "warrantyHoldPct không hợp lệ (0..100).");
            }
            data.warrantyHoldPct = new Prisma.Decimal(pct);
          }

          if (body.warrantyHoldAmount !== undefined) {
            const amt = parseOptionalNumber(body.warrantyHoldAmount);
            if (amt === undefined) {
              data.warrantyHoldAmount = new Prisma.Decimal(0);
            } else {
              if (amt < 0) throw httpError(400, "warrantyHoldAmount không hợp lệ (>= 0).");
              data.warrantyHoldAmount = new Prisma.Decimal(roundMoney(amt));
            }
          }
        }

        await tx.invoice.update({ where: { id }, data });

        let changedTotals = false;

        if (Array.isArray(body.lines)) {
          await tx.invoiceLine.deleteMany({ where: { invoiceId: id } });

          const validLines = body.lines
            .map((l: any) => ({
              ...l,
              itemId: l.itemId,
              qty: Number(l.qty || 0),
              price: Number(l.price || l.unitPrice || 0),
            }))
            .filter((l: any) => !!l.itemId && l.qty > 0);

          if (!validLines.length) {
            throw httpError(400, "Hoá đơn phải có ít nhất 1 sản phẩm (đã chọn từ danh sách).");
          }

          let subtotal = 0;
          const linesData = validLines.map((l: any) => {
            const amount = l.qty * l.price;
            subtotal += amount;
            return {
              invoiceId: id,
              itemId: l.itemId,
              qty: new Prisma.Decimal(l.qty),
              price: new Prisma.Decimal(l.price),
              amount: new Prisma.Decimal(amount),
              itemName: l.itemName || undefined,
              itemSku: l.itemSku || undefined,
            };
          });

          await tx.invoiceLine.createMany({ data: linesData });

          // ✅ VAT return
          let tax = calcTaxFromBody(subtotal, body);
          if (nextType === "SALES_RETURN" && originForReturn) {
            tax = computeReturnTaxFromOrigin({
              originSubtotal: toNum(originForReturn.subtotal),
              originTax: toNum(originForReturn.tax),
              returnSubtotal: subtotal,
            });
          }

          const total = roundMoney(subtotal + tax);

          await tx.invoice.update({
            where: { id },
            data: {
              subtotal: new Prisma.Decimal(subtotal),
              tax: new Prisma.Decimal(tax),
              total: new Prisma.Decimal(total),

              // giữ net* đồng bộ trong DRAFT
              netSubtotal: new Prisma.Decimal(subtotal),
              netTax: new Prisma.Decimal(tax),
              netTotal: new Prisma.Decimal(total),
            } as any,
          });

          changedTotals = true;
        }

        const fresh = await tx.invoice.findUnique({
          where: { id },
          select: {
            type: true,
            subtotal: true,
            tax: true,
            total: true,
            issueDate: true,

            netSubtotal: true,
            netTotal: true,

            hasWarrantyHold: true,
            warrantyHoldPct: true,
            warrantyHoldAmount: true,
          },
        });

        if (fresh && fresh.type === "SALES") {
          const total = toNum(fresh.total);
          const tax = toNum(fresh.tax);
          const subtotal =
            toNum(fresh.subtotal) > 0
              ? toNum(fresh.subtotal)
              : Math.max(0, roundMoney(total - tax));

          const calc = computeWarrantyHoldAndCollectible({
            subtotal,
            total,
            hasWarrantyHold: fresh.hasWarrantyHold === true,
            warrantyHoldPct: toNum(fresh.warrantyHoldPct),
            warrantyHoldAmount:
              toNum(fresh.warrantyHoldAmount) > 0 ? toNum(fresh.warrantyHoldAmount) : undefined,
            legacyPct: 5,
          });

          const due =
            fresh.hasWarrantyHold === true
              ? (() => {
                  const d = new Date(fresh.issueDate);
                  d.setFullYear(d.getFullYear() + 1);
                  return d;
                })()
              : null;

          await tx.invoice.update({
            where: { id },
            data: {
              warrantyHoldPct: new Prisma.Decimal(calc.pct),
              warrantyHoldAmount: new Prisma.Decimal(calc.holdAmount),
              warrantyDueDate: due,
            },
          });
        }

        if (
          Array.isArray(body.lines) ||
          body.hasWarrantyHold !== undefined ||
          body.warrantyHoldPct !== undefined ||
          body.warrantyHoldAmount !== undefined ||
          changedTotals
        ) {
          await syncInvoicePaidFromAllocations(tx, id);
        }

        const after = await getInvoiceAuditSnapshot(tx, id);
        await auditLog(tx, {
          userId: auditCtx?.userId ?? body.updatedById ?? body.createdById,
          userRole: auditCtx?.userRole,
          action: "INVOICE_UPDATE",
          entity: "Invoice",
          entityId: id,
          before,
          after,
          meta: mergeMeta(auditCtx?.meta, {
            hasLinesUpdate: Array.isArray(body.lines),
          }),
        });
      },
      { timeout: 20000, maxWait: 5000 }
    );
  } catch (e: any) {
    if (e && e.statusCode) throw e;
    handleUniqueInvoiceError(e);
  }

  return getInvoiceById(id);
}

export async function deleteInvoice(id: string, auditCtx?: AuditCtx) {
  const before = await getInvoiceAuditSnapshot(prisma, id);

  const inv = await prisma.invoice.findUnique({
    where: { id },
    select: { status: true },
  });
  if (!inv) throw httpError(404, "Invoice not found");
  if (inv.status === "APPROVED") {
    throw httpError(409, "Hóa đơn đã duyệt, không được xóa. Hãy dùng chứng từ điều chỉnh/hoàn trả.");
  }

  const hasMv = await prisma.movement.count({ where: { invoiceId: id } });
  if (hasMv > 0) {
    throw httpError(409, "Không thể xoá hoá đơn đã post tồn (đã có movement liên kết).");
  }

  await prisma.warrantyHold.deleteMany({ where: { invoiceId: id } });
  await prisma.invoiceLine.deleteMany({ where: { invoiceId: id } });
  const deleted = await prisma.invoice.delete({ where: { id } });

  await auditLog(prisma, {
    userId: auditCtx?.userId,
    userRole: auditCtx?.userRole,
    action: "INVOICE_DELETE",
    entity: "Invoice",
    entityId: id,
    before,
    after: null,
    meta: mergeMeta(auditCtx?.meta, {}),
  });

  return deleted;
}

export async function addInvoiceLine(invoiceId: string, body: any) {
  await assertInvoiceEditable(prisma, invoiceId);

  const qty = Number(body.qty || 0);
  const price = Number(body.price || 0);
  const amount = qty * price;

  const line = await prisma.invoiceLine.create({
    data: {
      invoiceId,
      itemId: body.itemId,
      qty: new Prisma.Decimal(qty),
      price: new Prisma.Decimal(price),
      amount: new Prisma.Decimal(amount),
      itemName: body.itemName || undefined,
      itemSku: body.itemSku || undefined,
    },
  });

  await recomputeInvoiceTotals(invoiceId);
  return line;
}

export async function updateInvoiceLine(lineId: string, body: any) {
  const row = await prisma.invoiceLine.findUnique({
    where: { id: lineId },
    select: {
      id: true,
      invoiceId: true,
      qty: true,
      price: true,
      itemId: true,
      itemName: true,
      itemSku: true,
    },
  });
  if (!row) throw httpError(404, "Invoice line not found");

  await assertInvoiceEditable(prisma, row.invoiceId);

  const qty = body.qty != null ? Number(body.qty) : toNum(row.qty);
  const price = body.price != null ? Number(body.price) : toNum(row.price);
  const amount = qty * price;

  const line = await prisma.invoiceLine.update({
    where: { id: lineId },
    data: {
      qty: new Prisma.Decimal(qty),
      price: new Prisma.Decimal(price),
      amount: new Prisma.Decimal(amount),
      itemId: body.itemId || row.itemId,
      itemName: body.itemName || row.itemName || undefined,
      itemSku: body.itemSku || row.itemSku || undefined,
    },
  });

  await recomputeInvoiceTotals(row.invoiceId);
  return line;
}

export async function deleteInvoiceLine(lineId: string) {
  const row = await prisma.invoiceLine.findUnique({
    where: { id: lineId },
    select: { id: true, invoiceId: true },
  });
  if (!row) throw httpError(404, "Invoice line not found");

  await assertInvoiceEditable(prisma, row.invoiceId);

  await prisma.invoiceLine.delete({ where: { id: lineId } });
  await recomputeInvoiceTotals(row.invoiceId);
  return true;
}

export async function linkMovement(invoiceId: string, movementId: string) {
  await assertInvoiceEditable(prisma, invoiceId);
  return prisma.movement.update({
    where: { id: movementId },
    data: { invoiceId },
  });
}

/** ========================= Submit / Approve / Reject ========================= **/

export async function submitInvoice(
  params: { invoiceId: string; submittedById: string },
  auditCtx?: AuditCtx
) {
  await prisma.$transaction(
    async (tx) => {
      const before = await getInvoiceAuditSnapshot(tx, params.invoiceId);

      const inv = await tx.invoice.findUnique({
        where: { id: params.invoiceId },
        select: { id: true, status: true },
      });
      if (!inv) throw httpError(404, "Invoice not found");
      if (inv.status === "APPROVED") throw httpError(409, "Hóa đơn đã duyệt rồi.");
      if (inv.status === "REJECTED") throw httpError(409, "Hóa đơn đã bị từ chối.");
      if (inv.status === "SUBMITTED") throw httpError(409, "Hóa đơn đã gửi duyệt rồi.");

      await tx.invoice.update({
        where: { id: inv.id },
        data: { status: "SUBMITTED" },
      });

      const after = await getInvoiceAuditSnapshot(tx, params.invoiceId);

      await auditLog(tx, {
        userId: auditCtx?.userId ?? params.submittedById,
        userRole: auditCtx?.userRole,
        action: "INVOICE_SUBMIT",
        entity: "Invoice",
        entityId: params.invoiceId,
        before,
        after,
        meta: mergeMeta(auditCtx?.meta, {}),
      });
    },
    { timeout: 20000, maxWait: 5000 }
  );

  return getInvoiceById(params.invoiceId);
}

export async function approveInvoice(
  params: { invoiceId: string; approvedById: string; warehouseId?: string },
  auditCtx?: AuditCtx
) {
  const warehouse = await ensureWarehouse(params.warehouseId);

  const isOutType = (t: InvoiceType) => t === "SALES" || t === "PURCHASE_RETURN";
  const isInType = (t: InvoiceType) => t === "PURCHASE" || t === "SALES_RETURN";

  return prisma.$transaction(
    async (tx) => {
      const before = await getInvoiceAuditSnapshot(tx, params.invoiceId);

      const invoice = await tx.invoice.findUnique({
        where: { id: params.invoiceId },
        include: { lines: true, warrantyHold: true },
      });
      if (!invoice) throw httpError(404, "Invoice not found");

      if (invoice.status === "APPROVED") throw httpError(409, "Hóa đơn đã duyệt rồi.");
      if (invoice.status === "REJECTED") throw httpError(409, "Hóa đơn đã bị từ chối.");
      if (invoice.status !== "SUBMITTED") {
        throw httpError(409, "Chỉ (SUBMITTED) mới được duyệt.");
      }

      if (!invoice.lines.length) throw httpError(400, "Hóa đơn phải có ít nhất 1 dòng hàng.");

      const existingMv = await tx.movement.count({ where: { invoiceId: invoice.id } });
      if (existingMv > 0) throw httpError(409, "Hóa đơn đã có movement, không thể duyệt lại.");

      let originForReturn: Awaited<ReturnType<typeof requireValidRefInvoiceForSalesReturn>> | null =
        null;

      if (invoice.type === "SALES_RETURN") {
        originForReturn = await requireValidRefInvoiceForSalesReturn(
          tx,
          (invoice as any).refInvoiceId
        );

        // ✅ FIX VAT RETURN: ép tax/total của phiếu trả theo VAT hóa đơn gốc
        const retSubtotal = roundMoney(toNum((invoice as any).subtotal));
        const retTax = computeReturnTaxFromOrigin({
          originSubtotal: toNum(originForReturn.subtotal),
          originTax: toNum(originForReturn.tax),
          returnSubtotal: retSubtotal,
        });
        const retTotal = roundMoney(retSubtotal + retTax);

        await tx.invoice.update({
          where: { id: invoice.id },
          data: {
            refInvoiceId: originForReturn.id,

            partnerId: originForReturn.partnerId ?? null,
            partnerName: originForReturn.partnerName ?? null,
            partnerPhone: originForReturn.partnerPhone ?? null,
            partnerTax: originForReturn.partnerTax ?? null,
            partnerAddr: originForReturn.partnerAddr ?? null,

            saleUserId: originForReturn.saleUserId ?? null,
            saleUserName: originForReturn.saleUserName ?? null,
            techUserId: originForReturn.techUserId ?? null,
            techUserName: originForReturn.techUserName ?? null,

            receiveAccountId: invoice.receiveAccountId ?? originForReturn.receiveAccountId ?? null,

            // ✅ return invoice never has hold
            hasWarrantyHold: false,
            warrantyHoldPct: new Prisma.Decimal(0),
            warrantyHoldAmount: new Prisma.Decimal(0),
            warrantyDueDate: null,

            // ✅ totals corrected
            tax: new Prisma.Decimal(retTax),
            total: new Prisma.Decimal(retTotal),
            netSubtotal: new Prisma.Decimal(retSubtotal),
            netTax: new Prisma.Decimal(retTax),
            netTotal: new Prisma.Decimal(retTotal),
          } as any,
        });
      }

      /**
       * SALES: tính lại warrantyHold fields ngay trước khi approve
       */
      if (invoice.type === "SALES") {
        const hasHold = invoice.hasWarrantyHold === true;

        const total = toNum((invoice as any).total);
        const tax = toNum((invoice as any).tax);
        const subtotal =
          toNum((invoice as any).subtotal) > 0
            ? toNum((invoice as any).subtotal)
            : Math.max(0, total - tax);

        await tx.invoice.update({
          where: { id: invoice.id },
          data: {
            returnedSubtotal: new Prisma.Decimal(0),
            returnedTax: new Prisma.Decimal(0),
            returnedTotal: new Prisma.Decimal(0),
            netSubtotal: new Prisma.Decimal(subtotal),
            netTax: new Prisma.Decimal(tax),
            netTotal: new Prisma.Decimal(total),
          } as any,
        });

        if (hasHold) {
          const calc = computeWarrantyHoldAndCollectible({
            subtotal,
            total,
            hasWarrantyHold: true,
            warrantyHoldPct: toNum((invoice as any).warrantyHoldPct),
            warrantyHoldAmount:
              toNum((invoice as any).warrantyHoldAmount) > 0
                ? toNum((invoice as any).warrantyHoldAmount)
                : undefined,
            legacyPct: 5,
          });

          const due = new Date((invoice as any).issueDate);
          due.setFullYear(due.getFullYear() + 1);

          await tx.invoice.update({
            where: { id: invoice.id },
            data: {
              hasWarrantyHold: true,
              warrantyHoldPct: new Prisma.Decimal(calc.pct),
              warrantyHoldAmount: new Prisma.Decimal(calc.holdAmount),
              warrantyDueDate: due,
            },
          });
        } else {
          await tx.invoice.update({
            where: { id: invoice.id },
            data: {
              hasWarrantyHold: false,
              warrantyHoldPct: new Prisma.Decimal(0),
              warrantyHoldAmount: new Prisma.Decimal(0),
              warrantyDueDate: null,
            },
          });
        }
      }

      const qtyByItem = sumQtyByItem(
        invoice.lines.map((l) => ({ itemId: l.itemId, qty: toNum(l.qty) }))
      );
      const itemIds = Array.from(qtyByItem.keys());

      const stocks = await tx.stock.findMany({
        where: { locationId: warehouse.id, itemId: { in: itemIds } },
        select: { itemId: true, qty: true, avgCost: true },
      });

      const stockMap = new Map<string, { qty: number; avgCost: number }>();
      for (const s of stocks) stockMap.set(s.itemId, { qty: toNum(s.qty), avgCost: toNum(s.avgCost) });

      if (isOutType(invoice.type as InvoiceType)) {
        const items = await tx.item.findMany({
          where: { id: { in: itemIds } },
          select: { id: true, name: true, sku: true },
        });
        const nameMap = new Map(items.map((it) => [it.id, it.name || it.sku || it.id]));

        const errors: string[] = [];
        for (const [itemId, needQty] of qtyByItem.entries()) {
          const curQty = stockMap.get(itemId)?.qty;
          const name = nameMap.get(itemId) ?? itemId;

          if (curQty == null) errors.push(`Sản phẩm "${name}" chưa có tồn trong kho để xuất.`);
          else if (curQty <= 0) errors.push(`Sản phẩm "${name}" đã hết hàng trong kho.`);
          else if (curQty < needQty)
            errors.push(`Sản phẩm "${name}" không đủ tồn (còn ${curQty}, cần ${needQty}).`);
        }
        if (errors.length) throw httpError(400, errors.join(" "));
      }

      if (isOutType(invoice.type as InvoiceType)) {
        await Promise.all(
          invoice.lines.map((l) => {
            const avg = stockMap.get(l.itemId)?.avgCost ?? 0;
            const qty = toNum(l.qty);
            const costTotal = avg * qty;
            return tx.invoiceLine.update({
              where: { id: l.id },
              data: {
                unitCost: new Prisma.Decimal(avg),
                costTotal: new Prisma.Decimal(costTotal),
              },
            });
          })
        );

        const totalCost = invoice.lines.reduce((s, l) => {
          const avg = stockMap.get(l.itemId)?.avgCost ?? 0;
          return s + avg * toNum(l.qty);
        }, 0);

        await tx.invoice.update({
          where: { id: invoice.id },
          data: { totalCost: new Prisma.Decimal(totalCost) },
        });
      }

      if (invoice.type === "PURCHASE") {
        const moneyByItem = new Map<string, number>();
        for (const l of invoice.lines) {
          const qty = toNum(l.qty);
          const unit = toNum(l.price);
          moneyByItem.set(l.itemId, (moneyByItem.get(l.itemId) || 0) + qty * unit);
        }

        for (const [itemId, inQty] of qtyByItem.entries()) {
          const inMoney = moneyByItem.get(itemId) || 0;
          const inUnitCost = inQty > 0 ? inMoney / inQty : 0;

          const existing = await tx.stock.findUnique({
            where: { itemId_locationId: { itemId, locationId: warehouse.id } },
            select: { qty: true, avgCost: true },
          });

          const curQty = existing ? toNum(existing.qty) : 0;
          const curAvg = existing ? toNum(existing.avgCost) : 0;

          const newAvg = computeNewAvgCost({ curQty, curAvg, inQty, inUnitCost });
          const newQty = curQty + inQty;

          if (!existing) {
            await tx.stock.create({
              data: {
                itemId,
                locationId: warehouse.id,
                qty: new Prisma.Decimal(inQty),
                avgCost: new Prisma.Decimal(newAvg),
              },
            });
          } else {
            await tx.stock.update({
              where: { itemId_locationId: { itemId, locationId: warehouse.id } },
              data: {
                qty: new Prisma.Decimal(newQty),
                avgCost: new Prisma.Decimal(newAvg),
              },
            });
          }
        }
      }

      if (invoice.type === "SALES" || invoice.type === "PURCHASE_RETURN") {
        const updatePromises: Array<Promise<any>> = [];
        for (const [itemId, outQty] of qtyByItem.entries()) {
          updatePromises.push(
            tx.stock.update({
              where: { itemId_locationId: { itemId, locationId: warehouse.id } },
              data: { qty: { increment: new Prisma.Decimal(-outQty) } },
            })
          );
        }
        if (updatePromises.length) await Promise.all(updatePromises);
      }

      if (invoice.type === "SALES_RETURN") {
        const existingStockItemIds = new Set(stocks.map((s) => s.itemId));
        const createData: Array<any> = [];
        const updatePromises: Array<Promise<any>> = [];

        for (const [itemId, inQty] of qtyByItem.entries()) {
          const keepAvg = stockMap.get(itemId)?.avgCost ?? 0;
          if (!existingStockItemIds.has(itemId)) {
            createData.push({
              itemId,
              locationId: warehouse.id,
              qty: new Prisma.Decimal(inQty),
              avgCost: new Prisma.Decimal(keepAvg),
            });
          } else {
            updatePromises.push(
              tx.stock.update({
                where: { itemId_locationId: { itemId, locationId: warehouse.id } },
                data: { qty: { increment: new Prisma.Decimal(inQty) } },
              })
            );
          }
        }

        if (createData.length) await tx.stock.createMany({ data: createData });
        if (updatePromises.length) await Promise.all(updatePromises);
      }

      const mvType: MovementType = isInType(invoice.type as InvoiceType) ? "IN" : "OUT";

      await tx.movement.create({
        data: {
          type: mvType,
          posted: true,
          invoiceId: invoice.id,
          lines: {
            createMany: {
              data: Array.from(qtyByItem.entries()).map(([itemId, qty]) => {
                const absQty = Math.abs(qty);
                const avg = stockMap.get(itemId)?.avgCost ?? 0;

                let unitCost: number | null = null;

                if (invoice.type === "PURCHASE") {
                  const totalMoney = invoice.lines
                    .filter((l) => l.itemId === itemId)
                    .reduce((s, l) => s + toNum(l.qty) * toNum(l.price), 0);
                  unitCost = absQty > 0 ? totalMoney / absQty : 0;
                } else {
                  unitCost = avg;
                }

                const costTotalLine = unitCost == null ? null : unitCost * absQty;

                return {
                  itemId,
                  qty: new Prisma.Decimal(qty),
                  toLocationId: mvType === "IN" ? warehouse.id : null,
                  fromLocationId: mvType === "OUT" ? warehouse.id : null,
                  unitCost: unitCost == null ? null : new Prisma.Decimal(unitCost),
                  costTotal: costTotalLine == null ? null : new Prisma.Decimal(costTotalLine),
                };
              }),
            },
          },
        },
      });

      await tx.invoice.update({
        where: { id: invoice.id },
        data: {
          status: "APPROVED",
          approvedById: params.approvedById,
          approvedAt: new Date(),
        },
      });

      await ensureWarrantyHoldOnApprove(tx, invoice.id, {
        userId: auditCtx?.userId ?? params.approvedById,
        userRole: auditCtx?.userRole,
        meta: auditCtx?.meta,
      });

      // ✅ Sync allocations:
      // - for SALES/PURCHASE only
      // - return types are forced to 0/unpaid by syncInvoicePaidFromAllocations()
      await syncInvoicePaidFromAllocations(tx, invoice.id);

      if (invoice.type === "SALES_RETURN") {
        const originId = originForReturn?.id || String((invoice as any).refInvoiceId || "");
        if (!originId) throw httpError(400, "Thiếu refInvoiceId để cập nhật hóa đơn gốc.");

        await applySalesReturnToOrigin(tx, {
          returnInvoiceId: invoice.id,
          originInvoiceId: originId,
          actorId: auditCtx?.userId ?? params.approvedById,
          auditCtx,
        });
      }

      const after = await getInvoiceAuditSnapshot(tx, params.invoiceId);

      await auditLog(tx, {
        userId: auditCtx?.userId ?? params.approvedById,
        userRole: auditCtx?.userRole,
        action: "INVOICE_APPROVE",
        entity: "Invoice",
        entityId: params.invoiceId,
        before,
        after,
        meta: mergeMeta(auditCtx?.meta, {
          warehouseId: warehouse.id,
          originInvoiceId: originForReturn?.id ?? null,
        }),
      });

      return getInvoiceByIdTx(tx, invoice.id);
    },
    { timeout: 20000, maxWait: 5000 }
  );
}

export async function rejectInvoice(
  params: { invoiceId: string; approvedById: string; reason?: string },
  auditCtx?: AuditCtx
) {
  await prisma.$transaction(
    async (tx) => {
      const before = await getInvoiceAuditSnapshot(tx, params.invoiceId);

      const inv = await tx.invoice.findUnique({
        where: { id: params.invoiceId },
        select: { status: true },
      });
      if (!inv) throw httpError(404, "Invoice not found");
      if (inv.status === "APPROVED") throw httpError(409, "Hóa đơn đã duyệt, không thể từ chối.");
      if (inv.status === "REJECTED") throw httpError(409, "Hóa đơn đã bị từ chối rồi.");
      if (inv.status !== "SUBMITTED") {
        throw httpError(409, "Chỉ hoá đơn CHỜ DUYỆT (SUBMITTED) mới được từ chối.");
      }

      await tx.invoice.update({
        where: { id: params.invoiceId },
        data: {
          status: "REJECTED",
          approvedById: params.approvedById,
          approvedAt: new Date(),
          note: params.reason ? `[REJECT] ${params.reason}` : undefined,
        },
      });

      const after = await getInvoiceAuditSnapshot(tx, params.invoiceId);

      await auditLog(tx, {
        userId: auditCtx?.userId ?? params.approvedById,
        userRole: auditCtx?.userRole,
        action: "INVOICE_REJECT",
        entity: "Invoice",
        entityId: params.invoiceId,
        before,
        after,
        meta: mergeMeta(auditCtx?.meta, { reason: params.reason ?? null }),
      });
    },
    { timeout: 20000, maxWait: 5000 }
  );

  return getInvoiceById(params.invoiceId);
}

/** ========================= Legacy Posting (deprecated) ========================= **/

export async function postInvoiceToStock(invoiceId: string, _auditCtx?: AuditCtx) {
  const inv = await prisma.invoice.findUnique({
    where: { id: invoiceId },
    select: { status: true },
  });
  if (!inv) throw httpError(404, "Invoice not found");
  if (inv.status === "APPROVED") {
    throw httpError(409, "Hóa đơn đã duyệt. Không dùng postInvoiceToStock nữa. Hãy dùng approveInvoice.");
  }

  const mvCount = await prisma.movement.count({ where: { invoiceId } });
  if (mvCount > 0) {
    throw httpError(409, "Hóa đơn đã có movement. Không hỗ trợ post lại kiểu legacy.");
  }

  throw httpError(400, "postInvoiceToStock đã deprecated. Hãy gọi approveInvoice(invoiceId, approvedById).");
}

export async function unpostInvoiceStock(invoiceId: string, _auditCtx?: AuditCtx) {
  const inv = await prisma.invoice.findUnique({
    where: { id: invoiceId },
    select: { status: true },
  });
  if (!inv) throw httpError(404, "Invoice not found");
  if (inv.status === "APPROVED") {
    throw httpError(409, "Hóa đơn đã duyệt (chốt sổ). Không hỗ trợ unpost. Hãy dùng chứng từ điều chỉnh/hoàn trả.");
  }
  throw httpError(400, "unpostInvoiceStock đã deprecated theo mô hình chốt sổ.");
}

export async function hardDeleteInvoice(id: string) {
  const inv = await prisma.invoice.findUnique({
    where: { id },
    select: { status: true },
  });
  if (!inv) throw httpError(404, "Invoice not found");
  if (inv.status === "APPROVED") {
    throw httpError(409, "Hóa đơn đã duyệt (chốt sổ). Không hỗ trợ hard delete.");
  }
  const mvCount = await prisma.movement.count({ where: { invoiceId: id } });
  if (mvCount > 0) throw httpError(409, "Invoice đã có movement, không hard delete.");

  await prisma.$transaction(async (tx) => {
    await tx.movement.deleteMany({ where: { invoiceId: id } });
    await tx.warrantyHold.deleteMany({ where: { invoiceId: id } });
    await tx.invoiceLine.deleteMany({ where: { invoiceId: id } });
    await tx.invoice.delete({ where: { id } });
  });

  return { ok: true, deleted: true };
}

/** ========================= Revenue Aggregation ========================= **/

function revenueSign(t: InvoiceType) {
  if (t === "SALES") return 1;
  if (t === "SALES_RETURN") return -1;
  return 0;
}

export async function aggregateRevenue(params: {
  from?: Date;
  to?: Date;
  type?: InvoiceType;
  saleUserId?: string;
  techUserId?: string;
  q?: string;
  paymentStatus?: PaymentStatus;
  dateField?: "issueDate" | "approvedAt";
  status?: InvoiceStatus;
  receiveAccountId?: string;
}) {
  const where: Prisma.InvoiceWhereInput = {
    status: "APPROVED",
    type: { in: ["SALES", "SALES_RETURN"] },
  };

  if (params.saleUserId) where.saleUserId = params.saleUserId as any;
  if (params.techUserId) where.techUserId = params.techUserId as any;
  if (params.receiveAccountId) where.receiveAccountId = params.receiveAccountId;
  if (params.paymentStatus) where.paymentStatus = params.paymentStatus;

  const dateField = params.dateField || "approvedAt";

  if (params.from || params.to) {
    (where as any)[dateField] = {};
    if (params.from) ((where as any)[dateField] as any).gte = params.from;
    if (params.to) ((where as any)[dateField] as any).lte = params.to;
  }

  if (params.q) {
    (where as any).OR = [
      { code: { contains: params.q, mode: "insensitive" } },
      { partnerName: { contains: params.q, mode: "insensitive" } },
    ];
  }

  const rows = await prisma.invoice.findMany({
    where,
    select: {
      id: true,
      code: true,
      type: true,
      issueDate: true,
      approvedAt: true,

      subtotal: true,
      tax: true,
      total: true,

      netSubtotal: true,
      netTax: true,
      netTotal: true,

      paidAmount: true,

      saleUserId: true,
      saleUserName: true,
      techUserId: true,
      techUserName: true,

      hasWarrantyHold: true,
      warrantyHoldPct: true,
      warrantyHoldAmount: true,
    },
    orderBy: { approvedAt: "asc" },
  });

  let netRevenue = 0;
  let netCollected = 0;

  const bySale = new Map<
    string,
    { userId: string | null; name: string; revenue: number; collected: number; count: number }
  >();
  const byTech = new Map<
    string,
    { userId: string | null; name: string; revenue: number; collected: number; count: number }
  >();

  for (const r of rows) {
    const sign = revenueSign(r.type as InvoiceType);

    const total = toNum(r.total);
    const tax = toNum(r.tax);

    const subtotal =
      toNum(r.subtotal) > 0 ? toNum(r.subtotal) : Math.max(0, roundMoney(total - tax));

    const netTotal = toNum((r as any).netTotal);
    const netSubtotal = toNum((r as any).netSubtotal);

    const baseTotal = Number.isFinite(netTotal) && netTotal >= 0 ? netTotal : total;
    const baseSubtotal2 = Number.isFinite(netSubtotal) && netSubtotal >= 0 ? netSubtotal : subtotal;

    const calc = computeWarrantyHoldAndCollectible({
      subtotal: baseSubtotal2,
      total: baseTotal,
      hasWarrantyHold: r.hasWarrantyHold === true,
      warrantyHoldPct: toNum(r.warrantyHoldPct),
      warrantyHoldAmount: toNum(r.warrantyHoldAmount) > 0 ? toNum(r.warrantyHoldAmount) : undefined,
      legacyPct: 5,
    });

    const hold = r.hasWarrantyHold ? calc.holdAmount : 0;

    const recognizedNet = Math.max(0, roundMoney(baseTotal - toNum(r.tax) - hold));
    const recognizedRevenue = sign * recognizedNet;

    const collected = sign * toNum(r.paidAmount);

    netRevenue += recognizedRevenue;
    netCollected += collected;

    const saleKey = r.saleUserId || r.saleUserName || "UNKNOWN";
    const saleName = r.saleUserName || r.saleUserId || "UNKNOWN";
    const s0 = bySale.get(saleKey) || {
      userId: r.saleUserId,
      name: saleName,
      revenue: 0,
      collected: 0,
      count: 0,
    };
    s0.revenue += recognizedRevenue;
    s0.collected += collected;
    s0.count += 1;
    bySale.set(saleKey, s0);

    const techKey = r.techUserId || r.techUserName || "UNKNOWN";
    const techName = r.techUserName || r.techUserId || "UNKNOWN";
    const t0 = byTech.get(techKey) || {
      userId: r.techUserId,
      name: techName,
      revenue: 0,
      collected: 0,
      count: 0,
    };
    t0.revenue += recognizedRevenue;
    t0.collected += collected;
    t0.count += 1;
    byTech.set(techKey, t0);
  }

  return {
    totalInvoices: rows.length,
    netRevenue,
    netCollected,
    bySale: Array.from(bySale.values()).sort((a, b) => b.revenue - a.revenue),
    byTech: Array.from(byTech.values()).sort((a, b) => b.revenue - a.revenue),
  };
}

export async function recallInvoice(
  params: { invoiceId: string; actorId: string },
  auditCtx?: AuditCtx
) {
  await prisma.$transaction(
    async (tx) => {
      const before = await getInvoiceAuditSnapshot(tx, params.invoiceId);

      const inv = await tx.invoice.findUnique({
        where: { id: params.invoiceId },
        select: { status: true },
      });

      if (!inv) throw httpError(404, "Invoice not found");
      if (inv.status !== "SUBMITTED") {
        throw httpError(409, "Chỉ hóa đơn CHỜ DUYỆT mới được thu hồi.");
      }

      await tx.invoice.update({
        where: { id: params.invoiceId },
        data: {
          status: "DRAFT",
          approvedById: null,
          approvedAt: null,
        },
      });

      const after = await getInvoiceAuditSnapshot(tx, params.invoiceId);

      await auditLog(tx, {
        userId: auditCtx?.userId ?? params.actorId,
        userRole: auditCtx?.userRole,
        action: "INVOICE_RECALL",
        entity: "Invoice",
        entityId: params.invoiceId,
        before,
        after,
        meta: mergeMeta(auditCtx?.meta, {}),
      });
    },
    { timeout: 20000, maxWait: 5000 }
  );

  return getInvoiceById(params.invoiceId);
}

/**
 * khóa cập nhật thanh toán trực tiếp trên invoice
 */
export async function updateInvoicePayment(_params: {
  invoiceId: string;
  paidAmount: number;
  receiveAccountId?: string | null;
}) {
  throw httpError(
    409,
    "Không cập nhật paidAmount trực tiếp trên hóa đơn nữa. Vui lòng tạo phiếu thu/chi tại /payments (có allocations) để có lịch sử và tránh sai lệch."
  );
}
