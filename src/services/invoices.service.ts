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
 * ✅ CHỐT THEO PHƯƠNG ÁN MỚI:
 * - warrantyHoldAmount tính trên DOANH THU KHÔNG VAT (subtotal)
 * - collectible (tiền cần thu ngay) vẫn là tiền khách cần trả: total - holdAmount
 *   (VAT vẫn thu đủ ngay, chỉ "giữ lại" phần BH theo subtotal)
 */
function computeWarrantyHoldAndCollectible(params: {
  subtotal: number;
  total: number;
  hasWarrantyHold: boolean;
  warrantyHoldPct: number;
}) {
  const subtotal = roundMoney(params.subtotal || 0);
  const total = roundMoney(params.total || 0);

  const pct =
    params.hasWarrantyHold && Number.isFinite(params.warrantyHoldPct) && params.warrantyHoldPct > 0
      ? params.warrantyHoldPct
      : params.hasWarrantyHold
        ? 5
        : 0;

  // ✅ HOLD tính trên subtotal (không VAT)
  const holdAmount = params.hasWarrantyHold ? roundMoney((subtotal * pct) / 100) : 0;

  // ✅ collectible vẫn theo total (khách trả VAT đầy đủ), trừ đi phần hold
  const collectible = Math.max(0, roundMoney(total - holdAmount));

  return { pct, holdAmount, collectible };
}

/**
 * ✅ Chuẩn hoá payment (legacy: khi tạo invoice cho phép set paidAmount)
 * - UNPAID => paidAmount = 0
 * - PAID => paidAmount = total
 * - PARTIAL => paidAmount = body.paidAmount (bắt buộc)
 * - nếu không gửi paymentStatus => suy luận từ paidAmount
 *
 * NOTE: normalizePayment chỉ xử lý theo "total" (gross).
 * Với treo BH, status thực sự sẽ được quyết định theo "collectible"
 * ở các bước phía dưới (create/approve/sync allocations).
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
 * ✅ NEW: Validate & load invoice gốc cho SALES_RETURN (hướng A)
 * - SALES_RETURN bắt buộc có refInvoiceId
 * - invoice gốc phải là SALES và đã APPROVED
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
    },
  });

  if (!origin) throw httpError(400, "Không tìm thấy hóa đơn gốc (refInvoiceId).");

  if (origin.type !== "SALES") {
    throw httpError(400, "Hóa đơn gốc của phiếu trả hàng phải là hóa đơn BÁN (SALES).");
  }

  if (origin.status !== "APPROVED") {
    throw httpError(400, "Hóa đơn gốc chưa được DUYỆT nên chưa thể tạo/duyệt phiếu trả hàng.");
  }

  return origin;
}

/**
 * ✅ (CÁCH B) Sync invoice.paidAmount/paymentStatus từ allocations (NORMAL)
 * - Với treo BH: PAID khi đã thu đủ collectible = total - hold(subtotal)
 * - Đảm bảo hold được tính cả khi warrantyHoldAmount đang 0 (do legacy)
 *
 * QUY ƯỚC TRONG FILE NÀY:
 * - invoice.paidAmount = số NORMAL đã thu (đã clamp theo collectible)
 *   (HOLD tiền treo BH không cộng vào paidAmount ở invoice)
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
      hasWarrantyHold: true,
      warrantyHoldPct: true,
      warrantyHoldAmount: true,
    },
  });
  if (!inv) throw httpError(404, "Invoice not found");

  const agg = await tx.paymentAllocation.aggregate({
    where: { invoiceId, kind: "NORMAL" },
    _sum: { amount: true },
  });

  const sumNormal = toNum(agg._sum.amount); // can be negative for SALES_RETURN
  const paidAbs = Math.abs(sumNormal);

  const total = toNum(inv.total);

  // ✅ subtotal robust: ưu tiên field subtotal, fallback total - tax
  const subtotal =
    toNum(inv.subtotal) > 0
      ? toNum(inv.subtotal)
      : Math.max(0, roundMoney(total - toNum(inv.tax)));

  // ✅ robust hold derive: nếu hasHold mà warrantyHoldAmount chưa set thì derive theo pct (trên subtotal)
  const pct = toNum(inv.warrantyHoldPct);
  const derived = computeWarrantyHoldAndCollectible({
    subtotal,
    total,
    hasWarrantyHold: inv.hasWarrantyHold === true,
    warrantyHoldPct: pct,
  });

  const hold =
    inv.hasWarrantyHold === true
      ? toNum(inv.warrantyHoldAmount) > 0
        ? toNum(inv.warrantyHoldAmount)
        : derived.holdAmount
      : 0;

  const collectible = Math.max(0, roundMoney(total - hold));

  let paymentStatus: PaymentStatus = "UNPAID";
  if (paidAbs <= 0) paymentStatus = "UNPAID";
  else if (paidAbs + 0.0001 < collectible) paymentStatus = "PARTIAL";
  else paymentStatus = "PAID";

  await tx.invoice.update({
    where: { id: invoiceId },
    data: {
      paidAmount: new Prisma.Decimal(Math.min(paidAbs, collectible)),
      paymentStatus,
    },
  });
}

/**
 * ✅ (CÁCH B) Nếu lúc tạo invoice có paidAmount > 0 => tạo Payment + Allocation
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

  if (!params.partnerId) {
    throw httpError(400, "Hóa đơn có 'Đã thu' nhưng chưa chọn khách hàng (partner).");
  }

  const inv = await tx.invoice.findUnique({
    where: { id: invoiceId },
    select: {
      type: true,
      code: true,
      subtotal: true,
      tax: true,
      total: true,
      hasWarrantyHold: true,
      warrantyHoldPct: true,
      warrantyHoldAmount: true,
    },
  });
  if (!inv) throw httpError(404, "Invoice not found");

  const paymentType =
    inv.type === "PURCHASE" || inv.type === "SALES_RETURN" ? "PAYMENT" : "RECEIPT";

  const total = toNum(inv.total);
  const subtotal =
    toNum(inv.subtotal) > 0 ? toNum(inv.subtotal) : Math.max(0, roundMoney(total - toNum(inv.tax)));

  const derived = computeWarrantyHoldAndCollectible({
    subtotal,
    total,
    hasWarrantyHold: inv.hasWarrantyHold === true,
    warrantyHoldPct: toNum(inv.warrantyHoldPct),
  });

  const hold =
    inv.hasWarrantyHold === true
      ? toNum(inv.warrantyHoldAmount) > 0
        ? toNum(inv.warrantyHoldAmount)
        : derived.holdAmount
      : 0;

  const collectible = Math.max(0, roundMoney(total - hold));

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
 * Recompute subtotal / total cho một invoice:
 * ✅ Đồng thời chuẩn hoá lại paidAmount theo rule legacy
 *
 * NOTE: hàm này chỉ dùng khi add/update/delete line ở DRAFT.
 * Với treo BH, status sẽ được sync theo allocations khi cần.
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
 * ✅ CHỈ cho sửa khi DRAFT
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

    subtotal: toNum(inv.subtotal),
    tax: toNum(inv.tax),
    total: toNum(inv.total),
    paidAmount: toNum(inv.paidAmount),
    warrantyHoldPct: toNum(inv.warrantyHoldPct),
    warrantyHoldAmount: toNum(inv.warrantyHoldAmount),
    totalCost: toNum(inv.totalCost),
  };
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
 * ✅ (CÁCH B) Create invoice
 * ✅ FIX: nếu hasWarrantyHold => tính warrantyHoldAmount ngay (không đợi approve)
 * ✅ FIX: paymentStatus khi paidClamped >0 phải so với collectible (total - hold(subtotal))
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
  const tax = calcTaxFromBody(subtotal, body);
  const { total, paidAmount } = normalizePayment(subtotal, tax, body);

  const hasWarrantyHold = body?.hasWarrantyHold === true;
  const warrantyHoldPctRaw =
    body?.warrantyHoldPct !== undefined &&
    body?.warrantyHoldPct !== null &&
    body?.warrantyHoldPct !== ""
      ? Number(body.warrantyHoldPct)
      : 0;

  const warrantyHoldPct = hasWarrantyHold
    ? Number.isFinite(warrantyHoldPctRaw) && warrantyHoldPctRaw > 0
      ? warrantyHoldPctRaw
      : 5
    : 0;

  try {
    const created = await prisma.$transaction(
      async (tx) => {
        const receiveAccountId = await validateReceiveAccountId(tx, body.receiveAccountId);

        let origin: Awaited<ReturnType<typeof requireValidRefInvoiceForSalesReturn>> | null = null;
        if (type === "SALES_RETURN") {
          origin = await requireValidRefInvoiceForSalesReturn(tx, body.refInvoiceId);

          if (!body.partnerId) body.partnerId = origin.partnerId ?? null;
          if (!body.partnerName) body.partnerName = origin.partnerName ?? null;

          if (body.partnerPhone == null) body.partnerPhone = origin.partnerPhone ?? null;
          if (body.partnerTax == null) body.partnerTax = origin.partnerTax ?? null;
          if (body.partnerAddr == null) body.partnerAddr = origin.partnerAddr ?? null;
        }

        if (type === "SALES_RETURN" && !body.partnerId) {
          throw httpError(
            400,
            "Phiếu KHÁCH TRẢ HÀNG cần có khách hàng (partnerId). Hãy chọn hóa đơn gốc hoặc chọn khách hàng."
          );
        }

        // ✅ tính warrantyHoldAmount ngay khi tạo (để UI/logic không hiểu nhầm)
        const holdCalc = computeWarrantyHoldAndCollectible({
          subtotal,
          total,
          hasWarrantyHold: type === "SALES" && hasWarrantyHold,
          warrantyHoldPct,
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
            tax: new Prisma.Decimal(tax),
            total: new Prisma.Decimal(total),

            paymentStatus: "UNPAID",
            paidAmount: new Prisma.Decimal(0),

            hasWarrantyHold: type === "SALES" ? hasWarrantyHold : false,
            warrantyHoldPct: new Prisma.Decimal(type === "SALES" ? holdCalc.pct : 0),
            warrantyHoldAmount: new Prisma.Decimal(type === "SALES" ? holdCalc.holdAmount : 0),
            warrantyDueDate: type === "SALES" ? due : null,

            status: "DRAFT",
          },
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

        let paidClamped = 0;
        if (paidAmount > 0) {
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
          // ✅ FIX: so với collectible (total - hold(subtotal)), không phải total
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

        // ✅ AUDIT create
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
 * ✅ FIX: nếu đổi lines/tax/total hoặc warrantyHoldPct/hasWarrantyHold => recompute warrantyHoldAmount (trên subtotal)
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

        if (nextType === "SALES_RETURN") {
          const refId =
            body.refInvoiceId !== undefined ? body.refInvoiceId : current.refInvoiceId;

          const origin = await requireValidRefInvoiceForSalesReturn(tx, refId);

          if (body.partnerId == null) body.partnerId = origin.partnerId ?? null;
          if (body.partnerName == null) body.partnerName = origin.partnerName ?? null;
          if (body.partnerPhone == null) body.partnerPhone = origin.partnerPhone ?? null;
          if (body.partnerTax == null) body.partnerTax = origin.partnerTax ?? null;
          if (body.partnerAddr == null) body.partnerAddr = origin.partnerAddr ?? null;

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

          const tax = calcTaxFromBody(subtotal, body);
          const total = subtotal + tax;

          await tx.invoice.update({
            where: { id },
            data: {
              subtotal: new Prisma.Decimal(subtotal),
              tax: new Prisma.Decimal(tax),
              total: new Prisma.Decimal(total),
            },
          });

          changedTotals = true;
        }

        // ✅ recompute warrantyHoldAmount nếu invoice SALES và có hold (trên subtotal)
        const fresh = await tx.invoice.findUnique({
          where: { id },
          select: {
            type: true,
            subtotal: true,
            tax: true,
            total: true,
            issueDate: true,
            hasWarrantyHold: true,
            warrantyHoldPct: true,
          },
        });

        if (fresh && fresh.type === "SALES") {
          const total = toNum(fresh.total);
          const subtotal =
            toNum(fresh.subtotal) > 0
              ? toNum(fresh.subtotal)
              : Math.max(0, roundMoney(total - toNum(fresh.tax)));

          const calc = computeWarrantyHoldAndCollectible({
            subtotal,
            total,
            hasWarrantyHold: fresh.hasWarrantyHold === true,
            warrantyHoldPct: toNum(fresh.warrantyHoldPct),
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

        // sync paid/status theo allocations (đặc biệt quan trọng khi total/hold đổi)
        if (
          Array.isArray(body.lines) ||
          body.hasWarrantyHold !== undefined ||
          body.warrantyHoldPct !== undefined ||
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

      if (invoice.type === "SALES_RETURN") {
        const origin = await requireValidRefInvoiceForSalesReturn(tx, (invoice as any).refInvoiceId);

        await tx.invoice.update({
          where: { id: invoice.id },
          data: {
            refInvoiceId: origin.id,

            partnerId: origin.partnerId ?? null,
            partnerName: origin.partnerName ?? null,
            partnerPhone: origin.partnerPhone ?? null,
            partnerTax: origin.partnerTax ?? null,
            partnerAddr: origin.partnerAddr ?? null,

            saleUserId: origin.saleUserId ?? null,
            saleUserName: origin.saleUserName ?? null,
            techUserId: origin.techUserId ?? null,
            techUserName: origin.techUserName ?? null,

            receiveAccountId: invoice.receiveAccountId ?? origin.receiveAccountId ?? null,
          },
        });
      }

      /**
       * ✅ SALES: tính lại warrantyHold fields ngay trước khi approve để đảm bảo data đúng
       * ✅ HOLD tính trên subtotal (không VAT)
       */
      if (invoice.type === "SALES") {
        const hasHold = invoice.hasWarrantyHold === true;

        if (hasHold) {
          const pct = (() => {
            const p = toNum((invoice as any).warrantyHoldPct);
            if (p > 0) return p;
            return 5;
          })();

          if (pct < 0 || pct > 100) throw httpError(400, "warrantyHoldPct không hợp lệ (0..100).");

          const total = toNum((invoice as any).total);
          const subtotal =
            toNum((invoice as any).subtotal) > 0
              ? toNum((invoice as any).subtotal)
              : Math.max(0, roundMoney(total - toNum((invoice as any).tax)));

          const holdAmount = roundMoney((subtotal * pct) / 100);

          const due = new Date((invoice as any).issueDate);
          due.setFullYear(due.getFullYear() + 1);

          await tx.invoice.update({
            where: { id: invoice.id },
            data: {
              hasWarrantyHold: true,
              warrantyHoldPct: new Prisma.Decimal(pct),
              warrantyHoldAmount: new Prisma.Decimal(holdAmount),
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
      for (const s of stocks)
        stockMap.set(s.itemId, { qty: toNum(s.qty), avgCost: toNum(s.avgCost) });

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

      // ✅ Set APPROVED
      await tx.invoice.update({
        where: { id: invoice.id },
        data: {
          status: "APPROVED",
          approvedById: params.approvedById,
          approvedAt: new Date(),
        },
      });

      // ✅ Create/Update/Void WarrantyHold theo invoice đã approve (có audit)
      await ensureWarrantyHoldOnApprove(tx, invoice.id, {
        userId: auditCtx?.userId ?? params.approvedById,
        userRole: auditCtx?.userRole,
        meta: auditCtx?.meta,
      });

      // ✅ Sync paid/status theo allocations NORMAL (đã clamp theo collectible)
      await syncInvoicePaidFromAllocations(tx, invoice.id);

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

/**
 * ✅ FIX CHUẨN THEO YÊU CẦU:
 * - HOLD tính trên subtotal (không VAT)
 * - Doanh thu nhân viên = total - tax - hold(subtotal)
 *
 * Lưu ý:
 * - Nếu DB chưa có warrantyHoldAmount nhưng có hasWarrantyHold + pct => derive lại theo subtotal.
 */
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

  let netRevenue = 0;   // ✅ net revenue (total - tax - hold(subtotal))
  let netCollected = 0; // thực thu NORMAL (paidAmount) (signed)

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

    // ✅ hold robust (derive nếu bị 0 do legacy) — derive trên subtotal
    let hold = 0;
    if (r.hasWarrantyHold === true) {
      const holdDb = toNum(r.warrantyHoldAmount);
      if (holdDb > 0) {
        hold = holdDb;
      } else {
        const derived = computeWarrantyHoldAndCollectible({
          subtotal,
          total,
          hasWarrantyHold: true,
          warrantyHoldPct: toNum(r.warrantyHoldPct),
        });
        hold = derived.holdAmount;
      }
    }

    // ✅ doanh thu thuần cho nhân viên: total - tax - hold(subtotal)
    const net = Math.max(0, roundMoney(total - tax - hold));
    const recognizedRevenue = sign * net;

    // ✅ collected: lấy theo paidAmount (đang là NORMAL đã thu, đã clamp theo collectible)
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

/** ========================= Sales Ledger + Payment deprecated ========================= **/

export type SalesLedgerRow = {
  issueDate: string;
  code: string;
  partnerName: string;

  itemName: string;
  itemSku?: string | null;

  qty: number;
  unitPrice: number;
  unitCost: number;
  costTotal: number;

  lineAmount: number;

  paid: number;
  debt: number;

  saleUserName: string;
  techUserName: string;
};

function round2(n: number) {
  return Math.round((n + Number.EPSILON) * 100) / 100;
}

export async function listSalesLedger(params: {
  from?: Date;
  to?: Date;
  q?: string;
  saleUserId?: string;
  techUserId?: string;
  paymentStatus?: PaymentStatus;
}) {
  const where: Prisma.InvoiceWhereInput = {
    status: "APPROVED",
    type: "SALES",
  };

  if (params.saleUserId) where.saleUserId = params.saleUserId as any;
  if (params.techUserId) where.techUserId = params.techUserId as any;
  if (params.paymentStatus) where.paymentStatus = params.paymentStatus;

  if (params.from || params.to) {
    where.issueDate = {};
    if (params.from) (where.issueDate as any).gte = params.from;
    if (params.to) (where.issueDate as any).lte = params.to;
  }

  if (params.q && params.q.trim()) {
    const q = params.q.trim();
    where.OR = [
      { code: { contains: q, mode: "insensitive" } },
      { partnerName: { contains: q, mode: "insensitive" } },
      { lines: { some: { itemName: { contains: q, mode: "insensitive" } } } },
      { lines: { some: { itemSku: { contains: q, mode: "insensitive" } } } },
    ];
  }

  const invoices = await prisma.invoice.findMany({
    where,
    orderBy: { issueDate: "desc" },
    include: {
      saleUser: true,
      techUser: true,
      lines: true,
      warrantyHold: true,
    },
  });

  const rows: SalesLedgerRow[] = [];

  for (const inv of invoices as any[]) {
    const invSubtotal = toNum(inv.subtotal);
    const invPaidNormal = toNum(inv.paidAmount);

    const base =
      invSubtotal > 0
        ? invSubtotal
        : (inv.lines || []).reduce((s: number, l: any) => s + toNum(l.amount), 0);

    const paidBase = Math.min(invPaidNormal, base > 0 ? base : invPaidNormal);

    const saleName =
      inv.saleUserName ||
      inv.saleUser?.username ||
      inv.saleUser?.name ||
      inv.saleUser?.email ||
      inv.saleUser?.id ||
      "";

    const techName =
      inv.techUserName ||
      inv.techUser?.username ||
      inv.techUser?.name ||
      inv.techUser?.email ||
      inv.techUser?.id ||
      "";

    const issueDateStr = new Date(inv.issueDate).toISOString().slice(0, 10);
    const partnerName = String(inv.partnerName || "");

    let paidAllocatedSum = 0;

    const linesArr: any[] = Array.isArray(inv.lines) ? inv.lines : [];
    for (let i = 0; i < linesArr.length; i++) {
      const l = linesArr[i];

      const qty = toNum(l.qty);
      const unitPrice = toNum(l.price);
      const lineAmount = toNum(l.amount);

      const unitCost = toNum(l.unitCost);
      const costTotal = toNum(l.costTotal);

      let paidLine = 0;
      if (base > 0 && paidBase > 0 && lineAmount > 0) {
        paidLine = round2((paidBase * lineAmount) / base);
      }

      if (i === linesArr.length - 1) {
        const remain = round2(paidBase - paidAllocatedSum);
        paidLine = Math.max(0, Math.min(lineAmount, remain));
      }

      paidAllocatedSum = round2(paidAllocatedSum + paidLine);
      const debt = round2(Math.max(0, lineAmount - paidLine));

      rows.push({
        issueDate: issueDateStr,
        code: String(inv.code),
        partnerName,

        itemName: String(l.itemName || ""),
        itemSku: l.itemSku ?? null,

        qty,
        unitPrice,
        unitCost,
        costTotal,

        lineAmount,

        paid: paidLine,
        debt,

        saleUserName: saleName,
        techUserName: techName,
      });
    }
  }

  const totals = rows.reduce(
    (acc, r) => {
      acc.totalRevenue = round2(acc.totalRevenue + r.lineAmount);
      acc.totalCost = round2(acc.totalCost + r.costTotal);
      acc.totalPaid = round2(acc.totalPaid + r.paid);
      acc.totalDebt = round2(acc.totalDebt + r.debt);
      return acc;
    },
    { totalRevenue: 0, totalCost: 0, totalPaid: 0, totalDebt: 0 }
  );

  return { rows, totals };
}

/**
 * ✅ NEW: khóa cập nhật thanh toán trực tiếp trên invoice
 */
export async function updateInvoicePayment(params: {
  invoiceId: string;
  paidAmount: number;
  receiveAccountId?: string | null;
}) {
  throw httpError(
    409,
    "Không cập nhật paidAmount trực tiếp trên hóa đơn nữa. Vui lòng tạo phiếu thu/chi tại /payments (có allocations) để có lịch sử và tránh sai lệch."
  );
}
