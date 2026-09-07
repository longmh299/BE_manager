// src/services/invoiceTrace.service.ts
import { PrismaClient, Prisma, InvoiceType, InvoiceStatus } from "@prisma/client";

const prisma = new PrismaClient();

function toNum(d: Prisma.Decimal | number | string | null | undefined): number {
  if (d == null) return 0;
  if (typeof d === "number") return d;
  return Number(d.toString());
}

export type TraceInvoiceLineRow = {
  lineId: string;
  invoiceId: string;
  invoiceCode: string;
  invoiceType: InvoiceType;
  status: InvoiceStatus;
  issueDate: Date;
  partnerId: string | null;
  partnerName: string | null;
  saleUserId: string | null;
  saleUserName: string | null;

  itemId: string;
  itemSku: string | null;
  itemName: string | null;

  qty: number;
  price: number;
  amount: number;
  unitCost?: number | null;
  costTotal?: number | null;
};

export type TraceInvoiceFilter = {
  code: string; // mã sản phẩm (sku) cần tra - hỗ trợ tìm gần đúng, không phân biệt hoa/thường
  type: InvoiceType; // PURCHASE (nhập) | SALES (bán) | PURCHASE_RETURN | SALES_RETURN
  from?: Date;
  to?: Date;
  status?: InvoiceStatus;
  page?: number;
  pageSize?: number;

  // RBAC
  saleUserId?: string; // staff: chỉ xem hoá đơn của chính mình
  excludeTypes?: InvoiceType[]; // ví dụ: non-admin không được xem PURCHASE_RETURN
};

/**
 * Truy xuất hoá đơn theo mã sản phẩm.
 * Trả về DANH SÁCH DÒNG (line-level), không trả cả hoá đơn:
 * mỗi dòng cho biết sản phẩm mã X nằm trong hoá đơn nào, ngày nhập/xuất bao nhiêu, số lượng/đơn giá bao nhiêu.
 */
export async function traceInvoiceLinesByItemCode(params: TraceInvoiceFilter) {
  const code = (params.code || "").trim();
  if (!code) {
    const err: any = new Error("Vui lòng nhập mã sản phẩm cần tra cứu.");
    err.statusCode = 400;
    throw err;
  }
  if (!params.type) {
    const err: any = new Error("Vui lòng chọn loại hoá đơn (nhập/bán).");
    err.statusCode = 400;
    throw err;
  }

  const page = params.page && params.page > 0 ? params.page : 1;
  const pageSize = params.pageSize && params.pageSize > 0 ? params.pageSize : 20;

  const invoiceWhere: Prisma.InvoiceWhereInput = { type: params.type };

  if (params.excludeTypes && params.excludeTypes.length > 0) {
    invoiceWhere.NOT = { type: { in: params.excludeTypes } };
  }
  if (params.saleUserId) invoiceWhere.saleUserId = params.saleUserId;
  if (params.status) invoiceWhere.status = params.status;
  if (params.from || params.to) {
    invoiceWhere.issueDate = {};
    if (params.from) (invoiceWhere.issueDate as any).gte = params.from;
    if (params.to) (invoiceWhere.issueDate as any).lte = params.to;
  }

  const lineWhere: Prisma.InvoiceLineWhereInput = {
    invoice: { is: invoiceWhere },
    OR: [
      { itemSku: { contains: code, mode: "insensitive" } },
      { item: { is: { sku: { contains: code, mode: "insensitive" } } } },
    ],
  };

  const [total, rows] = await Promise.all([
    prisma.invoiceLine.count({ where: lineWhere }),
    prisma.invoiceLine.findMany({
      where: lineWhere,
      orderBy: { invoice: { issueDate: "desc" } },
      include: {
        item: { select: { id: true, sku: true, name: true } },
        invoice: {
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
          },
        },
      },
      skip: (page - 1) * pageSize,
      take: pageSize,
    }),
  ]);

  const data: TraceInvoiceLineRow[] = rows.map((l) => ({
    lineId: l.id,
    invoiceId: l.invoice.id,
    invoiceCode: l.invoice.code,
    invoiceType: l.invoice.type,
    status: l.invoice.status,
    issueDate: l.invoice.issueDate,
    partnerId: l.invoice.partnerId,
    partnerName: l.invoice.partnerName,
    saleUserId: l.invoice.saleUserId,
    saleUserName: l.invoice.saleUserName,

    itemId: l.itemId,
    itemSku: l.itemSku ?? l.item?.sku ?? null,
    itemName: l.itemName ?? l.item?.name ?? null,

    qty: toNum(l.qty),
    price: toNum(l.price),
    amount: toNum(l.amount),
    unitCost: l.unitCost != null ? toNum(l.unitCost) : null,
    costTotal: l.costTotal != null ? toNum(l.costTotal) : null,
  }));

  return { data, total, page, pageSize };
}