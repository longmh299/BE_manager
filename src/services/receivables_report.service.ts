import { PrismaClient, Prisma, InvoiceStatus, InvoiceType, AllocationKind } from "@prisma/client";
import ExcelJS from "exceljs";


const prisma = new PrismaClient();

type Money = Prisma.Decimal;

function D(v: any): Money {
  if (v == null) return new Prisma.Decimal(0);
  // Prisma Decimal already
  if (typeof v === "object" && typeof v.toString === "function") return new Prisma.Decimal(v.toString());
  return new Prisma.Decimal(v);
}
function max0(x: Money): Money {
  return x.lessThan(0) ? new Prisma.Decimal(0) : x;
}

function parseAsOf(asOf?: string) {
  if (!asOf) return new Date();
  if (/^\d{4}-\d{2}-\d{2}$/.test(asOf)) return new Date(asOf + "T00:00:00.000Z");
  return new Date(asOf);
}

/**
 * Fallback allocation logic cho data cũ:
 * - paidAmount ưu tiên cấn vào phần NORMAL trước
 * - phần dư mới cấn vào WARRANTY_HOLD
 */
function fallbackSplitPaid(total: Money, warrantyHoldAmount: Money, paidAmount: Money) {
  const normalPortion = max0(total.minus(warrantyHoldAmount));
  const normalPaid = paidAmount.lessThan(normalPortion) ? paidAmount : normalPortion;
  const warrantyPaid = paidAmount.greaterThan(normalPortion) ? paidAmount.minus(normalPortion) : new Prisma.Decimal(0);
  return { normalPaid, warrantyPaid };
}

export type ReceivableInvoiceRow = {
  invoiceId: string;
  code: string;
  issueDate: string; // yyyy-MM-dd
  partnerId: string | null;
  partnerName: string;

  // ✅ NEW: sale info
  saleUserId?: string | null;
  saleName?: string | null;

  total: number;

  hasWarrantyHold: boolean;
  warrantyHoldAmount: number;
  warrantyDueDate: string | null;

  // Paid breakdown
  paidTotal: number;
  paidNormal: number;
  paidWarranty: number;

  // Outstanding
  normalOutstanding: number;
  warrantyOutstanding: number;

  warrantyHoldNotDue: number;
  warrantyHoldDue: number;

  totalOutstanding: number;
};

export type ReceivablesByPartnerRow = {
  partnerId: string | null;
  partnerName: string;

  normalOutstanding: number;
  warrantyHoldNotDue: number;
  warrantyHoldDue: number;

  totalOutstanding: number;
  invoiceCount: number;
};

export async function getReceivablesReport(params: { asOf?: string; includeRows?: boolean }) {
  const asOf = parseAsOf(params.asOf);
  const includeRows = params.includeRows !== false;

  // 1) Lấy list invoice SALES đã APPROVED
  const invoices = await prisma.invoice.findMany({
    where: {
      type: "SALES" as InvoiceType,
      status: "APPROVED" as InvoiceStatus,
    },
    select: {
      id: true,
      code: true,
      issueDate: true,

      partnerId: true,
      partnerName: true,

      // ✅ NEW: sale info
      saleUserId: true,
      saleUserName: true,
      saleUser: {
        select: {
          id: true,
          username: true, // NOTE: schema User không có "name"
        },
      },

      total: true,
      paidAmount: true,

      hasWarrantyHold: true,
      warrantyHoldAmount: true,
      warrantyDueDate: true,

      warrantyHold: {
        select: {
          amount: true,
          dueDate: true,
          status: true,
        },
      },
    },
    orderBy: { issueDate: "desc" },
  });

  if (invoices.length === 0) {
    return {
      ok: true,
      data: {
        asOf: asOf.toISOString().slice(0, 10),
        byPartner: [] as ReceivablesByPartnerRow[],
        rows: [] as ReceivableInvoiceRow[],
        summary: {
          normalOutstanding: 0,
          warrantyHoldNotDue: 0,
          warrantyHoldDue: 0,
          totalOutstanding: 0,
          invoiceCount: 0,
        },
      },
    };
  }

  const invoiceIds = invoices.map((x) => x.id);

  // 2) Gom allocation theo invoiceId + kind
  const allocGroups = await prisma.paymentAllocation.groupBy({
    by: ["invoiceId", "kind"],
    where: { invoiceId: { in: invoiceIds } },
    _sum: { amount: true },
  });

  const allocMap = new Map<string, { normal: Money; warranty: Money }>();
  for (const g of allocGroups) {
    const cur = allocMap.get(g.invoiceId) ?? { normal: new Prisma.Decimal(0), warranty: new Prisma.Decimal(0) };
    const sumAmt = D(g._sum.amount);
    if (g.kind === ("NORMAL" as AllocationKind)) cur.normal = cur.normal.plus(sumAmt);
    if (g.kind === ("WARRANTY_HOLD" as AllocationKind)) cur.warranty = cur.warranty.plus(sumAmt);
    allocMap.set(g.invoiceId, cur);
  }

  // 3) Tính rows + aggregate by partner
  const byPartnerMap = new Map<string, ReceivablesByPartnerRow>();
  let sumNormal = 0;
  let sumHoldNotDue = 0;
  let sumHoldDue = 0;
  let sumTotal = 0;

  const rows: ReceivableInvoiceRow[] = [];

  for (const inv of invoices) {
    const total = D(inv.total);
    const paidTotal = D(inv.paidAmount);

    // warrantyHoldAmount: ưu tiên invoice.warrantyHoldAmount, nếu 0 mà có warrantyHold record thì dùng record
    let warrantyHoldAmount = D(inv.warrantyHoldAmount);
    if (warrantyHoldAmount.equals(0) && inv.warrantyHold?.amount) {
      warrantyHoldAmount = D(inv.warrantyHold.amount);
    }

    const hasWarrantyHold = Boolean(inv.hasWarrantyHold) || warrantyHoldAmount.greaterThan(0);

    // dueDate: ưu tiên invoice.warrantyDueDate, nếu null mà có warrantyHold record thì dùng record
    let dueDate: Date | null = inv.warrantyDueDate ?? null;
    if (!dueDate && inv.warrantyHold?.dueDate) dueDate = inv.warrantyHold.dueDate;

    const alloc = allocMap.get(inv.id);
    const hasAnyAllocation = alloc != null;

    let paidNormal = new Prisma.Decimal(0);
    let paidWarranty = new Prisma.Decimal(0);

    if (hasAnyAllocation) {
      paidNormal = D(alloc!.normal);
      paidWarranty = D(alloc!.warranty);
    } else {
      // fallback: data cũ chưa dùng allocation kind
      const split = fallbackSplitPaid(total, warrantyHoldAmount, paidTotal);
      paidNormal = split.normalPaid;
      paidWarranty = split.warrantyPaid;
    }

    // normal portion = total - warrantyHoldAmount (không âm)
    const normalPortion = max0(total.minus(warrantyHoldAmount));

    const normalOutstanding = max0(normalPortion.minus(paidNormal));
    const warrantyOutstanding = max0(warrantyHoldAmount.minus(paidWarranty));

    const matured = dueDate ? asOf.getTime() >= dueDate.getTime() : false;

    const warrantyHoldDue = matured ? warrantyOutstanding : new Prisma.Decimal(0);
    const warrantyHoldNotDue = matured ? new Prisma.Decimal(0) : warrantyOutstanding;

    const totalOutstanding = normalOutstanding.plus(warrantyOutstanding);

    const partnerKey = inv.partnerId ?? `__NO_PARTNER__:${inv.partnerName ?? ""}`;
    const partnerName = inv.partnerName ?? "(Không rõ đối tác)";

    const cur =
      byPartnerMap.get(partnerKey) ??
      ({
        partnerId: inv.partnerId ?? null,
        partnerName,
        normalOutstanding: 0,
        warrantyHoldNotDue: 0,
        warrantyHoldDue: 0,
        totalOutstanding: 0,
        invoiceCount: 0,
      } as ReceivablesByPartnerRow);

    const n = Number(normalOutstanding);
    const h = Number(warrantyHoldNotDue);
    const d = Number(warrantyHoldDue);
    const t = Number(totalOutstanding);

    cur.normalOutstanding += n;
    cur.warrantyHoldNotDue += h;
    cur.warrantyHoldDue += d;
    cur.totalOutstanding += t;
    cur.invoiceCount += 1;

    byPartnerMap.set(partnerKey, cur);

    sumNormal += n;
    sumHoldNotDue += h;
    sumHoldDue += d;
    sumTotal += t;

    if (includeRows) {
      // ✅ sale name fallback: saleUserName snapshot -> saleUser.username -> null
      const saleName = (inv.saleUserName || inv.saleUser?.username || null) as string | null;

      rows.push({
        invoiceId: inv.id,
        code: inv.code,
        issueDate: inv.issueDate.toISOString().slice(0, 10),
        partnerId: inv.partnerId ?? null,
        partnerName,

        saleUserId: inv.saleUserId ?? null,
        saleName,

        total: Number(total),

        hasWarrantyHold,
        warrantyHoldAmount: Number(warrantyHoldAmount),
        warrantyDueDate: dueDate ? dueDate.toISOString().slice(0, 10) : null,

        paidTotal: Number(paidTotal),
        paidNormal: Number(paidNormal),
        paidWarranty: Number(paidWarranty),

        normalOutstanding: n,
        warrantyOutstanding: Number(warrantyOutstanding),

        warrantyHoldNotDue: h,
        warrantyHoldDue: d,

        totalOutstanding: t,
      });
    }
  }

  const byPartner = Array.from(byPartnerMap.values()).sort((a, b) => b.totalOutstanding - a.totalOutstanding);

  return {
    ok: true,
    data: {
      asOf: asOf.toISOString().slice(0, 10),
      summary: {
        normalOutstanding: sumNormal,
        warrantyHoldNotDue: sumHoldNotDue,
        warrantyHoldDue: sumHoldDue,
        totalOutstanding: sumTotal,
        invoiceCount: invoices.length,
      },
      byPartner,
      rows,
    },
  };
}


function setHeaderStyle(cell: ExcelJS.Cell) {
  cell.font = { bold: true };
  cell.alignment = { vertical: "middle", horizontal: "center", wrapText: true };
  cell.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FFF3F4F6" }, // gray-100
  };
  cell.border = {
    top: { style: "thin" },
    left: { style: "thin" },
    bottom: { style: "thin" },
    right: { style: "thin" },
  };
}

function setMoneyCell(cell: ExcelJS.Cell) {
  // format tiền VND: 1,234,567
  cell.numFmt = '#,##0 "đ"';
  cell.alignment = { vertical: "middle", horizontal: "right" };
}

function setTextCell(cell: ExcelJS.Cell) {
  cell.alignment = { vertical: "middle", horizontal: "left", wrapText: true };
}

function addTitle(ws: ExcelJS.Worksheet, title: string, sub?: string) {
  ws.addRow([title]);
  ws.mergeCells(ws.rowCount, 1, ws.rowCount, 10);
  const r1 = ws.getRow(1);
  r1.height = 22;
  const c1 = ws.getCell("A1");
  c1.font = { bold: true, size: 14 };
  c1.alignment = { vertical: "middle", horizontal: "left" };

  if (sub) {
    ws.addRow([sub]);
    ws.mergeCells(ws.rowCount, 1, ws.rowCount, 10);
    const c2 = ws.getCell(`A2`);
    c2.font = { italic: true, size: 10, color: { argb: "FF6B7280" } };
    c2.alignment = { vertical: "middle", horizontal: "left" };
  }

  ws.addRow([]);
}

export async function buildReceivablesExcel(params: { asOf?: string; includeRows?: boolean }) {
  const res = await getReceivablesReport({ asOf: params.asOf, includeRows: params.includeRows });
  const data = res.data;

  const wb = new ExcelJS.Workbook();
  wb.creator = "Warehouse App";
  wb.created = new Date();

  // ===================== Sheet 1: Tổng quan =====================
  const ws1 = wb.addWorksheet("Tong quan", {
    views: [{ state: "frozen", ySplit: 4 }],
  });

  addTitle(
    ws1,
    "BÁO CÁO CÔNG NỢ PHẢI THU",
    `Chốt đến: ${data.asOf} • (Tất cả số liệu là GROSS - tiền thực)`
  );

  ws1.addRow(["Chỉ tiêu", "Giá trị"]);
  setHeaderStyle(ws1.getCell("A4"));
  setHeaderStyle(ws1.getCell("B4"));
  ws1.getColumn(1).width = 32;
  ws1.getColumn(2).width = 22;

  const s = data.summary;
  const rows1 = [
    ["Nợ thường", s.normalOutstanding],
    ["BH treo (chưa đến hạn)", s.warrantyHoldNotDue],
    ["BH đến hạn", s.warrantyHoldDue],
    ["Tổng phải thu", s.totalOutstanding],
    ["Số hóa đơn", s.invoiceCount],
  ];

  for (const [k, v] of rows1) {
    const r = ws1.addRow([k, v as any]);
    setTextCell(r.getCell(1));
    if (k === "Số hóa đơn") {
      r.getCell(2).alignment = { vertical: "middle", horizontal: "right" };
    } else {
      setMoneyCell(r.getCell(2));
    }
  }

  // dòng tổng nhấn mạnh
  const totalRowIdx = 4 + 1 + 3; // header row + 4 mục đầu (ước lượng)
  // không cần tuyệt đối, chỉ style theo text:
  for (let i = 1; i <= ws1.rowCount; i++) {
    if (ws1.getCell(i, 1).value === "Tổng phải thu") {
      ws1.getRow(i).font = { bold: true };
      ws1.getCell(i, 2).font = { bold: true };
    }
  }

  // ===================== Sheet 2: Theo khách =====================
  const ws2 = wb.addWorksheet("Theo khach", {
    views: [{ state: "frozen", ySplit: 4 }],
  });

  addTitle(ws2, "TỔNG HỢP THEO KHÁCH HÀNG", `Chốt đến: ${data.asOf}`);

  ws2.addRow([
    "Khách hàng",
    "Số HĐ",
    "Nợ thường",
    "BH treo",
    "BH đến hạn",
    "Tổng nợ",
  ]);

  const headerRow2 = ws2.getRow(4);
  headerRow2.height = 18;
  for (let c = 1; c <= 6; c++) setHeaderStyle(ws2.getCell(4, c));

  ws2.getColumn(1).width = 34;
  ws2.getColumn(2).width = 10;
  ws2.getColumn(3).width = 16;
  ws2.getColumn(4).width = 16;
  ws2.getColumn(5).width = 16;
  ws2.getColumn(6).width = 18;

  for (const p of data.byPartner) {
    const r = ws2.addRow([
      p.partnerName,
      p.invoiceCount,
      p.normalOutstanding,
      p.warrantyHoldNotDue,
      p.warrantyHoldDue,
      p.totalOutstanding,
    ]);
    setTextCell(r.getCell(1));
    r.getCell(2).alignment = { vertical: "middle", horizontal: "right" };
    setMoneyCell(r.getCell(3));
    setMoneyCell(r.getCell(4));
    setMoneyCell(r.getCell(5));
    setMoneyCell(r.getCell(6));
  }

  // tổng cuối sheet 2
  ws2.addRow([]);
  const sumRow2 = ws2.addRow([
    "TỔNG",
    s.invoiceCount,
    s.normalOutstanding,
    s.warrantyHoldNotDue,
    s.warrantyHoldDue,
    s.totalOutstanding,
  ]);
  sumRow2.font = { bold: true };
  setTextCell(sumRow2.getCell(1));
  sumRow2.getCell(2).alignment = { vertical: "middle", horizontal: "right" };
  setMoneyCell(sumRow2.getCell(3));
  setMoneyCell(sumRow2.getCell(4));
  setMoneyCell(sumRow2.getCell(5));
  setMoneyCell(sumRow2.getCell(6));

  // ===================== Sheet 3: Chi tiết hóa đơn =====================
  const ws3 = wb.addWorksheet("Chi tiet hoa don", {
    views: [{ state: "frozen", ySplit: 4 }],
  });

  addTitle(ws3, "CHI TIẾT HÓA ĐƠN CÒN NỢ", `Chốt đến: ${data.asOf}`);

  ws3.addRow([
    "Mã HĐ",
    "Ngày",
    "Khách hàng",
    "NV sale",
    "Tổng HĐ",
    "Đã thu",
    "Nợ thường",
    "BH treo",
    "BH đến hạn",
    "Tổng nợ",
  ]);

  for (let c = 1; c <= 10; c++) setHeaderStyle(ws3.getCell(4, c));

  ws3.getColumn(1).width = 14;
  ws3.getColumn(2).width = 12;
  ws3.getColumn(3).width = 28;
  ws3.getColumn(4).width = 18;
  ws3.getColumn(5).width = 16;
  ws3.getColumn(6).width = 16;
  ws3.getColumn(7).width = 16;
  ws3.getColumn(8).width = 16;
  ws3.getColumn(9).width = 16;
  ws3.getColumn(10).width = 16;

  const rowsDetail = Array.isArray(data.rows) ? data.rows : [];
  for (const x of rowsDetail) {
    const r = ws3.addRow([
      x.code,
      x.issueDate,
      x.partnerName,
      x.saleName ?? "",
      x.total,
      x.paidTotal,
      x.normalOutstanding,
      x.warrantyHoldNotDue,
      x.warrantyHoldDue,
      x.totalOutstanding,
    ]);
    setTextCell(r.getCell(1));
    setTextCell(r.getCell(2));
    setTextCell(r.getCell(3));
    setTextCell(r.getCell(4));
    for (let c = 5; c <= 10; c++) setMoneyCell(r.getCell(c));
  }

  // total cuối sheet 3
  ws3.addRow([]);
  const sumRow3 = ws3.addRow([
    "TỔNG",
    "",
    "",
    "",
    s.totalOutstanding + (s.invoiceCount ? 0 : 0), // chỉ để giữ cột, tiền tính dưới
    "",
    s.normalOutstanding,
    s.warrantyHoldNotDue,
    s.warrantyHoldDue,
    s.totalOutstanding,
  ]);
  sumRow3.font = { bold: true };
  ws3.mergeCells(sumRow3.number, 1, sumRow3.number, 4);
  setTextCell(sumRow3.getCell(1));
  // cột 7-10 mới là tổng thật
  setMoneyCell(sumRow3.getCell(7));
  setMoneyCell(sumRow3.getCell(8));
  setMoneyCell(sumRow3.getCell(9));
  setMoneyCell(sumRow3.getCell(10));

  const buf = await wb.xlsx.writeBuffer();
  return Buffer.from(buf as any);
}