// src/services/reports.service.ts
import { PrismaClient, Prisma, MovementType, PaymentStatus } from "@prisma/client";
import ExcelJS from "exceljs";

const prisma = new PrismaClient();

function toNum(d: Prisma.Decimal | number | string | null | undefined): number {
  if (d == null) return 0;
  if (typeof d === "number") return d;
  return Number(d.toString());
}

function round2(n: number) {
  return Math.round((n + Number.EPSILON) * 100) / 100;
}

function pad2(n: number) {
  return String(n).padStart(2, "0");
}

// ✅ tránh lệch ngày/tháng do timezone (không dùng toISOString)
function toLocalYMD(d: Date) {
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
}

function parseYMD(ymd: string): { year: number; month: number; day: number } {
  const s = String(ymd || "").slice(0, 10);
  const [y, m, d] = s.split("-").map((x) => Number(x));
  return {
    year: Number.isFinite(y) ? y : 1970,
    month: Number.isFinite(m) ? m : 1,
    day: Number.isFinite(d) ? d : 1,
  };
}

export type LedgerRow = {
  at: string; // ISO
  movementId: string;
  movementType: MovementType;
  invoiceId?: string | null;
  invoiceCode?: string | null;
  invoiceType?: string | null;

  itemId: string;
  itemSku?: string | null;
  itemName?: string | null;

  qty: number; // IN: +, OUT: -, ADJUST: line sign
  unitCost?: number | null;
  costTotal?: number | null;

  note?: string | null;
};

export async function getLedger(params: {
  from?: Date;
  to?: Date;
  q?: string;
  itemId?: string;
  type?: MovementType;
}) {
  const whereMv: Prisma.MovementWhereInput = {
    posted: true,
  };

  // ✅ CHUẨN: lọc theo occurredAt (ngày phát sinh)
  if (params.from || params.to) {
    whereMv.occurredAt = {};
    if (params.from) (whereMv.occurredAt as any).gte = params.from;
    if (params.to) (whereMv.occurredAt as any).lte = params.to;
  }

  if (params.type) whereMv.type = params.type;

  if (params.q && params.q.trim()) {
    const q = params.q.trim();

    whereMv.OR = [
      { refNo: { contains: q, mode: "insensitive" } },
      { invoice: { is: { code: { contains: q, mode: "insensitive" } } } },
      { invoice: { is: { partnerName: { contains: q, mode: "insensitive" } } } },
      {
        lines: {
          some: {
            OR: [
              { item: { name: { contains: q, mode: "insensitive" } } },
              { item: { sku: { contains: q, mode: "insensitive" } } },
            ],
          },
        },
      },
    ];
  }

  const movements = await prisma.movement.findMany({
    where: whereMv,
    orderBy: [{ occurredAt: "desc" }, { createdAt: "desc" }],
    include: {
      invoice: { select: { id: true, code: true, type: true, note: true, partnerName: true } },
      lines: {
        where: params.itemId ? { itemId: params.itemId } : undefined,
        include: { item: { select: { id: true, sku: true, name: true } } },
      },
    },
  });

  const rows: LedgerRow[] = [];
  for (const mv of movements as any[]) {
    for (const ln of mv.lines as any[]) {
      let qty = toNum(ln.qty);

      if (mv.type === "OUT") qty = -Math.abs(qty);
      if (mv.type === "IN") qty = Math.abs(qty);

      const note =
        (mv.invoice?.note && String(mv.invoice.note).trim()) ||
        (mv.note && String(mv.note).trim()) ||
        (ln.note && String(ln.note).trim()) ||
        null;

      // ✅ thời gian hiển thị: occurredAt
      const at = (mv.occurredAt ?? mv.createdAt) as Date;

      rows.push({
        at: new Date(at).toISOString(),
        movementId: mv.id,
        movementType: mv.type,
        invoiceId: mv.invoice?.id ?? null,
        invoiceCode: mv.invoice?.code ?? mv.refNo ?? null,
        invoiceType: (mv.invoice?.type as any) ?? null,

        itemId: ln.itemId,
        itemSku: ln.item?.sku ?? null,
        itemName: ln.item?.name ?? null,

        qty,
        unitCost: ln.unitCost != null ? toNum(ln.unitCost as any) : null,
        costTotal: ln.costTotal != null ? toNum(ln.costTotal as any) : null,
        note,
      });
    }
  }

  const totalIn = rows.reduce((s, r) => (r.qty > 0 ? s + r.qty : s), 0);
  const totalOut = rows.reduce((s, r) => (r.qty < 0 ? s + Math.abs(r.qty) : s), 0);

  return {
    rows,
    summary: {
      totalIn,
      totalOut,
      count: rows.length,
    },
  };
}

/** ✅ Export Excel đẹp bằng ExcelJS */
export async function exportLedgerExcel(params: {
  from?: Date;
  to?: Date;
  q?: string;
  itemId?: string;
  type?: MovementType;
}) {
  const { rows, summary } = await getLedger(params);

  const wb = new ExcelJS.Workbook();
  wb.creator = "Warehouse App";
  wb.created = new Date();

  const ws = wb.addWorksheet("Lịch sử xuất nhập", {
    views: [{ state: "frozen", ySplit: 2 }],
  });

  ws.mergeCells("A1:I1");
  ws.getCell("A1").value = "LỊCH SỬ XUẤT NHẬP KHO";
  ws.getCell("A1").font = { size: 14, bold: true };
  ws.getCell("A1").alignment = { vertical: "middle", horizontal: "center" };
  ws.getRow(1).height = 22;

  const header = [
    "Thời gian",
    "Chứng từ",
    "Loại",
    "Mã hàng",
    "Tên hàng",
    "SL (+/-)",
    "Giá vốn",
    "Thành tiền vốn",
    "Ghi chú",
  ];
  ws.addRow(header);

  const headerRow = ws.getRow(2);
  headerRow.font = { bold: true, color: { argb: "FFFFFFFF" } };
  headerRow.alignment = { vertical: "middle", horizontal: "center", wrapText: true };
  headerRow.height = 18;
  headerRow.eachCell((c) => {
    c.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FF1F2937" } };
    c.border = {
      top: { style: "thin", color: { argb: "FFCBD5E1" } },
      left: { style: "thin", color: { argb: "FFCBD5E1" } },
      bottom: { style: "thin", color: { argb: "FFCBD5E1" } },
      right: { style: "thin", color: { argb: "FFCBD5E1" } },
    };
  });

  const typeLabel = (t: MovementType) => {
    if (t === "IN") return "Nhập kho";
    if (t === "OUT") return "Xuất kho";
    if (t === "ADJUST") return "Điều chỉnh";
    return "Chuyển kho";
  };

  for (const r of rows) {
    const dt = new Date(r.at);
    ws.addRow([
      dt,
      r.invoiceCode ?? "",
      typeLabel(r.movementType),
      r.itemSku ?? "",
      r.itemName ?? "",
      r.qty,
      r.unitCost ?? null,
      r.costTotal ?? null,
      r.note ?? "",
    ]);
  }

  ws.getColumn(1).numFmt = "dd/mm/yyyy hh:mm";
  ws.getColumn(6).numFmt = "#,##0.###";
  ws.getColumn(7).numFmt = "#,##0";
  ws.getColumn(8).numFmt = "#,##0";

  ws.getColumn(1).alignment = { vertical: "middle", horizontal: "left" };
  ws.getColumn(2).alignment = { vertical: "middle", horizontal: "left" };
  ws.getColumn(3).alignment = { vertical: "middle", horizontal: "center" };
  ws.getColumn(4).alignment = { vertical: "middle", horizontal: "left" };
  ws.getColumn(5).alignment = { vertical: "middle", horizontal: "left", wrapText: true };
  ws.getColumn(6).alignment = { vertical: "middle", horizontal: "right" };
  ws.getColumn(7).alignment = { vertical: "middle", horizontal: "right" };
  ws.getColumn(8).alignment = { vertical: "middle", horizontal: "right" };
  ws.getColumn(9).alignment = { vertical: "middle", horizontal: "left", wrapText: true };

  for (let i = 3; i <= ws.rowCount; i++) {
    ws.getRow(i).eachCell((c) => {
      c.border = {
        top: { style: "thin", color: { argb: "FFE5E7EB" } },
        left: { style: "thin", color: { argb: "FFE5E7EB" } },
        bottom: { style: "thin", color: { argb: "FFE5E7EB" } },
        right: { style: "thin", color: { argb: "FFE5E7EB" } },
      };
    });
  }

  const summaryRowIndex = ws.rowCount + 2;
  ws.getCell(`A${summaryRowIndex}`).value = "Tổng nhập:";
  ws.getCell(`B${summaryRowIndex}`).value = summary.totalIn;
  ws.getCell(`D${summaryRowIndex}`).value = "Tổng xuất:";
  ws.getCell(`E${summaryRowIndex}`).value = summary.totalOut;

  ws.getCell(`A${summaryRowIndex}`).font = { bold: true };
  ws.getCell(`D${summaryRowIndex}`).font = { bold: true };
  ws.getCell(`B${summaryRowIndex}`).numFmt = "#,##0.###";
  ws.getCell(`E${summaryRowIndex}`).numFmt = "#,##0.###";

  const widths = [18, 16, 12, 14, 30, 12, 14, 16, 36];
  widths.forEach((w, idx) => (ws.getColumn(idx + 1).width = w));

  const buffer = await wb.xlsx.writeBuffer();
  return Buffer.from(buffer);
}

/** ========================= Sales Ledger (Bảng kê bán hàng) ========================= **/

export type SalesLedgerRow = {
  invoiceId: string;

  issueDate: string; // yyyy-mm-dd
  code: string;
  partnerName: string;

  itemId: string;

  itemName: string;
  itemSku?: string | null;

  qty: number;
  unitPrice: number;
  unitCost: number;
  costTotal: number;

  unitCostMonthAvg: number;
  costTotalMonthAvg: number;

  lineAmount: number;

  paid: number;
  debt: number;

  saleUserName: string;
  techUserName: string;
};

type ReturnAggByItem = { qty: number; amount: number };

async function loadSalesReturnAgg(params: { invoiceIds: string[]; asOf?: Date }) {
  const { invoiceIds, asOf } = params;
  if (!invoiceIds.length) return new Map<string, Map<string, ReturnAggByItem>>();

  const where: Prisma.InvoiceWhereInput = {
    status: "APPROVED",
    type: "SALES_RETURN",
    refInvoiceId: { in: invoiceIds },
  };

  if (asOf) {
    where.issueDate = { lte: asOf };
  }

  const returns = await prisma.invoice.findMany({
    where,
    select: {
      id: true,
      refInvoiceId: true,
      lines: { select: { itemId: true, qty: true, price: true } },
    },
  });

  const retMap = new Map<string, Map<string, ReturnAggByItem>>();

  for (const r of returns as any[]) {
    const sid = String(r.refInvoiceId || "");
    if (!sid) continue;

    let itemMap = retMap.get(sid);
    if (!itemMap) {
      itemMap = new Map<string, ReturnAggByItem>();
      retMap.set(sid, itemMap);
    }

    const ls: any[] = Array.isArray(r.lines) ? r.lines : [];
    for (const l of ls) {
      const itemId = String(l.itemId || "");
      if (!itemId) continue;

      const qty = toNum(l.qty);
      const price = toNum(l.price);
      const amt = round2(qty * price);

      const cur = itemMap.get(itemId) || { qty: 0, amount: 0 };
      cur.qty = round2(cur.qty + qty);
      cur.amount = round2(cur.amount + amt);
      itemMap.set(itemId, cur);
    }
  }

  return retMap;
}

async function attachMonthlyAvgCostToSalesRows(rows: SalesLedgerRow[], opts?: { asOf?: Date }) {
  if (!rows.length) return;

  const itemIds = Array.from(new Set(rows.map((r) => r.itemId).filter(Boolean)));

  let locationId: string | null = null;
  const stockLoc = await prisma.stock.findFirst({
    where: { itemId: { in: itemIds } },
    select: { locationId: true },
  });
  locationId = (stockLoc?.locationId as any) ?? null;

  if (!locationId) {
    const defaultLoc = await prisma.location.findFirst({ select: { id: true } });
    locationId = defaultLoc?.id ?? null;
  }

  if (!locationId) {
    for (const r of rows) {
      r.unitCostMonthAvg = r.unitCost || 0;
      r.costTotalMonthAvg = round2((r.qty || 0) * (r.unitCostMonthAvg || 0));
    }
    return;
  }

  const ymSet = new Set<string>();
  const yms: Array<{ year: number; month: number }> = [];
  for (const r of rows) {
    const { year, month } = parseYMD(r.issueDate);
    const k = `${year}-${month}`;
    if (ymSet.has(k)) continue;
    ymSet.add(k);
    yms.push({ year, month });
  }

  let asOfYear: number | null = null;
  let asOfMonth: number | null = null;
  if (opts?.asOf instanceof Date && !Number.isNaN(opts.asOf.getTime())) {
    asOfYear = opts.asOf.getFullYear();
    asOfMonth = opts.asOf.getMonth() + 1;
  }

  const map = new Map<string, number>(); // key: year-month-itemId -> avg

  for (const ym of yms) {
    let found = await prisma.monthlyAvgCost.findMany({
      where: {
        year: ym.year,
        month: ym.month,
        locationId,
        itemId: { in: itemIds },
      },
      select: { year: true, month: true, itemId: true, avgCost: true },
    });

    if (found.length === 0) {
      found = await prisma.monthlyAvgCost.findMany({
        where: {
          year: ym.year,
          month: ym.month,
          itemId: { in: itemIds },
        },
        select: { year: true, month: true, itemId: true, avgCost: true },
      });
    }

    for (const x of found) {
      const kk = `${x.year}-${x.month}-${x.itemId}`;
      if (!map.has(kk)) map.set(kk, toNum(x.avgCost));
    }
  }

  if (asOfYear && asOfMonth) {
    const missingItemIds = new Set<string>();
    for (const r of rows) {
      const { year, month } = parseYMD(r.issueDate);
      if (year !== asOfYear || month !== asOfMonth) continue;
      const kk = `${year}-${month}-${r.itemId}`;
      if (!map.has(kk)) missingItemIds.add(r.itemId);
    }

    if (missingItemIds.size > 0) {
      const stocks = await prisma.stock.findMany({
        where: {
          locationId,
          itemId: { in: Array.from(missingItemIds) },
        },
        select: { itemId: true, avgCost: true },
      });

      for (const s of stocks) {
        const kk = `${asOfYear}-${asOfMonth}-${s.itemId}`;
        map.set(kk, toNum(s.avgCost));
      }
    }
  }

  for (const r of rows) {
    const { year, month } = parseYMD(r.issueDate);
    const kk = `${year}-${month}-${r.itemId}`;
    const monthAvg = map.get(kk);

    r.unitCostMonthAvg = monthAvg ?? r.unitCost ?? 0;
    r.costTotalMonthAvg = round2((r.qty || 0) * (r.unitCostMonthAvg || 0));
  }
}

export async function getSalesLedger(params: {
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
    },
  });

  const invoiceIds = (invoices as any[]).map((x) => String(x.id));
  const asOf = params.to ? params.to : undefined;
  const returnAgg = await loadSalesReturnAgg({ invoiceIds, asOf });

  const rows: SalesLedgerRow[] = [];

  for (const inv of invoices as any[]) {
    const invId = String(inv.id);
    const invPaid = toNum(inv.paidAmount);

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

    const issueDateStr = toLocalYMD(new Date(inv.issueDate));
    const partnerName = String(inv.partnerName || "");

    const linesArr: any[] = Array.isArray(inv.lines) ? inv.lines : [];
    if (linesArr.length === 0) continue;

    const retItemMap = returnAgg.get(invId);

    const netLines = linesArr.map((l: any) => {
      const itemId = String(l.itemId || "");
      const qty = toNum(l.qty);
      const unitPrice = toNum(l.price);
      const lineAmount = toNum(l.amount);

      const unitCost = toNum(l.unitCost);
      const lineCostTotal = toNum(l.costTotal);

      const ret = retItemMap?.get(itemId);
      const retQty = ret ? toNum(ret.qty) : 0;
      const retAmt = ret ? toNum(ret.amount) : 0;

      const netQty = Math.max(0, round2(qty - retQty));
      const netAmount = Math.max(0, round2(lineAmount - retAmt));

      let useUnitCost = unitCost;
      if (!useUnitCost && qty > 0 && lineCostTotal > 0) useUnitCost = round2(lineCostTotal / qty);

      const netCostTotal = round2(Math.max(0, useUnitCost * netQty));

      return {
        itemId,
        itemName: String(l.itemName || ""),
        itemSku: l.itemSku ?? null,
        netQty,
        unitPrice,
        unitCost: useUnitCost,
        costTotal: netCostTotal,
        lineAmount: netAmount,
      };
    });

    const netBase = round2(netLines.reduce((s: number, x: any) => s + toNum(x.lineAmount), 0));
    if (netBase <= 0.0001) continue;

    const paidBase = Math.min(invPaid, netBase > 0 ? netBase : invPaid);

    let paidAllocatedSum = 0;

    for (let i = 0; i < netLines.length; i++) {
      const l = netLines[i];
      if (toNum(l.lineAmount) <= 0.0001) continue;

      let paidLine = 0;
      if (netBase > 0 && paidBase > 0 && toNum(l.lineAmount) > 0) {
        paidLine = round2((paidBase * toNum(l.lineAmount)) / netBase);
      }

      if (i === netLines.length - 1) {
        const remain = round2(paidBase - paidAllocatedSum);
        paidLine = Math.max(0, Math.min(toNum(l.lineAmount), remain));
      }

      paidAllocatedSum = round2(paidAllocatedSum + paidLine);
      const debt = round2(Math.max(0, toNum(l.lineAmount) - paidLine));

      rows.push({
        invoiceId: invId,
        issueDate: issueDateStr,
        code: String(inv.code),
        partnerName,

        itemId: String(l.itemId),
        itemName: String(l.itemName || ""),
        itemSku: l.itemSku ?? null,

        qty: toNum(l.netQty),
        unitPrice: toNum(l.unitPrice),
        unitCost: toNum(l.unitCost),
        costTotal: toNum(l.costTotal),

        unitCostMonthAvg: 0,
        costTotalMonthAvg: 0,

        lineAmount: toNum(l.lineAmount),
        paid: paidLine,
        debt,

        saleUserName: saleName,
        techUserName: techName,
      });
    }
  }

  await attachMonthlyAvgCostToSalesRows(rows, { asOf });

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

export async function exportSalesLedgerExcel(params: {
  from?: Date;
  to?: Date;
  q?: string;
  saleUserId?: string;
  techUserId?: string;
  paymentStatus?: PaymentStatus;
}) {
  const { rows, totals } = await getSalesLedger(params);

  const wb = new ExcelJS.Workbook();
  wb.creator = "Warehouse App";
  wb.created = new Date();

  const ws = wb.addWorksheet("Bảng kê bán", {
    views: [{ state: "frozen", ySplit: 2 }],
  });

  ws.mergeCells("A1:M1");
  ws.getCell("A1").value = "BẢNG KÊ BÁN HÀNG";
  ws.getCell("A1").font = { size: 14, bold: true };
  ws.getCell("A1").alignment = { vertical: "middle", horizontal: "center" };
  ws.getRow(1).height = 22;

  const header = [
    "Ngày",
    "Số chứng từ",
    "Tên khách hàng",
    "Tên sản phẩm",
    "Đơn giá",
    "Đơn giá vốn",
    "Giá vốn TB (kỳ)",
    "Tiền vốn",
    "Thành tiền",
    "Đã thanh toán",
    "Còn nợ",
    "NV sale",
    "Kĩ thuật",
  ];
  ws.addRow(header);

  const headerRow = ws.getRow(2);
  headerRow.font = { bold: true, color: { argb: "FFFFFFFF" } };
  headerRow.alignment = { vertical: "middle", horizontal: "center", wrapText: true };
  headerRow.height = 18;
  headerRow.eachCell((c) => {
    c.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FF1F2937" } };
    c.border = {
      top: { style: "thin", color: { argb: "FFCBD5E1" } },
      left: { style: "thin", color: { argb: "FFCBD5E1" } },
      bottom: { style: "thin", color: { argb: "FFCBD5E1" } },
      right: { style: "thin", color: { argb: "FFCBD5E1" } },
    };
  });

  for (const r of rows) {
    const d = new Date(r.issueDate + "T00:00:00");
    ws.addRow([
      d,
      r.code,
      r.partnerName,
      r.itemName,
      r.unitPrice,
      r.unitCost,
      r.unitCostMonthAvg,
      r.costTotal,
      r.lineAmount,
      r.paid,
      r.debt,
      r.saleUserName,
      r.techUserName,
    ]);
  }

  ws.getColumn(1).numFmt = "dd/mm/yyyy";
  [5, 6, 7, 8, 9, 10, 11].forEach((col) => (ws.getColumn(col).numFmt = "#,##0"));

  ws.getColumn(1).alignment = { vertical: "middle", horizontal: "left" };
  ws.getColumn(2).alignment = { vertical: "middle", horizontal: "left" };
  ws.getColumn(3).alignment = { vertical: "middle", horizontal: "left", wrapText: true };
  ws.getColumn(4).alignment = { vertical: "middle", horizontal: "left", wrapText: true };
  for (let c = 5; c <= 11; c++) {
    ws.getColumn(c).alignment = { vertical: "middle", horizontal: "right" };
  }
  ws.getColumn(12).alignment = { vertical: "middle", horizontal: "left" };
  ws.getColumn(13).alignment = { vertical: "middle", horizontal: "left" };

  for (let i = 3; i <= ws.rowCount; i++) {
    ws.getRow(i).eachCell((c) => {
      c.border = {
        top: { style: "thin", color: { argb: "FFE5E7EB" } },
        left: { style: "thin", color: { argb: "FFE5E7EB" } },
        bottom: { style: "thin", color: { argb: "FFE5E7EB" } },
        right: { style: "thin", color: { argb: "FFE5E7EB" } },
      };
    });
  }

  const summaryRowIndex = ws.rowCount + 2;
  ws.getCell(`A${summaryRowIndex}`).value = "Tổng doanh thu:";
  ws.getCell(`B${summaryRowIndex}`).value = totals.totalRevenue;

  ws.getCell(`D${summaryRowIndex}`).value = "Tổng vốn:";
  ws.getCell(`E${summaryRowIndex}`).value = totals.totalCost;

  ws.getCell(`G${summaryRowIndex}`).value = "Tổng đã thu:";
  ws.getCell(`H${summaryRowIndex}`).value = totals.totalPaid;

  ws.getCell(`J${summaryRowIndex}`).value = "Tổng còn nợ:";
  ws.getCell(`K${summaryRowIndex}`).value = totals.totalDebt;

  ["A", "D", "G", "J"].forEach((col) => (ws.getCell(`${col}${summaryRowIndex}`).font = { bold: true }));
  ["B", "E", "H", "K"].forEach((col) => (ws.getCell(`${col}${summaryRowIndex}`).numFmt = "#,##0"));

  const widths = [12, 18, 26, 28, 12, 12, 14, 14, 14, 14, 14, 18, 18];
  widths.forEach((w, idx) => (ws.getColumn(idx + 1).width = w));

  const buffer = await wb.xlsx.writeBuffer();
  return Buffer.from(buffer);
}
