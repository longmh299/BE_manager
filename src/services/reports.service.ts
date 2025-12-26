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

  if (params.from || params.to) {
    whereMv.createdAt = {};
    if (params.from) (whereMv.createdAt as any).gte = params.from;
    if (params.to) (whereMv.createdAt as any).lte = params.to;
  }

  if (params.type) whereMv.type = params.type;

  if (params.q && params.q.trim()) {
    const q = params.q.trim();

    // ✅ Search theo:
    // - refNo (movement)
    // - invoice.code, invoice.partnerName
    // - item.name / item.sku (movement lines)
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
    orderBy: { createdAt: "desc" },
    include: {
      // ✅ include note để dùng hiển thị
      invoice: { select: { id: true, code: true, type: true, note: true, partnerName: true } },
      lines: {
        where: params.itemId ? { itemId: params.itemId } : undefined,
        include: { item: { select: { id: true, sku: true, name: true } } },
      },
    },
  });

  const rows: LedgerRow[] = [];
  for (const mv of movements) {
    for (const ln of mv.lines) {
      let qty = toNum(ln.qty);

      // 1 kho: chuẩn hóa sign
      if (mv.type === "OUT") qty = -Math.abs(qty);
      if (mv.type === "IN") qty = Math.abs(qty);
      // ADJUST: giữ nguyên qty theo line

      // ✅ NOTE: ưu tiên invoice.note (đúng nghiệp vụ), rồi movement.note, rồi line.note
      const note =
        (mv.invoice?.note && String(mv.invoice.note).trim()) ||
        (mv.note && String(mv.note).trim()) ||
        (ln.note && String(ln.note).trim()) ||
        null;

      rows.push({
        at: mv.createdAt.toISOString(),
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

  // Title row
  ws.mergeCells("A1:I1");
  ws.getCell("A1").value = "LỊCH SỬ XUẤT NHẬP KHO";
  ws.getCell("A1").font = { size: 14, bold: true };
  ws.getCell("A1").alignment = { vertical: "middle", horizontal: "center" };
  ws.getRow(1).height = 22;

  // Header row
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
  ws.getColumn(7).numFmt = "#,##0.00";
  ws.getColumn(8).numFmt = "#,##0.00";

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
  issueDate: string; // yyyy-mm-dd
  code: string;
  partnerName: string;

  itemName: string;
  itemSku?: string | null;

  qty: number;
  unitPrice: number;  // đơn giá
  unitCost: number;   // đơn giá vốn
  costTotal: number;  // tiền vốn (đã net theo trả hàng)

  lineAmount: number; // thành tiền (đã net theo trả hàng)

  paid: number;       // đã thanh toán (phân bổ theo dòng - theo NET sau trả hàng)
  debt: number;       // còn nợ (theo NET sau trả hàng)

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

  // as-of: để report “tới ngày” thì trả hàng sau ngày đó không tính vào
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

  // Map: salesInvoiceId -> (itemId -> {qty, amount})
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

  // include true để khỏi kẹt typing + chắc chắn có inv.lines
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

    const issueDateStr = new Date(inv.issueDate).toISOString().slice(0, 10);
    const partnerName = String(inv.partnerName || "");

    const linesArr: any[] = Array.isArray(inv.lines) ? inv.lines : [];
    if (linesArr.length === 0) continue;

    const retItemMap = returnAgg.get(invId); // may be undefined

    // 1) Tính NET line theo trả hàng
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

      // qty net
      const netQty = Math.max(0, round2(qty - retQty));

      // amount net: ưu tiên trừ theo amount trả hàng (qty*price trên phiếu return)
      const netAmount = Math.max(0, round2(lineAmount - retAmt));

      // cost net: giảm theo qty net (unitCost * netQty)
      // nếu thiếu unitCost thì fallback theo tỉ lệ lineCostTotal/qty
      let useUnitCost = unitCost;
      if (!useUnitCost && qty > 0 && lineCostTotal > 0) useUnitCost = round2(lineCostTotal / qty);

      const netCostTotal = round2(Math.max(0, useUnitCost * netQty));

      return {
        itemId,
        itemName: String(l.itemName || ""),
        itemSku: l.itemSku ?? null,
        qty,
        unitPrice,
        unitCost: useUnitCost,
        costTotal: netCostTotal,
        lineAmount: netAmount,
      };
    });

    const netBase = round2(netLines.reduce((s: number, x: any) => s + toNum(x.lineAmount), 0));

    // ✅ FULL RETURN => bỏ khỏi bảng kê (không show “còn nợ” nữa)
    if (netBase <= 0.0001) {
      continue;
    }

    // 2) paidBase phân bổ theo NET base để không bị “paidLine > lineAmount”
    const paidBase = Math.min(invPaid, netBase > 0 ? netBase : invPaid);

    // 3) phân bổ paid xuống line theo NET lineAmount
    let paidAllocatedSum = 0;

    for (let i = 0; i < netLines.length; i++) {
      const l = netLines[i];

      // bỏ line đã trả hết (net = 0)
      if (toNum(l.lineAmount) <= 0.0001) continue;

      let paidLine = 0;
      if (netBase > 0 && paidBase > 0 && toNum(l.lineAmount) > 0) {
        paidLine = round2((paidBase * toNum(l.lineAmount)) / netBase);
      }

      // dòng cuối: chỉnh để tổng paidLine = paidBase (tránh lệch do làm tròn)
      if (i === netLines.length - 1) {
        const remain = round2(paidBase - paidAllocatedSum);
        paidLine = Math.max(0, Math.min(toNum(l.lineAmount), remain));
      }

      paidAllocatedSum = round2(paidAllocatedSum + paidLine);
      const debt = round2(Math.max(0, toNum(l.lineAmount) - paidLine));

      rows.push({
        issueDate: issueDateStr,
        code: String(inv.code),
        partnerName,

        itemName: String(l.itemName || ""),
        itemSku: l.itemSku ?? null,

        qty: toNum(l.qty), // qty gốc (đang hiển thị giống hệ thống hiện tại)
        unitPrice: toNum(l.unitPrice),
        unitCost: toNum(l.unitCost),
        costTotal: toNum(l.costTotal),

        lineAmount: toNum(l.lineAmount),
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

  // Title row
  ws.mergeCells("A1:L1");
  ws.getCell("A1").value = "BẢNG KÊ BÁN HÀNG";
  ws.getCell("A1").font = { size: 14, bold: true };
  ws.getCell("A1").alignment = { vertical: "middle", horizontal: "center" };
  ws.getRow(1).height = 22;

  // Header row (12 cột đúng yêu cầu)
  const header = [
    "Ngày",
    "Số chứng từ",
    "Tên khách hàng",
    "Tên sản phẩm",
    "Đơn giá",
    "Đơn giá vốn",
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
    // Ngày: để Date object để Excel format đẹp
    const d = new Date(r.issueDate + "T00:00:00.000Z");

    ws.addRow([
      d,
      r.code,
      r.partnerName,
      r.itemName,
      r.unitPrice,
      r.unitCost,
      r.costTotal,
      r.lineAmount,
      r.paid,
      r.debt,
      r.saleUserName,
      r.techUserName,
    ]);
  }

  // Format / alignment
  ws.getColumn(1).numFmt = "dd/mm/yyyy";
  ws.getColumn(5).numFmt = "#,##0.00";
  ws.getColumn(6).numFmt = "#,##0.00";
  ws.getColumn(7).numFmt = "#,##0.00";
  ws.getColumn(8).numFmt = "#,##0.00";
  ws.getColumn(9).numFmt = "#,##0.00";
  ws.getColumn(10).numFmt = "#,##0.00";

  ws.getColumn(1).alignment = { vertical: "middle", horizontal: "left" };
  ws.getColumn(2).alignment = { vertical: "middle", horizontal: "left" };
  ws.getColumn(3).alignment = { vertical: "middle", horizontal: "left", wrapText: true };
  ws.getColumn(4).alignment = { vertical: "middle", horizontal: "left", wrapText: true };
  ws.getColumn(5).alignment = { vertical: "middle", horizontal: "right" };
  ws.getColumn(6).alignment = { vertical: "middle", horizontal: "right" };
  ws.getColumn(7).alignment = { vertical: "middle", horizontal: "right" };
  ws.getColumn(8).alignment = { vertical: "middle", horizontal: "right" };
  ws.getColumn(9).alignment = { vertical: "middle", horizontal: "right" };
  ws.getColumn(10).alignment = { vertical: "middle", horizontal: "right" };
  ws.getColumn(11).alignment = { vertical: "middle", horizontal: "left" };
  ws.getColumn(12).alignment = { vertical: "middle", horizontal: "left" };

  // Borders data rows
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

  // Summary
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
  ["B", "E", "H", "K"].forEach((col) => (ws.getCell(`${col}${summaryRowIndex}`).numFmt = "#,##0.00"));

  const widths = [12, 18, 26, 28, 12, 12, 14, 14, 14, 14, 18, 18];
  widths.forEach((w, idx) => (ws.getColumn(idx + 1).width = w));

  const buffer = await wb.xlsx.writeBuffer();
  return Buffer.from(buffer);
}
