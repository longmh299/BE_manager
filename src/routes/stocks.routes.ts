// src/routes/stocks.routes.ts
import { Router } from "express";
import { requireAuth, getUser } from "../middlewares/auth";
import { getStocks, getStockSummaryByItem } from "../services/stocks.service";
import * as XLSX from "xlsx";
import { buildReport, sendXlsx, exportSubtitle, stampName, KIND_VN, type ReportCol } from "../lib/excelReport"; // ✅ khuôn Excel dùng chung

const r = Router();
r.use(requireAuth);

// ✅ bảo mật: giá vốn / giá nhập / giá trị tồn CHỈ admin + kế toán thấy.
// Trước đây giao diện chỉ ẨN cột với nhân viên, nhưng API vẫn trả về -> bấm F12 (tab Network) là thấy.
const canSeeCost = (req: any) => ["admin", "accountant"].includes(String(getUser(req)?.role || ""));
const COST_KEYS = ["avgCost", "stockValue", "totalValue", "price", "monthlyAvgCosts"];
function hideCost<T>(row: T): T {
  if (!row || typeof row !== "object") return row;
  const o: any = { ...(row as any) };
  for (const k of COST_KEYS) delete o[k];
  if (o.item && typeof o.item === "object") {
    o.item = { ...o.item };
    for (const k of COST_KEYS) delete o.item[k];
  }
  return o;
}

// ✅ tránh 304 cache -> đôi khi browser báo CORS error dù BE vẫn chạy
r.use((req, res, next) => {
  res.setHeader("Cache-Control", "no-store");
  next();
});

/**
 * GET /stocks?q=&itemId=&locationId=&kind=&page=&pageSize=
 */
r.get("/", async (req, res, next) => {
  try {
    const { itemId, locationId, q, kind, page, pageSize } = req.query as any;

    const pageNum = Number(page) || 1;
    const sizeNum = Number(pageSize) || 500;

    const { rows, total } = await getStocks({
      itemId: itemId || undefined,
      locationId: locationId || undefined,
      q: q || undefined,
      kind: (kind as any) || undefined,
      page: pageNum,
      pageSize: sizeNum,
    });

    res.json({ ok: true, data: canSeeCost(req) ? rows : rows.map(hideCost), total });
  } catch (e) {
    next(e);
  }
});

/**
 * GET /stocks/export?q=&itemId=&locationId=&kind=
 */
r.get("/export", async (req, res, next) => {
  try {
    const { itemId, locationId, q, kind } = req.query as any;

    const { rows } = await getStocks({
      itemId: itemId || undefined,
      locationId: locationId || undefined,
      q: q || undefined,
      kind: (kind as any) || undefined,
      page: 1,
      pageSize: 10_000,
    });

    // ✅ cùng dữ liệu như trước, chỉ trình bày lại (tên cột tiếng Việt, số định dạng, dòng tổng)
    const cost = canSeeCost(req);
    const excelRows = rows.map((s: any) => ({
      sku: s.item?.sku ?? "",
      name: s.item?.name ?? "",
      unit: s.item?.unit?.name || s.item?.unit?.code || "Cái",
      location: s.location?.name || s.location?.code || "",
      qty: s.qty,
      avgCost: s.avgCost,
      value: Number(s.qty || 0) * Number(s.avgCost || 0),
    }));
    const cols: ReportCol[] = [
      { header: "Mã hàng", key: "sku", width: 14 },
      { header: "Tên hàng", key: "name", width: 46, wrap: true },
      { header: "ĐVT", key: "unit", width: 8, type: "center" },
      { header: "Kho", key: "location", width: 16 },
      { header: "Tồn", key: "qty", width: 10, type: "qty", total: true, redIfZero: true },
      ...(cost
        ? ([
            { header: "Giá vốn TB", key: "avgCost", width: 15, type: "money" },
            { header: "Giá trị tồn", key: "value", width: 17, type: "money", total: true },
          ] as ReportCol[])
        : []),
    ];
    const buf = await buildReport({
      sheetName: "Tồn kho",
      title: "BÁO CÁO TỒN KHO THEO KHO",
      subtitle: exportSubtitle(getUser(req), q ? `Tìm: "${q}"` : "", `${rows.length} dòng`),
      columns: cols,
      rows: excelRows,
      stt: true,
      totals: true,
    });
    sendXlsx(res, buf, stampName("ton-kho-theo-kho"));
  } catch (e) {
    next(e);
  }
});

/**
 * GET /stocks/summary-by-item?q=&kind=&page=&pageSize=
 */
r.get("/summary-by-item", async (req, res, next) => {
  try {
    const { q, kind, page, pageSize } = req.query as any;

    const pageNum = Number(page) || 1;
    const sizeNum = Number(pageSize) || 50;

    const { rows, total } = await getStockSummaryByItem({
      q: q || undefined,
      kind: (kind as any) || undefined,
      page: pageNum,
      pageSize: sizeNum,
    });

    res.json({ ok: true, data: canSeeCost(req) ? rows : rows.map(hideCost), total });
  } catch (e) {
    next(e);
  }
});

/**
 * GET /stocks/summary-by-item/export?q=&kind=
 */
r.get("/summary-by-item/export", async (req, res, next) => {
  try {
    const { q, kind } = req.query as any;

    const { rows } = await getStockSummaryByItem({
      q: q || undefined,
      kind: (kind as any) || undefined,
      page: 1,
      pageSize: 10_000,
    });

    // ✅ cùng dữ liệu như trước, chỉ trình bày lại
    const cost = canSeeCost(req);
    const exportRows = rows.map((r: any) => ({
      sku: r.sku ?? "",
      name: r.name ?? "",
      unit: r.unitName || r.unit || "",
      kind: KIND_VN[r.kind] ?? r.kind ?? "",
      sellPrice: r.sellPrice,
      totalQty: r.totalQty,
      avgCost: r.avgCost,
      stockValue: r.stockValue,
      note: r.note ?? "",
    }));
    const cols: ReportCol[] = [
      { header: "Mã hàng", key: "sku", width: 14 },
      { header: "Tên hàng", key: "name", width: 46, wrap: true },
      { header: "ĐVT", key: "unit", width: 8, type: "center" },
      { header: "Loại", key: "kind", width: 11, type: "center" },
      { header: "Giá bán", key: "sellPrice", width: 15, type: "money" },
      { header: "Tồn", key: "totalQty", width: 10, type: "qty", total: true, redIfZero: true },
      ...(cost
        ? ([
            { header: "Giá vốn TB", key: "avgCost", width: 15, type: "money" },
            { header: "Giá trị tồn", key: "stockValue", width: 17, type: "money", total: true },
          ] as ReportCol[])
        : []),
      { header: "Ghi chú", key: "note", width: 24, wrap: true },
    ];
    const what = kind === "MACHINE" ? "MÁY" : kind === "PART" ? "PHỤ TÙNG" : "HÀNG HOÁ";
    const buf = await buildReport({
      sheetName: kind === "MACHINE" ? "Tồn máy" : kind === "PART" ? "Tồn phụ tùng" : "Tồn kho",
      title: `BÁO CÁO TỒN KHO ${what}`,
      subtitle: exportSubtitle(getUser(req), q ? `Tìm: "${q}"` : "", `${rows.length} mặt hàng`),
      columns: cols,
      rows: exportRows,
      stt: true,
      totals: true,
    });
    sendXlsx(res, buf, stampName(kind === "MACHINE" ? "ton-kho-may" : kind === "PART" ? "ton-kho-phu-tung" : "ton-kho"));
  } catch (e) {
    next(e);
  }
});

export default r;
