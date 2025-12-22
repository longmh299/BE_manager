// src/routes/stocks.routes.ts
import { Router } from "express";
import { requireAuth } from "../middlewares/auth";
import { getStocks, getStockSummaryByItem } from "../services/stocks.service";
import * as XLSX from "xlsx";

const r = Router();
r.use(requireAuth);

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

    res.json({ ok: true, data: rows, total });
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

    const excelRows = rows.map((s: any) => ({
      sku: s.item?.sku ?? "",
      name: s.item?.name ?? "",
      unit: s.item?.unit?.code ?? "pcs",
      location: s.location?.code ?? "",
      qty: (s.qty as any)?.toString?.() ?? "0",
      avgCost: (s.avgCost as any)?.toString?.() ?? "0",
    }));

    const ws = XLSX.utils.json_to_sheet(excelRows);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Stocks");
    const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

    res.setHeader("Content-Disposition", 'attachment; filename="stocks.xlsx"');
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.send(buf);
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

    res.json({ ok: true, data: rows, total });
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

    const exportRows = rows.map((r: any) => ({
      sku: r.sku ?? "",
      name: r.name ?? "",
      unit: r.unit ?? "",
      kind: r.kind ?? "",
      sellPrice: r.sellPrice == null ? "" : (r.sellPrice as any)?.toString?.() ?? "",
      totalQty: (r.totalQty as any)?.toString?.() ?? "0",
      avgCost: r.avgCost == null ? "" : String(r.avgCost),
      stockValue: r.stockValue == null ? "" : String(r.stockValue),
    }));

    const ws = XLSX.utils.json_to_sheet(exportRows);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "SummaryByItem");
    const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

    res.setHeader(
      "Content-Disposition",
      'attachment; filename="stocks_summary_by_item.xlsx"'
    );
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.send(buf);
  } catch (e) {
    next(e);
  }
});

export default r;
