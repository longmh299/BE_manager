// src/routes/stocks.routes.ts
import { Router } from "express";
import { requireAuth } from "../middlewares/auth";
import { getStocks, getStockSummaryByItem } from "../services/stocks.service";
import * as XLSX from "xlsx";

const r = Router();
r.use(requireAuth);

/**
 * GET /stocks?q=&itemId=&locationId=&kind=&page=&pageSize=
 * Trả về danh sách tồn chi tiết theo kho (kèm item, location)
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
 * Xuất danh sách tồn (chi tiết theo kho) ra Excel
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
      unit: s.item?.unit ?? "",
      location: s.location?.code ?? "",
      qty: (s.qty as any)?.toString?.() ?? "0",
    }));

    const ws = XLSX.utils.json_to_sheet(excelRows);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Stocks");
    const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

    res.setHeader(
      "Content-Disposition",
      'attachment; filename="stocks.xlsx"',
    );
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    );
    res.send(buf);
  } catch (e) {
    next(e);
  }
});

/**
 * GET /stocks/summary-by-item?q=&kind=&page=&pageSize=
 * Tổng hợp tồn theo item (gộp nhiều kho, có phân trang)
 *  - kind: MACHINE | PART (tuỳ chọn)
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
 * Xuất bảng tổng hợp tồn theo item ra Excel
 */
r.get("/summary-by-item/export", async (req, res, next) => {
  try {
    const { q, kind } = req.query as any;

    const { rows } = await getStockSummaryByItem({
      q: q || undefined,
      kind: (kind as any) || undefined,
      page: 1,
      pageSize: 10_000, // lấy max cho export
    });

    const exportRows = rows.map((r: any) => ({
      sku: r.sku ?? "",
      name: r.name ?? "",
      unit: r.unit ?? "",
      kind: r.kind ?? "",
      sellPrice:
        r.sellPrice == null ? "" : (r.sellPrice as any)?.toString?.() ?? "",
      totalQty: (r.totalQty as any)?.toString?.() ?? "0",
    }));

    const ws = XLSX.utils.json_to_sheet(exportRows);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "SummaryByItem");
    const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

    res.setHeader(
      "Content-Disposition",
      'attachment; filename="stocks_summary_by_item.xlsx"',
    );
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    );
    res.send(buf);
  } catch (e) {
    next(e);
  }
});

export default r;
