// src/routes/debts.routes.ts
import { Router } from "express";
import { requireAuth } from "../middlewares/auth";
import {
  getDebtsBySale,
  getDebtsSummaryBySale,
  updateDebtNote,
} from "../services/debts.service";
import ExcelJS from "exceljs"; // ✅ dùng exceljs thay vì xlsx

const r = Router();

r.use(requireAuth);

r.get("/ping", (req, res) => {
  res.json({ ok: true, route: "debts", message: "debts router is mounted" });
});

/**
 * Bảng công nợ chi tiết theo sale
 */
r.get("/by-sale", async (req, res, next) => {
  try {
    const rows = await getDebtsBySale({
      from: req.query.from as string | undefined,
      to: req.query.to as string | undefined,
      saleUserId: req.query.saleUserId as string | undefined,
    });

    res.json(rows);
  } catch (err) {
    next(err);
  }
});

/**
 * Tổng hợp công nợ theo sale
 */
r.get("/summary-by-sale", async (req, res, next) => {
  try {
    const rows = await getDebtsSummaryBySale({
      from: req.query.from as string | undefined,
      to: req.query.to as string | undefined,
    });

    res.json(rows);
  } catch (err) {
    next(err);
  }
});

/**
 * ✅ Xuất Excel công nợ theo sale – có màu mè, format đẹp
 * GET /api/debts/by-sale/export?from=&to=&saleUserId=
 */
r.get("/by-sale/export", async (req, res, next) => {
  try {
    const rows = await getDebtsBySale({
      from: req.query.from as string | undefined,
      to: req.query.to as string | undefined,
      saleUserId: req.query.saleUserId as string | undefined,
    });

    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet("Cong_no", {
      views: [{ state: "frozen", ySplit: 1 }], // freeze hàng tiêu đề
    });

    const headers = [
      "NGÀY",
      "Mã KH",
      "Tên KH",
      "Tên hàng",
      "SL",
      "Đơn giá",
      "Thành tiền",
      "Thanh toán",
      "Nợ",
      "Sale",
      "Ghi chú",
    ];

    // Cột + width
    sheet.columns = [
      { header: headers[0], key: "date", width: 12 },
      { header: headers[1], key: "customerCode", width: 14 },
      { header: headers[2], key: "customerName", width: 26 },
      { header: headers[3], key: "itemName", width: 30 },
      { header: headers[4], key: "qty", width: 10 },
      { header: headers[5], key: "unitPrice", width: 14 },
      { header: headers[6], key: "amount", width: 16 },
      { header: headers[7], key: "paid", width: 16 },
      { header: headers[8], key: "debt", width: 16 },
      { header: headers[9], key: "saleUserName", width: 18 },
      { header: headers[10], key: "note", width: 28 },
    ];

    // Header row style
    const headerRow = sheet.getRow(1);
    headerRow.values = headers;
    headerRow.height = 20;
    headerRow.eachCell((cell) => {
      cell.font = { bold: true, color: { argb: "FFFFFFFF" } };
      cell.alignment = { vertical: "middle", horizontal: "center" };
      cell.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "FF2563EB" }, // xanh dương
      };
      cell.border = {
        top: { style: "thin", color: { argb: "FFCBD5F5" } },
        left: { style: "thin", color: { argb: "FFCBD5F5" } },
        bottom: { style: "thin", color: { argb: "FFCBD5F5" } },
        right: { style: "thin", color: { argb: "FFCBD5F5" } },
      };
    });

    // Number format & alignment cho cột số
    [5, 6, 7, 8, 9].forEach((colIdx) => {
      const col = sheet.getColumn(colIdx);
      col.alignment = { horizontal: "right" };
      if (colIdx >= 6 && colIdx <= 8) {
        // Đơn giá / Thành tiền / Thanh toán / Nợ = tiền
        col.numFmt = "#,##0;[Red]-#,##0";
      }
    });

    // Thêm data rows
    let totalAmount = 0;
    let totalPaid = 0;
    let totalDebt = 0;

    rows.forEach((r: any, index: number) => {
      const qty = Number(r.qty ?? 0);
      const unitPrice = Number(r.unitPrice ?? 0);
      const amount = Number(r.amount ?? 0);
      const paid = Number(r.paid ?? 0);
      const debt = Number(r.debt ?? 0);

      totalAmount += amount;
      totalPaid += paid;
      totalDebt += debt;

      const row = sheet.addRow([
        r.date,
        r.customerCode,
        r.customerName,
        r.itemName,
        qty,
        unitPrice,
        amount,
        paid,
        debt,
        r.saleUserName ?? "",
        r.note ?? "",
      ]);

      // Zebra stripe: dòng chẵn lẻ khác màu
      const isOdd = (index + 1) % 2 === 1;
      row.eachCell((cell, colNumber) => {
        cell.border = {
          top: { style: "thin", color: { argb: "FFE5E7EB" } },
          left: { style: "thin", color: { argb: "FFE5E7EB" } },
          bottom: { style: "thin", color: { argb: "FFE5E7EB" } },
          right: { style: "thin", color: { argb: "FFE5E7EB" } },
        };
        cell.alignment = {
          vertical: "middle",
          horizontal:
            colNumber >= 5 && colNumber <= 9 ? "right" : "left",
        };
        if (isOdd) {
          cell.fill = {
            type: "pattern",
            pattern: "solid",
            fgColor: { argb: "FFF9FAFB" }, // xám rất nhẹ
          };
        }
      });

      // tô đỏ nhẹ cho cột Nợ
      const debtCell = row.getCell(9);
      if (debt > 0) {
        debtCell.font = { color: { argb: "FFDC2626" }, bold: true };
      }
    });

    // Hàng tổng cuối
    const totalRow = sheet.addRow([
      "",
      "",
      "",
      "TỔNG",
      "",
      "",
      totalAmount,
      totalPaid,
      totalDebt,
      "",
      "",
    ]);

    totalRow.eachCell((cell, colNumber) => {
      cell.font = {
        bold: true,
        color:
          colNumber === 9
            ? { argb: "FFB91C1C" } // đỏ hơn cho tổng nợ
            : { argb: "FF111827" },
      };
      cell.alignment = {
        vertical: "middle",
        horizontal: colNumber >= 7 && colNumber <= 9 ? "right" : "left",
      };
      cell.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "FFE5E7EB" },
      };
      cell.border = {
        top: { style: "medium", color: { argb: "FF9CA3AF" } },
        left: { style: "thin", color: { argb: "FF9CA3AF" } },
        bottom: { style: "medium", color: { argb: "FF9CA3AF" } },
        right: { style: "thin", color: { argb: "FF9CA3AF" } },
      };
    });

    // Auto filter trên header
    // sheet.autoFilter = {
    //   from: { row: 1, column: 1 },
    //   to: { row: 1, column: headers.length },
    // };

    const buf = await workbook.xlsx.writeBuffer();

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="cong-no-theo-sale.xlsx"`
    );
    res.send(Buffer.from(buf));
  } catch (err) {
    next(err);
  }
});

/**
 * Lưu ghi chú công nợ
 */
r.patch("/:invoiceId/note", async (req, res, next) => {
  try {
    const { invoiceId } = req.params;
    const { note } = req.body as { note?: string };
    const updated = await updateDebtNote(invoiceId, note ?? "");
    res.json({ ok: true, data: updated });
  } catch (err) {
    next(err);
  }
});

export default r;
