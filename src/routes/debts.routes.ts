// src/routes/debts.routes.ts
import { Router } from "express";
import { requireAuth } from "../middlewares/auth";
import {
  getDebtsBySale,
  getDebtsSummaryBySale,
  updateDebtNote,
} from "../services/debts.service";
import ExcelJS from "exceljs";

const r = Router();

r.use(requireAuth);

function getUserId(req: any): string | undefined {
  return req.user?.id || req.userId;
}

function getUserRole(req: any): string | undefined {
  return req.user?.role || req.userRole;
}

function buildAuditMeta(req: any) {
  return {
    ip: req.ip,
    userAgent: req.headers?.["user-agent"],
    path: req.originalUrl || req.url,
    method: req.method,
  };
}

function requireUserOr401(req: any, res: any) {
  const userId = getUserId(req);
  if (!userId) {
    res.status(401).json({ ok: false, message: "Chưa đăng nhập." });
    return null;
  }
  return { userId, userRole: getUserRole(req) };
}

r.get("/ping", (req, res) => {
  res.json({ ok: true, route: "debts", message: "debts router is mounted" });
});

/**
 * Bảng công nợ chi tiết theo sale (từng dòng hàng)
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
 * ✅ Xuất Excel công nợ theo sale – từng dòng hàng
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
      views: [{ state: "frozen", ySplit: 1 }],
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

    const headerRow = sheet.getRow(1);
    headerRow.values = headers;
    headerRow.height = 20;
    headerRow.eachCell((cell) => {
      cell.font = { bold: true, color: { argb: "FFFFFFFF" } };
      cell.alignment = { vertical: "middle", horizontal: "center" };
      cell.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "FF2563EB" },
      };
      cell.border = {
        top: { style: "thin", color: { argb: "FFCBD5F5" } },
        left: { style: "thin", color: { argb: "FFCBD5F5" } },
        bottom: { style: "thin", color: { argb: "FFCBD5F5" } },
        right: { style: "thin", color: { argb: "FFCBD5F5" } },
      };
    });

    // ✅ FIX: format số tới cột 9 (Nợ) luôn
    [5, 6, 7, 8, 9].forEach((colIdx) => {
      const col = sheet.getColumn(colIdx);
      col.alignment = { horizontal: "right" };
      // cột 6-9 là các cột tiền/đơn giá/paid/debt
      if (colIdx >= 6 && colIdx <= 9) {
        col.numFmt = "#,##0;[Red]-#,##0";
      }
    });

    let totalAmount = 0;
    let totalPaid = 0;
    let totalDebt = 0;

    rows.forEach((r0: any, index: number) => {
      const qty = Number(r0.qty ?? 0);
      const unitPrice = Number(r0.unitPrice ?? 0);
      const amount = Number(r0.amount ?? 0);
      const paid = Number(r0.paid ?? 0);
      const debt = Number(r0.debt ?? 0);

      totalAmount += amount;
      totalPaid += paid;
      totalDebt += debt;

      const row = sheet.addRow([
        r0.date,
        r0.customerCode,
        r0.customerName,
        r0.itemName,
        qty,
        unitPrice,
        amount,
        paid,
        debt,
        r0.saleUserName ?? "",
        r0.note ?? "",
      ]);

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
          horizontal: colNumber >= 5 && colNumber <= 9 ? "right" : "left",
        };
        if (isOdd) {
          cell.fill = {
            type: "pattern",
            pattern: "solid",
            fgColor: { argb: "FFF9FAFB" },
          };
        }
      });

      const debtCell = row.getCell(9);
      if (debt > 0) {
        debtCell.font = { color: { argb: "FFDC2626" }, bold: true };
      }
    });

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
        color: colNumber === 9 ? { argb: "FFB91C1C" } : { argb: "FF111827" },
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
 * Lưu ghi chú công nợ (MUTATE → CÓ AUDIT)
 */
r.patch("/:invoiceId/note", async (req, res, next) => {
  try {
    const { invoiceId } = req.params;
    const { note } = req.body as { note?: string };

    const audit = requireUserOr401(req, res);
    if (!audit) return;

    const updated = await updateDebtNote(invoiceId, note ?? "", {
      userId: audit.userId,
      userRole: audit.userRole,
      meta: buildAuditMeta(req),
    });

    res.json({ ok: true, data: updated });
  } catch (err) {
    next(err);
  }
});

export default r;
