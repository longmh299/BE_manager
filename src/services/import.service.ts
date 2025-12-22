// src/services/import.service.ts
import * as XLSX from "xlsx";
import { PrismaClient } from "@prisma/client";
import { importOpeningFromExcelBuffer } from "./stocks_import.service";

const prisma = new PrismaClient();

/**
 * IMPORT OPENING STOCK (TỒN ĐẦU)
 * - tạo ImportBatch
 * - gọi core importer (stocks_import.service)
 * - ghi ImportLog cho các dòng warning (nếu có)
 *
 * Dùng cho:
 *  - upload Excel tồn đầu
 *  - audit / trace lịch sử import
 */
export async function importOpeningStockExcel(
  buf: Buffer,
  fileName?: string
) {
  // 1️⃣ tạo batch
  const batchId = `OPENING-${Date.now()}`;

  await prisma.importBatch.create({
    data: {
      batchId,
      fileName: fileName ?? null,
      notes: "OPENING STOCK IMPORT (items + opening qty + base cost)",
    },
  });

  // 2️⃣ gọi core importer
  const result = await importOpeningFromExcelBuffer(buf, {
    mode: "replace",
    batchId,
  });

  // 3️⃣ ghi warning (KHÔNG fail import)
  if (result.warningRows && result.warningRows.length > 0) {
    await prisma.importLog.createMany({
      data: result.warningRows.map((w) => ({
        batchId,
        rowIndex: w.row, // row excel (1-based)
        status: "SKIPPED",
        message: w.message,
      })),
    });
  }

  // 4️⃣ trả kết quả
  return {
    ok: true,
    batchId,
    summary: result.summary,
    warningCount: result.warningRows?.length ?? 0,
  };
}

/**
 * (OPTIONAL)
 * IMPORT OPENING từ CSV (convert → Excel buffer)
 * dùng chung logic, không copy code
 */
export async function importOpeningStockCsv(
  csvText: string,
  fileName?: string
) {
  if (!csvText.trim()) {
    throw new Error("Empty CSV content");
  }

  const wb = XLSX.read(csvText, { type: "string" });
  const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

  return importOpeningStockExcel(buf, fileName);
}
