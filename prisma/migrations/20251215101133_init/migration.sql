/*
  Warnings:

  - You are about to alter the column `paidAmount` on the `Invoice` table. The data in that column could be lost. The data in that column will be cast from `Decimal(65,30)` to `Decimal(18,2)`.
  - You are about to drop the column `unit` on the `Item` table. All the data in the column will be lost.
  - Added the required column `unitId` to the `Item` table without a default value. This is not possible if the table is not empty.

*/
-- CreateEnum
CREATE TYPE "InvoiceStatus" AS ENUM ('DRAFT', 'SUBMITTED', 'APPROVED', 'REJECTED');

-- CreateEnum
CREATE TYPE "AccountType" AS ENUM ('CASH', 'BANK', 'EWALLET', 'OTHER');

-- AlterTable
ALTER TABLE "Invoice" ADD COLUMN     "approvedAt" TIMESTAMP(3),
ADD COLUMN     "approvedById" TEXT,
ADD COLUMN     "partnerCode" TEXT,
ADD COLUMN     "status" "InvoiceStatus" NOT NULL DEFAULT 'DRAFT',
ADD COLUMN     "totalCost" DECIMAL(18,2),
ALTER COLUMN "paidAmount" SET DATA TYPE DECIMAL(18,2);

-- AlterTable
ALTER TABLE "InvoiceLine" ADD COLUMN     "costTotal" DECIMAL(18,2),
ADD COLUMN     "unitCost" DECIMAL(18,2);

-- AlterTable
ALTER TABLE "Item" DROP COLUMN "unit",
ADD COLUMN     "unitId" TEXT NOT NULL;

-- AlterTable
ALTER TABLE "MovementLine" ADD COLUMN     "costTotal" DECIMAL(18,4),
ADD COLUMN     "unitCost" DECIMAL(18,4);

-- AlterTable
ALTER TABLE "Payment" ADD COLUMN     "accountId" TEXT;

-- AlterTable
ALTER TABLE "Stock" ADD COLUMN     "avgCost" DECIMAL(18,4) NOT NULL DEFAULT 0;

-- CreateTable
CREATE TABLE "Unit" (
    "id" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Unit_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StockSnapshot" (
    "id" TEXT NOT NULL,
    "itemId" TEXT NOT NULL,
    "locationId" TEXT NOT NULL,
    "date" TIMESTAMP(3) NOT NULL,
    "qty" DECIMAL(18,3) NOT NULL,
    "avgCost" DECIMAL(18,4),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "StockSnapshot_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "MonthlyAvgCost" (
    "id" TEXT NOT NULL,
    "itemId" TEXT NOT NULL,
    "locationId" TEXT NOT NULL,
    "year" INTEGER NOT NULL,
    "month" INTEGER NOT NULL,
    "avgCost" DECIMAL(18,4) NOT NULL,
    "qtyTotal" DECIMAL(18,3) NOT NULL,
    "computedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "MonthlyAvgCost_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PaymentAccount" (
    "id" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "type" "AccountType" NOT NULL DEFAULT 'BANK',
    "bankName" TEXT,
    "accountNo" TEXT,
    "holder" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "PaymentAccount_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ImportBatch" (
    "id" TEXT NOT NULL,
    "batchId" TEXT NOT NULL,
    "fileName" TEXT,
    "actorId" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "notes" TEXT,

    CONSTRAINT "ImportBatch_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ImportLog" (
    "id" TEXT NOT NULL,
    "batchId" TEXT NOT NULL,
    "rowIndex" INTEGER NOT NULL,
    "refNo" TEXT,
    "itemId" TEXT,
    "status" TEXT NOT NULL,
    "message" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ImportLog_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Unit_code_key" ON "Unit"("code");

-- CreateIndex
CREATE INDEX "StockSnapshot_itemId_idx" ON "StockSnapshot"("itemId");

-- CreateIndex
CREATE INDEX "StockSnapshot_locationId_idx" ON "StockSnapshot"("locationId");

-- CreateIndex
CREATE INDEX "StockSnapshot_date_idx" ON "StockSnapshot"("date");

-- CreateIndex
CREATE UNIQUE INDEX "StockSnapshot_itemId_locationId_date_key" ON "StockSnapshot"("itemId", "locationId", "date");

-- CreateIndex
CREATE INDEX "MonthlyAvgCost_itemId_idx" ON "MonthlyAvgCost"("itemId");

-- CreateIndex
CREATE INDEX "MonthlyAvgCost_locationId_idx" ON "MonthlyAvgCost"("locationId");

-- CreateIndex
CREATE INDEX "MonthlyAvgCost_year_month_idx" ON "MonthlyAvgCost"("year", "month");

-- CreateIndex
CREATE UNIQUE INDEX "MonthlyAvgCost_itemId_locationId_year_month_key" ON "MonthlyAvgCost"("itemId", "locationId", "year", "month");

-- CreateIndex
CREATE UNIQUE INDEX "PaymentAccount_code_key" ON "PaymentAccount"("code");

-- CreateIndex
CREATE UNIQUE INDEX "ImportBatch_batchId_key" ON "ImportBatch"("batchId");

-- CreateIndex
CREATE INDEX "ImportLog_batchId_idx" ON "ImportLog"("batchId");

-- CreateIndex
CREATE INDEX "ImportLog_itemId_idx" ON "ImportLog"("itemId");

-- CreateIndex
CREATE INDEX "Item_unitId_idx" ON "Item"("unitId");

-- CreateIndex
CREATE INDEX "Payment_accountId_idx" ON "Payment"("accountId");

-- AddForeignKey
ALTER TABLE "Item" ADD CONSTRAINT "Item_unitId_fkey" FOREIGN KEY ("unitId") REFERENCES "Unit"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Invoice" ADD CONSTRAINT "Invoice_approvedById_fkey" FOREIGN KEY ("approvedById") REFERENCES "User"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StockSnapshot" ADD CONSTRAINT "StockSnapshot_itemId_fkey" FOREIGN KEY ("itemId") REFERENCES "Item"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StockSnapshot" ADD CONSTRAINT "StockSnapshot_locationId_fkey" FOREIGN KEY ("locationId") REFERENCES "Location"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MonthlyAvgCost" ADD CONSTRAINT "MonthlyAvgCost_itemId_fkey" FOREIGN KEY ("itemId") REFERENCES "Item"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MonthlyAvgCost" ADD CONSTRAINT "MonthlyAvgCost_locationId_fkey" FOREIGN KEY ("locationId") REFERENCES "Location"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Payment" ADD CONSTRAINT "Payment_accountId_fkey" FOREIGN KEY ("accountId") REFERENCES "PaymentAccount"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ImportLog" ADD CONSTRAINT "ImportLog_batchId_fkey" FOREIGN KEY ("batchId") REFERENCES "ImportBatch"("batchId") ON DELETE CASCADE ON UPDATE CASCADE;
