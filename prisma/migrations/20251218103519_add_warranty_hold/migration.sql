-- CreateEnum
CREATE TYPE "AllocationKind" AS ENUM ('NORMAL', 'WARRANTY_HOLD');

-- AlterTable
ALTER TABLE "Invoice" ADD COLUMN     "hasWarrantyHold" BOOLEAN NOT NULL DEFAULT false,
ADD COLUMN     "warrantyDueDate" TIMESTAMP(3),
ADD COLUMN     "warrantyHoldAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
ADD COLUMN     "warrantyHoldPct" DECIMAL(5,2) NOT NULL DEFAULT 0;

-- AlterTable
ALTER TABLE "PaymentAllocation" ADD COLUMN     "kind" "AllocationKind" NOT NULL DEFAULT 'NORMAL';

-- CreateTable
CREATE TABLE "WarrantyHold" (
    "id" TEXT NOT NULL,
    "invoiceId" TEXT NOT NULL,
    "amount" DECIMAL(18,2) NOT NULL,
    "dueDate" TIMESTAMP(3) NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'OPEN',
    "paidAt" TIMESTAMP(3),
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "WarrantyHold_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "WarrantyHold_invoiceId_key" ON "WarrantyHold"("invoiceId");

-- CreateIndex
CREATE INDEX "WarrantyHold_dueDate_idx" ON "WarrantyHold"("dueDate");

-- CreateIndex
CREATE INDEX "WarrantyHold_status_idx" ON "WarrantyHold"("status");

-- CreateIndex
CREATE INDEX "PaymentAllocation_kind_idx" ON "PaymentAllocation"("kind");

-- AddForeignKey
ALTER TABLE "WarrantyHold" ADD CONSTRAINT "WarrantyHold_invoiceId_fkey" FOREIGN KEY ("invoiceId") REFERENCES "Invoice"("id") ON DELETE CASCADE ON UPDATE CASCADE;
