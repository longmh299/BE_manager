-- AlterTable
ALTER TABLE "Invoice" ADD COLUMN     "receiveAccountId" TEXT;

-- CreateIndex
CREATE INDEX "Invoice_receiveAccountId_idx" ON "Invoice"("receiveAccountId");

-- AddForeignKey
ALTER TABLE "Invoice" ADD CONSTRAINT "Invoice_receiveAccountId_fkey" FOREIGN KEY ("receiveAccountId") REFERENCES "PaymentAccount"("id") ON DELETE SET NULL ON UPDATE CASCADE;
