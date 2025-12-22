import { PrismaClient } from "@prisma/client";
const prisma = new PrismaClient();

async function main() {
  /* ===================== UNIT ===================== */
  const units = [
    { code: "pcs", name: "Cái" },
    { code: "pair", name: "Cặp" },
    { code: "m", name: "Mét" },
  ];

  for (const u of units) {
    await prisma.unit.upsert({
      where: { code: u.code },
      update: { name: u.name },
      create: u,
    });
  }

  /* ===================== PAYMENT ACCOUNT ===================== */
  const accounts = [
    { code: "CASH", name: "Tiền mặt", type: "CASH" as const },
    { code: "BANK1", name: "Tài khoản 1", type: "BANK" as const },
    { code: "BANK2", name: "Tài khoản 2", type: "BANK" as const },
  ];

  for (const a of accounts) {
    await prisma.paymentAccount.upsert({
      where: { code: a.code },
      update: {
        name: a.name,
        type: a.type,
        isActive: true,
      },
      create: {
        code: a.code,
        name: a.name,
        type: a.type,
        isActive: true,
      },
    });
  }

  /* ===================== DEFAULT WAREHOUSE ===================== */
  await prisma.location.upsert({
    where: { code: "KHO-01" },
    update: {
      name: "Kho chính",
      kind: "warehouse",
    },
    create: {
      code: "KHO-01",
      name: "Kho chính",
      kind: "warehouse",
    },
  });

  console.log("✅ Seeded: Units, Payment Accounts, Default Warehouse");
}

main()
  .catch((e) => {
    console.error("❌ Seed error:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
