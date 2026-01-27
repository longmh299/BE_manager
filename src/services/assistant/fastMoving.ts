import type { PrismaClient } from "@prisma/client";

export async function getSold30dMap(prisma: PrismaClient) {
  const from = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);

  const rows = await prisma.invoiceLine.groupBy({
    by: ["itemId"],
    where: {
      invoice: {
        type: "SALES",
        status: "APPROVED",
        issueDate: { gte: from },
      },
    },
    _sum: { qty: true },
  });

  const map = new Map<string, number>();
  for (const r of rows) {
    map.set(r.itemId, Number(r._sum.qty ?? 0));
  }
  return map;
}
