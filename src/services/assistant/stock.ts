import type { PrismaClient } from "@prisma/client";

export async function getStockMap(prisma: PrismaClient) {
  const rows = await prisma.stock.findMany({
    select: { itemId: true, qty: true },
  });

  const map = new Map<string, number>();
  for (const r of rows) {
    map.set(r.itemId, Number(r.qty));
  }
  return map;
}
