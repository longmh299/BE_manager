import type { PrismaClient } from "@prisma/client";
import { getSold30dMap } from "./fastMoving";
import { getStockMap } from "./stock";

/**
 * Cảnh báo sắp hết hàng
 * Rule:
 *  - Gate: sold30d < 3 => bỏ
 *  - MACHINE: dùng tỷ lệ cover = stock / sold30d
 *      < 0.5  => HIGH
 *      < 1.0  => MEDIUM
 *  - PART: dùng coverDays = stock / (sold30d / 30)
 *      <= 5 ngày  => HIGH
 *      <= 10 ngày => MEDIUM
 */
export async function getLowStockAlerts(prisma: PrismaClient) {
  // ===== FACTS =====
  const soldMap = await getSold30dMap(prisma);   // itemId -> sold30d
  const stockMap = await getStockMap(prisma);   // itemId -> stock qty

  // ===== RULE GATE: bán quá ít thì bỏ =====
  const candidateIds = [...soldMap.entries()]
    .filter(([, sold30d]) => sold30d >= 3)
    .map(([itemId]) => itemId);

  if (candidateIds.length === 0) {
    return [];
  }

  const items = await prisma.item.findMany({
    where: { id: { in: candidateIds } },
    select: {
      id: true,
      sku: true,
      name: true,
      kind: true, // MACHINE | PART
    },
  });

  const alerts: Array<{
    sku: string;
    name: string | null;
    kind: "MACHINE" | "PART";
    qty: number;
    level: "HIGH" | "MEDIUM";
    cover?: number;       // MACHINE
    coverDays?: number;  // PART
    reason?: string;
  }> = [];

  // ===== APPLY RULE =====
  for (const it of items) {
    const sold30d = soldMap.get(it.id) ?? 0;
    const stock = stockMap.get(it.id) ?? 0;

    // ================= MACHINE =================
    if (it.kind === "MACHINE") {
      if (sold30d <= 0) continue;

      // cover theo chu kỳ bán (tỷ lệ)
      const cover = stock / sold30d;

      if (cover < 0.5) {
        alerts.push({
          sku: it.sku,
          name: it.name,
          kind: "MACHINE",
          qty: stock,
          level: "HIGH",
          cover: Number(cover.toFixed(2)),
          reason: `Máy bán ${sold30d}/30 ngày, tồn ${stock}`,
        });
      } else if (cover < 1) {
        alerts.push({
          sku: it.sku,
          name: it.name,
          kind: "MACHINE",
          qty: stock,
          level: "MEDIUM",
          cover: Number(cover.toFixed(2)),
          reason: `Máy bán ${sold30d}/30 ngày, tồn ${stock}`,
        });
      }

      continue;
    }

    // ================= PART =================
    // bán trung bình / ngày
    const avgDaily = sold30d / 30;
    if (avgDaily <= 0) continue;

    const coverDays = stock / avgDaily;

    if (coverDays <= 5) {
      alerts.push({
        sku: it.sku,
        name: it.name,
        kind: "PART",
        qty: stock,
        level: "HIGH",
        coverDays: Math.round(coverDays),
        reason: `Linh kiện bán ~${avgDaily.toFixed(2)}/ngày, tồn ${stock}`,
      });
    } else if (coverDays <= 10) {
      alerts.push({
        sku: it.sku,
        name: it.name,
        kind: "PART",
        qty: stock,
        level: "MEDIUM",
        coverDays: Math.round(coverDays),
        reason: `Linh kiện bán ~${avgDaily.toFixed(2)}/ngày, tồn ${stock}`,
      });
    }
  }

  return alerts;
}
