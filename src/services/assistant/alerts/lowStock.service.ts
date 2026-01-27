import type { PrismaClient } from "@prisma/client";

type Severity = "CRITICAL" | "LOW";

function clamp(n: number, min: number, max: number) {
  return Math.min(Math.max(n, min), max);
}

/**
 * Threshold động theo tốc độ bán:
 * - bán rất chạy (>=20/tháng) => 50%
 * - chạy (>=10/tháng)        => 40%
 * - bình thường              => 30% (min 3)
 * + luôn có sàn "minFloor" để không quá thấp
 */
function calcPartThreshold(sold30d: number) {
  const s = Math.max(0, sold30d);
  if (s >= 20) return Math.max(6, Math.ceil(s * 0.5));
  if (s >= 10) return Math.max(4, Math.ceil(s * 0.4));
  return Math.max(3, Math.ceil(s * 0.3));
}

function calcDaysCover(qty: number, sold30d: number) {
  const q = Math.max(0, qty);
  const s = Math.max(0, sold30d);
  if (s <= 0) return null;
  const perDay = s / 30;
  if (perDay <= 0) return null;
  return Math.round((q / perDay) * 10) / 10; // 1 decimal
}

/** daysCover theo cửa sổ N ngày (dùng cho MACHINE theo Option A) */
function calcDaysCoverN(qty: number, soldNd: number, nDays: number) {
  const q = Math.max(0, qty);
  const s = Math.max(0, soldNd);
  if (s <= 0) return null;
  const perDay = s / nDays;
  if (perDay <= 0) return null;
  return Math.round((q / perDay) * 10) / 10; // 1 decimal
}

/**
 * Gợi ý nhập (PART):
 * - target = threshold * 2 (đủ 2 chu kỳ)
 * - suggest = max(target - qty, 0)
 */
function calcSuggestQty(qty: number, threshold: number) {
  const target = Math.max(0, threshold * 2);
  return Math.max(0, Math.ceil(target - Math.max(0, qty)));
}

/** Gợi ý nhập theo mục tiêu đủ X ngày bán (MACHINE theo Option A) */
function calcSuggestQtyByDaysCover(
  qty: number,
  soldNd: number,
  nDays: number,
  targetDays: number
) {
  const q = Math.max(0, qty);
  const s = Math.max(0, soldNd);
  if (s <= 0) return 0;
  const perDay = s / nDays;
  if (perDay <= 0) return 0;
  const targetQty = perDay * targetDays;
  return Math.max(0, Math.ceil(targetQty - q));
}

export async function buildLowStockAlerts(prisma: PrismaClient) {
  const now = new Date();
  const from30d = new Date(now.getTime() - 30 * 86400000);
  const from60d = new Date(now.getTime() - 60 * 86400000);

  // ✅ IMPORTANT: đúng enum hệ bạn
  const SALE_TYPE = "SALES";
  const APPROVED_STATUS = "APPROVED";

  // ===== SOLD 30D (PART) =====
  const sold30 = await prisma.invoiceLine.groupBy({
    by: ["itemId"],
    where: {
      invoice: {
        type: SALE_TYPE as any,
        status: APPROVED_STATUS as any,
        issueDate: { gte: from30d },
      },
    },
    _sum: { qty: true },
  });

  // ===== SOLD 60D (MACHINE) =====
  const sold60 = await prisma.invoiceLine.groupBy({
    by: ["itemId"],
    where: {
      invoice: {
        type: SALE_TYPE as any,
        status: APPROVED_STATUS as any,
        issueDate: { gte: from60d },
      },
    },
    _sum: { qty: true },
  });

  const sold30Map = new Map<string, number>();
  for (const s of sold30) sold30Map.set(s.itemId, Number(s._sum.qty ?? 0));

  const sold60Map = new Map<string, number>();
  for (const s of sold60) sold60Map.set(s.itemId, Number(s._sum.qty ?? 0));

  // ===== STOCK MAP (cộng hết stock theo location) =====
  const stocks = await prisma.stock.findMany({
    select: { itemId: true, qty: true },
  });

  const stockMap = new Map<string, number>();
  for (const s of stocks) {
    stockMap.set(s.itemId, (stockMap.get(s.itemId) || 0) + Number(s.qty));
  }

  // ===== ITEMS =====
  const items = await prisma.item.findMany({
    select: { id: true, sku: true, name: true, kind: true },
  });

  // ===== CONFIG cho MACHINE theo Option A (60 ngày) =====
  const MACHINE_DAYS_WINDOW = 60; // dùng sold60d/60
  const MACHINE_DAYS_COVER_LOW = 14; // còn đủ <= 14 ngày thì cảnh báo LOW
  const MACHINE_DAYS_TARGET = 30; // gợi ý nhập để đủ ~30 ngày

  const alerts: any[] = [];

  for (const it of items) {
    const qty = stockMap.get(it.id) ?? 0;

    // ---------- PART ----------
    if (it.kind === "PART") {
      const sold30d = sold30Map.get(it.id) ?? 0;

      // ✅ Gate nhu cầu: không bán => không cảnh báo
      if (sold30d <= 0) continue;

      const threshold = calcPartThreshold(sold30d);

      // ✅ Rule: qty <= threshold mới cảnh báo
      if (qty <= threshold) {
        const severity: Severity = qty <= 0 ? "CRITICAL" : "LOW";
        const daysCover = calcDaysCover(qty, sold30d);
        const suggestQty = calcSuggestQty(qty, threshold);

        alerts.push({
          sku: it.sku,
          name: it.name,
          kind: "PART",
          qty,
          sold30d,
          threshold,
          daysCover,
          suggestQty,
          severity,
        });
      }
    }

    // ---------- MACHINE (Option A, 60D) ----------
    if (it.kind === "MACHINE") {
      const sold60d = sold60Map.get(it.id) ?? 0;

      // ✅ Gate nhu cầu: 60d không bán => không cảnh báo
      if (sold60d <= 0) continue;

      // daysCover theo 60d
      const daysCover60 = calcDaysCoverN(qty, sold60d, MACHINE_DAYS_WINDOW);

      // CRITICAL: hết hàng
      if (qty <= 0) {
        const suggestQty = calcSuggestQtyByDaysCover(
          qty,
          sold60d,
          MACHINE_DAYS_WINDOW,
          MACHINE_DAYS_TARGET
        );

        alerts.push({
          sku: it.sku,
          name: it.name,
          kind: "MACHINE",
          qty,
          sold60d,
          threshold: null,
          daysCover: daysCover60,
          suggestQty,
          severity: "CRITICAL" as Severity,
        });
        continue;
      }

      // LOW: còn hàng nhưng sắp hết theo days cover
      if (daysCover60 !== null && daysCover60 <= MACHINE_DAYS_COVER_LOW) {
        const suggestQty = calcSuggestQtyByDaysCover(
          qty,
          sold60d,
          MACHINE_DAYS_WINDOW,
          MACHINE_DAYS_TARGET
        );

        alerts.push({
          sku: it.sku,
          name: it.name,
          kind: "MACHINE",
          qty,
          sold60d,
          threshold: null,
          daysCover: daysCover60,
          suggestQty,
          severity: "LOW" as Severity,
        });
      }
    }
  }

  // ===== SORT: CRITICAL lên đầu -> qty tăng dần -> bán nhiều lên trước =====
  const sevRank = (s: Severity) => (s === "CRITICAL" ? 0 : 1);

  alerts.sort((a, b) => {
    const ra = sevRank(a.severity);
    const rb = sevRank(b.severity);
    if (ra !== rb) return ra - rb;

    if (a.qty !== b.qty) return a.qty - b.qty;

    const sa = Number(a.sold30d ?? a.sold60d ?? 0);
    const sb = Number(b.sold30d ?? b.sold60d ?? 0);
    if (sa !== sb) return sb - sa;

    return String(a.sku).localeCompare(String(b.sku));
  });

  return alerts;
}
