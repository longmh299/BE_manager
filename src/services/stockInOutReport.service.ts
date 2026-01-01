// src/services/stockInOutReport.service.ts
import { Prisma, PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

function toNum(d: Prisma.Decimal | number | string | null | undefined): number {
  if (d == null) return 0;
  if (typeof d === "number") return d;
  return Number(d.toString());
}

function toISODateOnly(d: Date) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function coerceDate(v?: string | Date) {
  if (!v) return undefined;
  if (v instanceof Date) return v;
  const d = new Date(String(v));
  if (Number.isNaN(d.getTime())) return undefined;
  return d;
}

function parseRange(fromRaw?: string | Date, toRaw?: string | Date) {
  const now = new Date();

  const fromD = coerceDate(fromRaw) ?? new Date(now.getFullYear(), now.getMonth(), 1);
  const toD = coerceDate(toRaw) ?? now;

  // start of day
  const fromStart = new Date(fromD);
  fromStart.setHours(0, 0, 0, 0);

  // end of day
  const toEnd = new Date(toD);
  toEnd.setHours(23, 59, 59, 999);

  return { fromStart, toEnd };
}

async function ensureSingleWarehouse(warehouseId?: string | null) {
  if (warehouseId) {
    const w = await prisma.location.findUnique({ where: { id: warehouseId } });
    if (!w) throw new Error("Warehouse not found");
    return w;
  }

  const warehouses = await prisma.location.findMany({ where: { kind: "warehouse" } });
  if (warehouses.length === 0) throw new Error("No warehouse found");
  if (warehouses.length > 1) {
    throw new Error("Multiple warehouses detected. Please specify warehouseId.");
  }
  return warehouses[0];
}

export type StockInOutRow = {
  itemId: string;
  sku: string;
  name: string;
  unitCode: string;

  openingQty: number;
  inQty: number;
  outQty: number;
  closingQty: number;
};

export async function getStockInOutReport(params: {
  from?: string | Date;
  to?: string | Date;
  q?: string;
  warehouseId?: string;
}) {
  const q = (params.q || "").trim();
  const { fromStart, toEnd } = parseRange(params.from, params.to);

  const wh = await ensureSingleWarehouse(params.warehouseId);

  // 1) current stocks (as-of now)
  const stockRows = await prisma.stock.findMany({
    where: {
      locationId: wh.id,
      ...(q
        ? {
            item: {
              is: {
                OR: [
                  { name: { contains: q, mode: "insensitive" } },
                  { sku: { contains: q, mode: "insensitive" } },
                ],
              },
            },
          }
        : {}),
    },
    select: {
      itemId: true,
      qty: true,
      item: {
        select: {
          sku: true,
          name: true,
          unit: { select: { code: true } },
        },
      },
    },
  });

  const currentQtyByItem = new Map<string, number>();
  for (const s of stockRows) currentQtyByItem.set(s.itemId, toNum(s.qty));

  // 2) in/out within period (by occurredAt)
  type AggRow = { itemId: string; inqty: any; outqty: any };

  const inOutAgg = await prisma.$queryRaw<AggRow[]>`
    SELECT
      ml."itemId" as "itemId",
      SUM(
        CASE
          WHEN m."type" = 'IN' AND ml."toLocationId" = ${wh.id} THEN ml."qty"
          WHEN m."type" = 'TRANSFER' AND ml."toLocationId" = ${wh.id} THEN ml."qty"
          WHEN m."type" = 'ADJUST' AND ml."toLocationId" = ${wh.id} THEN ml."qty"
          ELSE 0
        END
      ) as "inqty",
      SUM(
        CASE
          WHEN m."type" = 'OUT' AND ml."fromLocationId" = ${wh.id} THEN ml."qty"
          WHEN m."type" = 'TRANSFER' AND ml."fromLocationId" = ${wh.id} THEN ml."qty"
          WHEN m."type" = 'ADJUST' AND ml."fromLocationId" = ${wh.id} THEN ml."qty"
          ELSE 0
        END
      ) as "outqty"
    FROM "MovementLine" ml
    JOIN "Movement" m ON m."id" = ml."movementId"
    JOIN "Item" i ON i."id" = ml."itemId"
    WHERE
      m."posted" = true
      AND m."occurredAt" >= ${fromStart}
      AND m."occurredAt" <= ${toEnd}
      AND (ml."fromLocationId" = ${wh.id} OR ml."toLocationId" = ${wh.id})
      AND (${q === ""} OR i."name" ILIKE ${"%" + q + "%"} OR i."sku" ILIKE ${"%" + q + "%"})
    GROUP BY ml."itemId"
  `;

  const inQtyByItem = new Map<string, number>();
  const outQtyByItem = new Map<string, number>();
  for (const r of inOutAgg) {
    inQtyByItem.set(r.itemId, toNum(r.inqty));
    outQtyByItem.set(r.itemId, toNum(r.outqty));
  }

  // 3) back-calc opening/closing from current
  type NetRow = { itemId: string; net: any };

  const netFromNow = await prisma.$queryRaw<NetRow[]>`
    SELECT
      ml."itemId" as "itemId",
      SUM(
        CASE
          WHEN m."type" = 'IN' AND ml."toLocationId" = ${wh.id} THEN ml."qty"
          WHEN m."type" = 'OUT' AND ml."fromLocationId" = ${wh.id} THEN -ml."qty"
          WHEN m."type" = 'TRANSFER' AND ml."toLocationId" = ${wh.id} THEN ml."qty"
          WHEN m."type" = 'TRANSFER' AND ml."fromLocationId" = ${wh.id} THEN -ml."qty"
          WHEN m."type" = 'ADJUST' AND ml."toLocationId" = ${wh.id} THEN ml."qty"
          WHEN m."type" = 'ADJUST' AND ml."fromLocationId" = ${wh.id} THEN -ml."qty"
          ELSE 0
        END
      ) as "net"
    FROM "MovementLine" ml
    JOIN "Movement" m ON m."id" = ml."movementId"
    JOIN "Item" i ON i."id" = ml."itemId"
    WHERE
      m."posted" = true
      AND m."occurredAt" >= ${fromStart}
      AND (ml."fromLocationId" = ${wh.id} OR ml."toLocationId" = ${wh.id})
      AND (${q === ""} OR i."name" ILIKE ${"%" + q + "%"} OR i."sku" ILIKE ${"%" + q + "%"})
    GROUP BY ml."itemId"
  `;

  const netAfterTo = await prisma.$queryRaw<NetRow[]>`
    SELECT
      ml."itemId" as "itemId",
      SUM(
        CASE
          WHEN m."type" = 'IN' AND ml."toLocationId" = ${wh.id} THEN ml."qty"
          WHEN m."type" = 'OUT' AND ml."fromLocationId" = ${wh.id} THEN -ml."qty"
          WHEN m."type" = 'TRANSFER' AND ml."toLocationId" = ${wh.id} THEN ml."qty"
          WHEN m."type" = 'TRANSFER' AND ml."fromLocationId" = ${wh.id} THEN -ml."qty"
          WHEN m."type" = 'ADJUST' AND ml."toLocationId" = ${wh.id} THEN ml."qty"
          WHEN m."type" = 'ADJUST' AND ml."fromLocationId" = ${wh.id} THEN -ml."qty"
          ELSE 0
        END
      ) as "net"
    FROM "MovementLine" ml
    JOIN "Movement" m ON m."id" = ml."movementId"
    JOIN "Item" i ON i."id" = ml."itemId"
    WHERE
      m."posted" = true
      AND m."occurredAt" > ${toEnd}
      AND (ml."fromLocationId" = ${wh.id} OR ml."toLocationId" = ${wh.id})
      AND (${q === ""} OR i."name" ILIKE ${"%" + q + "%"} OR i."sku" ILIKE ${"%" + q + "%"})
    GROUP BY ml."itemId"
  `;

  const netFromNowByItem = new Map<string, number>();
  for (const r of netFromNow) netFromNowByItem.set(r.itemId, toNum(r.net));

  const netAfterToByItem = new Map<string, number>();
  for (const r of netAfterTo) netAfterToByItem.set(r.itemId, toNum(r.net));

  // 4) union itemIds
  const itemIds = new Set<string>();
  for (const s of stockRows) itemIds.add(s.itemId);
  for (const r of inOutAgg) itemIds.add(r.itemId);
  for (const r of netFromNow) itemIds.add(r.itemId);
  for (const r of netAfterTo) itemIds.add(r.itemId);

  if (itemIds.size === 0) {
    return {
      warehouse: { id: wh.id, code: wh.code, name: wh.name },
      from: toISODateOnly(fromStart),
      to: toISODateOnly(toEnd),
      rows: [] as StockInOutRow[],
      totals: { openingQty: 0, inQty: 0, outQty: 0, closingQty: 0 },
    };
  }

  const items = await prisma.item.findMany({
    where: { id: { in: Array.from(itemIds) } },
    select: { id: true, sku: true, name: true, unit: { select: { code: true } } },
    orderBy: [{ name: "asc" }],
  });

  const rows: StockInOutRow[] = items.map((it) => {
    const current = currentQtyByItem.get(it.id) || 0;

    const netFrom = netFromNowByItem.get(it.id) || 0;
    const netAfter = netAfterToByItem.get(it.id) || 0;

    const openingQty = current - netFrom;
    const closingQty = current - netAfter;

    const inQty = inQtyByItem.get(it.id) || 0;
    const outQty = outQtyByItem.get(it.id) || 0;

    return {
      itemId: it.id,
      sku: it.sku,
      name: it.name,
      unitCode: it.unit?.code || "",
      openingQty,
      inQty,
      outQty,
      closingQty,
    };
  });

  const totals = rows.reduce(
    (s, r) => {
      s.openingQty += r.openingQty;
      s.inQty += r.inQty;
      s.outQty += r.outQty;
      s.closingQty += r.closingQty;
      return s;
    },
    { openingQty: 0, inQty: 0, outQty: 0, closingQty: 0 }
  );

  return {
    warehouse: { id: wh.id, code: wh.code, name: wh.name },
    from: toISODateOnly(fromStart),
    to: toISODateOnly(toEnd),
    rows,
    totals,
  };
}
