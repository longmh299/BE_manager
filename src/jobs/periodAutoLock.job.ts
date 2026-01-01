// src/jobs/periodAutoLock.job.ts
import { PrismaClient, MovementType, Prisma } from "@prisma/client";

const prisma = new PrismaClient();

const CHECK_INTERVAL_MS = Number(process.env.PERIOD_AUTOLOCK_INTERVAL_MS || 30 * 60 * 1000);
const PERIOD_AUTOLOCK_ENABLED = String(process.env.PERIOD_AUTOLOCK_ENABLED ?? "1") !== "0";

// rebuild tháng hiện tại mỗi lần chạy (để TB kỳ tháng hiện tại có số). Mặc định ON.
const PERIOD_AVGCOST_BUILD_CURRENT = String(process.env.PERIOD_AVGCOST_BUILD_CURRENT ?? "1") !== "0";

// rebuild lại dù đã có (để test)
const PERIOD_AVGCOST_REBUILD = String(process.env.PERIOD_AVGCOST_REBUILD ?? "0") === "1";

const CREATE_MANY_CHUNK = Number(process.env.PERIOD_AVGCOST_CHUNK || 1000);

/**
 * =========================
 * Time helpers (VN timezone, fixed +07:00)
 * =========================
 */
const VN_OFFSET_MS = 7 * 60 * 60 * 1000;

function pad2(n: number) {
  return String(n).padStart(2, "0");
}

function vnParts(d: Date) {
  const x = new Date(d.getTime() + VN_OFFSET_MS);
  return { year: x.getUTCFullYear(), month: x.getUTCMonth() + 1, day: x.getUTCDate() };
}

function makeVNDate(year: number, month1to12: number, day: number, hh = 0, mm = 0, ss = 0, ms = 0) {
  return new Date(Date.UTC(year, month1to12 - 1, day, hh - 7, mm, ss, ms));
}

function fmtYMDVN(d: Date) {
  const p = vnParts(d);
  return `${p.year}-${pad2(p.month)}-${pad2(p.day)}`;
}

function toEndOfDayVN(d: Date) {
  const p = vnParts(d);
  return makeVNDate(p.year, p.month, p.day, 23, 59, 59, 999);
}

function startOfMonthVN(year: number, month1to12: number) {
  return makeVNDate(year, month1to12, 1, 0, 0, 0, 0);
}

function startOfNextMonthVN(year: number, month1to12: number) {
  if (month1to12 >= 12) return makeVNDate(year + 1, 1, 1, 0, 0, 0, 0);
  return makeVNDate(year, month1to12 + 1, 1, 0, 0, 0, 0);
}

function ymFromDateVN(d: Date) {
  const p = vnParts(d);
  return { year: p.year, month: p.month };
}

function ymLabelVN(d: Date) {
  const p = vnParts(d);
  return `${p.year}-${pad2(p.month)}`;
}

function endOfPrevMonthVN(now = new Date()) {
  const { year, month } = ymFromDateVN(now);
  const startCur = startOfMonthVN(year, month);
  return new Date(startCur.getTime() - 1);
}

function toNum(v: any): number {
  if (v == null) return 0;
  if (typeof v === "number") return v;
  return Number(v.toString());
}

async function getClosedUntilEOD(): Promise<Date | null> {
  const row = await prisma.periodLock.findFirst({
    orderBy: { closedUntil: "desc" },
    select: { closedUntil: true },
  });
  if (!row?.closedUntil) return null;
  return toEndOfDayVN(row.closedUntil);
}

/** ========================= AVG COST MONTHLY ========================= **/

let _defaultLocationId: string | null | undefined;
async function getDefaultLocationId(): Promise<string | null> {
  if (_defaultLocationId !== undefined) return _defaultLocationId;
  const loc = await prisma.location.findFirst({ select: { id: true } });
  _defaultLocationId = loc?.id ?? null;
  return _defaultLocationId;
}

async function monthlyAvgCostExists(year: number, month: number) {
  const c = await prisma.monthlyAvgCost.count({ where: { year, month } });
  return c > 0;
}

function buildMonthlyAvgCostCreateData(args: {
  year: number;
  month: number;
  itemId: string;
  locationId: string;
  avgCost: number;
  qtyTotal: number;
}) {
  return {
    year: args.year,
    month: args.month,
    itemId: args.itemId,
    locationId: args.locationId,
    avgCost: new Prisma.Decimal(String(args.avgCost)),
    qtyTotal: new Prisma.Decimal(String(args.qtyTotal)),
  };
}

/**
 * Build MonthlyAvgCost cho tháng (year,month) bằng reverse từ Stock hiện tại:
 * Begin = Now - Net(from start->now)
 * avgCostMonth = (BeginVal + InValMonth) / (BeginQty + InQtyMonth)
 */
async function ensureMonthlyAvgCostForMonth(year: number, month: number, opts?: { force?: boolean }) {
  const force = !!opts?.force;

  if (!force && !PERIOD_AVGCOST_REBUILD) {
    const existed = await monthlyAvgCostExists(year, month);
    if (existed) return { ok: true, didBuild: false };
  }

  const start = startOfMonthVN(year, month);
  const end = startOfNextMonthVN(year, month);
  const now = new Date();
  const defaultLocId = await getDefaultLocationId();

  // 1) Stock hiện tại
  const stocks = await prisma.stock.findMany({
    select: { itemId: true, locationId: true, qty: true, avgCost: true },
  });

  const stockMap = new Map<string, { qtyNow: number; valNow: number }>();
  for (const s of stocks) {
    const qtyNow = toNum(s.qty);
    const avgNow = toNum(s.avgCost);
    stockMap.set(`${s.itemId}::${s.locationId}`, { qtyNow, valNow: qtyNow * avgNow });
  }

  // 2) MovementLine từ đầu tháng -> NOW (dựa trên occurredAt)
  const lines = await prisma.movementLine.findMany({
    where: {
      movement: {
        posted: true,
        occurredAt: { gte: start, lte: now }, // ✅ chuẩn theo ngày phát sinh
        type: { in: [MovementType.IN, MovementType.OUT, MovementType.TRANSFER, MovementType.ADJUST] },
      },
    },
    select: {
      itemId: true,
      qty: true,
      unitCost: true,
      costTotal: true,
      fromLocationId: true,
      toLocationId: true,
      movement: { select: { type: true, occurredAt: true } },
    },
  });

  const netMap = new Map<string, { netQty: number; netVal: number }>(); // IN - OUT từ start -> now
  const inMonthMap = new Map<string, { inQty: number; inVal: number }>(); // IN trong [start,end)

  for (const l of lines as any[]) {
    const m = l.movement;
    const t = m?.occurredAt ? new Date(m.occurredAt) : null;
    if (!t) continue;

    const qtyRaw = toNum(l.qty);
    if (qtyRaw === 0) continue;

    const qtyAbs = Math.abs(qtyRaw);

    const unitCost = toNum(l.unitCost);
    const costAbs = l.costTotal != null ? Math.abs(toNum(l.costTotal)) : qtyAbs * unitCost;

    const type = m.type as MovementType;
    const isInMonth = t.getTime() >= start.getTime() && t.getTime() < end.getTime();

    const inboundLoc = l.toLocationId ?? l.fromLocationId ?? defaultLocId ?? null;
    const outboundLoc = l.fromLocationId ?? l.toLocationId ?? defaultLocId ?? null;

    if (type === MovementType.ADJUST) {
      const loc = qtyRaw >= 0 ? inboundLoc : outboundLoc;
      if (!loc) continue;

      const key = `${l.itemId}::${loc}`;
      const cur = netMap.get(key) ?? { netQty: 0, netVal: 0 };

      if (qtyRaw >= 0) {
        cur.netQty += qtyAbs;
        cur.netVal += costAbs;

        if (isInMonth) {
          const curIn = inMonthMap.get(key) ?? { inQty: 0, inVal: 0 };
          curIn.inQty += qtyAbs;
          curIn.inVal += costAbs;
          inMonthMap.set(key, curIn);
        }
      } else {
        cur.netQty -= qtyAbs;
        cur.netVal -= costAbs;
      }

      netMap.set(key, cur);
      continue;
    }

    if (type === MovementType.IN) {
      const loc = inboundLoc;
      if (!loc) continue;

      const key = `${l.itemId}::${loc}`;
      const cur = netMap.get(key) ?? { netQty: 0, netVal: 0 };
      cur.netQty += qtyAbs;
      cur.netVal += costAbs;
      netMap.set(key, cur);

      if (isInMonth) {
        const curIn = inMonthMap.get(key) ?? { inQty: 0, inVal: 0 };
        curIn.inQty += qtyAbs;
        curIn.inVal += costAbs;
        inMonthMap.set(key, curIn);
      }
      continue;
    }

    if (type === MovementType.OUT) {
      const loc = outboundLoc;
      if (!loc) continue;

      const key = `${l.itemId}::${loc}`;
      const cur = netMap.get(key) ?? { netQty: 0, netVal: 0 };
      cur.netQty -= qtyAbs;
      cur.netVal -= costAbs;
      netMap.set(key, cur);
      continue;
    }

    if (type === MovementType.TRANSFER) {
      const fromLoc = outboundLoc;
      const toLoc = inboundLoc;

      if (fromLoc) {
        const keyOut = `${l.itemId}::${fromLoc}`;
        const curOut = netMap.get(keyOut) ?? { netQty: 0, netVal: 0 };
        curOut.netQty -= qtyAbs;
        curOut.netVal -= costAbs;
        netMap.set(keyOut, curOut);
      }

      if (toLoc) {
        const keyIn = `${l.itemId}::${toLoc}`;
        const curInNet = netMap.get(keyIn) ?? { netQty: 0, netVal: 0 };
        curInNet.netQty += qtyAbs;
        curInNet.netVal += costAbs;
        netMap.set(keyIn, curInNet);

        if (isInMonth) {
          const curIn = inMonthMap.get(keyIn) ?? { inQty: 0, inVal: 0 };
          curIn.inQty += qtyAbs;
          curIn.inVal += costAbs;
          inMonthMap.set(keyIn, curIn);
        }
      }
      continue;
    }
  }

  const keys = new Set<string>([...stockMap.keys(), ...netMap.keys(), ...inMonthMap.keys()]);

  await prisma.monthlyAvgCost.deleteMany({ where: { year, month } });

  const rows: Array<{
    year: number;
    month: number;
    itemId: string;
    locationId: string;
    avgCost: number;
    qtyTotal: number;
  }> = [];

  for (const key of keys) {
    const [itemId, locationId] = key.split("::");

    const stock = stockMap.get(key) ?? { qtyNow: 0, valNow: 0 };
    const net = netMap.get(key) ?? { netQty: 0, netVal: 0 };
    const inm = inMonthMap.get(key) ?? { inQty: 0, inVal: 0 };

    const beginQty = stock.qtyNow - net.netQty;
    const beginVal = stock.valNow - net.netVal;

    const qtyTotal = beginQty + inm.inQty;
    const costTotal = beginVal + inm.inVal;

    let avg = 0;
    if (qtyTotal > 0) avg = costTotal / qtyTotal;
    else if (beginQty > 0) avg = beginVal / beginQty;

    if (!Number.isFinite(avg)) avg = 0;

    if (Math.abs(qtyTotal) < 1e-9 && Math.abs(beginQty) < 1e-9 && Math.abs(stock.qtyNow) < 1e-9) {
      continue;
    }

    rows.push({ year, month, itemId, locationId, avgCost: avg, qtyTotal });
  }

  for (let i = 0; i < rows.length; i += CREATE_MANY_CHUNK) {
    const chunk = rows.slice(i, i + CREATE_MANY_CHUNK);
    await prisma.monthlyAvgCost.createMany({
      data: chunk.map((r) =>
        buildMonthlyAvgCostCreateData({
          year: r.year,
          month: r.month,
          itemId: r.itemId,
          locationId: r.locationId,
          avgCost: r.avgCost,
          qtyTotal: r.qtyTotal,
        })
      ),
    });
  }

  console.log(`📌 [AVG-COST] Built MonthlyAvgCost for ${year}-${pad2(month)} (${rows.length} keys)`);
  return { ok: true, didBuild: true, keys: rows.length };
}

/** ========================= AUTO LOCK + AVG COST ========================= **/

export async function autoLockPeriodOnce() {
  const now = new Date();

  // ✅ build cho tháng hiện tại
  if (PERIOD_AVGCOST_BUILD_CURRENT) {
    const cur = ymFromDateVN(now);
    await ensureMonthlyAvgCostForMonth(cur.year, cur.month, { force: true });
  }

  // ✅ build cho tháng trước
  const target = endOfPrevMonthVN(now);
  const prev = ymFromDateVN(target);
  await ensureMonthlyAvgCostForMonth(prev.year, prev.month);

  if (!PERIOD_AUTOLOCK_ENABLED) {
    return { ok: true, didLock: false, note: "PERIOD_AUTOLOCK_ENABLED=0" };
  }

  const closed = await getClosedUntilEOD();
  if (closed && closed.getTime() >= target.getTime()) {
    return { ok: true, didLock: false, closedUntil: closed };
  }

  const note = `AUTO LOCK kỳ đến hết ${fmtYMDVN(target)} (chốt tháng ${ymLabelVN(target)})`;

  const created = await prisma.periodLock.create({
    data: { closedUntil: target, note },
    select: { id: true, closedUntil: true, note: true, createdAt: true },
  });

  console.log(`🔒 [AUTO-LOCK] Locked period until ${fmtYMDVN(created.closedUntil)}`);
  return { ok: true, didLock: true, data: created };
}

export function startPeriodAutoLockJob() {
  autoLockPeriodOnce().catch((e) => console.error("[AUTO-LOCK] error:", e));

  setInterval(() => {
    autoLockPeriodOnce().catch((e) => console.error("[AUTO-LOCK] error:", e));
  }, CHECK_INTERVAL_MS);

  console.log(`🕒 [AUTO-LOCK] Job enabled (interval=${Math.round(CHECK_INTERVAL_MS / 60000)}m)`);
}
