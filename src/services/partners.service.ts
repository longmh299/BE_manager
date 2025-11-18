// src/services/reports.service.ts
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

export interface UserRevenueRow {
  userId: string;            // id:<userId> hoặc name:<normalizedName> hoặc unknown
  userName: string | null;   // tên hiển thị (snapshot hoặc username)
  invoiceCount: number;
  revenue: number;           // number (đã convert từ Decimal)
}

// Helpers chuẩn hoá ngày theo LOCAL time (server)
function startOfDay(d: Date) {
  return new Date(d.getFullYear(), d.getMonth(), d.getDate(), 0, 0, 0, 0);
}
function startOfNextDay(d: Date) {
  return new Date(d.getFullYear(), d.getMonth(), d.getDate() + 1, 0, 0, 0, 0);
}

// Chuẩn hoá tên: trim + lowercase để gom nhóm snapshot "Len", "len"
function normalizeName(name?: string | null) {
  const s = (name ?? '').trim();
  return s ? s.toLowerCase() : '';
}

/**
 * Gom nhóm doanh thu:
 * - Nếu có saleUserId/techUserId => key = id:<id>
 * - Nếu id null nhưng có snapshot name => key = name:<normalizedName>
 * - Nếu cả hai đều trống => key = unknown
 * nameForDisplay:
 *  - Ưu tiên snapshot (giữ nguyên chữ hoa/thường người nhập)
 *  - Nếu không có snapshot mà có id => lấy username từ bảng User (làm sau khi gom)
 */
function upsertRow(
  map: Map<string, UserRevenueRow>,
  key: string,
  nameForDisplay: string | null,
  amount: number
) {
  const cur = map.get(key) ?? {
    userId: key,
    userName: nameForDisplay,
    invoiceCount: 0,
    revenue: 0,
  };
  cur.invoiceCount += 1;
  cur.revenue += amount;
  // nếu trước đó chưa có tên hiển thị, set luôn
  if (!cur.userName && nameForDisplay) cur.userName = nameForDisplay;
  map.set(key, cur);
}

// ============ SALES BY SALE USER ============
export async function getSalesRevenueBySaleUser(
  from: Date,
  to: Date
): Promise<UserRevenueRow[]> {
  const from0 = startOfDay(from);
  const toExclusive = startOfNextDay(to);

  const invoices = await prisma.invoice.findMany({
    where: {
      type: 'SALES',
      issueDate: { gte: from0, lt: toExclusive },
      movements: { some: {} }, // đã post (có movement)
    },
    select: {
      saleUserId: true,
      saleUserName: true,
      total: true,
    },
  });

  const map = new Map<string, UserRevenueRow>();

  for (const inv of invoices) {
    const amount = Number(inv.total ?? 0);
    const id = inv.saleUserId;
    const snap = (inv.saleUserName ?? '').trim();

    if (id) {
      // Ưu tiên gộp theo ID
      upsertRow(map, `id:${id}`, snap || null, amount);
    } else {
      // Không có ID -> gộp theo snapshot name (nếu có)
      const norm = normalizeName(snap);
      if (norm) {
        upsertRow(map, `name:${norm}`, snap, amount);
      } else {
        // chẳng có gì -> nhóm unknown
        upsertRow(map, 'unknown', null, amount);
      }
    }
  }

  if (map.size === 0) return [];

  // Bổ sung username cho các nhóm theo ID nếu thiếu tên
  const idKeys = Array.from(map.keys()).filter((k) => k.startsWith('id:'));
  const userIds = idKeys.map((k) => k.slice(3));
  if (userIds.length) {
    const users = await prisma.user.findMany({
      where: { id: { in: userIds } },
      select: { id: true, username: true },
    });
    const userMap = new Map(users.map((u) => [u.id, u.username]));

    for (const k of idKeys) {
      const row = map.get(k)!;
      if (!row.userName) {
        const uid = k.slice(3);
        row.userName = userMap.get(uid) ?? row.userName ?? null;
        map.set(k, row);
      }
    }
  }

  // Đặt tên mặc định cho unknown
  if (map.has('unknown')) {
    const r = map.get('unknown')!;
    if (!r.userName) r.userName = '(Chưa có tên)';
    map.set('unknown', r);
  }

  return Array.from(map.values());
}

// ============ SALES BY TECH USER ============
export async function getSalesRevenueByTechUser(
  from: Date,
  to: Date
): Promise<UserRevenueRow[]> {
  const from0 = startOfDay(from);
  const toExclusive = startOfNextDay(to);

  const invoices = await prisma.invoice.findMany({
    where: {
      type: 'SALES',
      issueDate: { gte: from0, lt: toExclusive },
      movements: { some: {} },
    },
    select: {
      techUserId: true,
      techUserName: true,
      total: true,
    },
  });

  const map = new Map<string, UserRevenueRow>();

  for (const inv of invoices) {
    const amount = Number(inv.total ?? 0);
    const id = inv.techUserId;
    const snap = (inv.techUserName ?? '').trim();

    if (id) {
      upsertRow(map, `id:${id}`, snap || null, amount);
    } else {
      const norm = normalizeName(snap);
      if (norm) {
        upsertRow(map, `name:${norm}`, snap, amount);
      } else {
        upsertRow(map, 'unknown', null, amount);
      }
    }
  }

  if (map.size === 0) return [];

  const idKeys = Array.from(map.keys()).filter((k) => k.startsWith('id:'));
  const userIds = idKeys.map((k) => k.slice(3));
  if (userIds.length) {
    const users = await prisma.user.findMany({
      where: { id: { in: userIds } },
      select: { id: true, username: true },
    });
    const userMap = new Map(users.map((u) => [u.id, u.username]));
    for (const k of idKeys) {
      const row = map.get(k)!;
      if (!row.userName) {
        const uid = k.slice(3);
        row.userName = userMap.get(uid) ?? row.userName ?? null;
        map.set(k, row);
      }
    }
  }

  if (map.has('unknown')) {
    const r = map.get('unknown')!;
    if (!r.userName) r.userName = '(Chưa có tên)';
    map.set('unknown', r);
  }

  return Array.from(map.values());
}
