import { PrismaClient, InvoiceType } from "@prisma/client";

const prisma = new PrismaClient();

export interface RevenueUserStat {
  userId: string;
  username: string;
  fullName?: string | null;
  totalRevenue: number;   // doanh thu (tiền hàng, dùng subtotal)
  invoiceCount: number;
}

export interface RevenueProductStat {
  itemId: string;
  sku: string | null;
  name: string | null;
  qty: number;
  revenue: number;
}

export interface RevenueSummary {
  from: string; // yyyy-mm-dd
  to: string;   // yyyy-mm-dd
  currency: string;
  totalRevenue: number;   // tổng doanh thu (subtotal, không VAT)
  invoiceCount: number;
  bySaleUser: RevenueUserStat[];
  byTechUser: RevenueUserStat[];
  topProducts: RevenueProductStat[];
}

/**
 * Tính range mặc định là THÁNG HIỆN TẠI
 */
function getDefaultMonthRange() {
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth(), 1);
  const end = new Date(now.getFullYear(), now.getMonth() + 1, 0);
  return { start, end };
}

/**
 * Chuyển yyyy-mm-dd -> Date (00:00)
 */
function parseDateOnly(d: string): Date {
  const [y, m, day] = d.split("-").map((x) => Number(x));
  return new Date(y, (m || 1) - 1, day || 1);
}

/**
 * Thống kê doanh thu:
 *  - chỉ type = SALES
 *  - CHỈ hóa đơn:
 *      + đã thanh toán đủ (paymentStatus = PAID)
 *      + đã post tồn (có movement)
 *      + có bán máy (ít nhất 1 dòng sản phẩm là máy)
 *  - from/to: khoảng thời gian
 *  - nếu có userId => chỉ lấy HĐ mà user đó là sale hoặc tech
 *  - Doanh thu = subtotal (tiền hàng, không VAT)
 */
export async function getRevenueSummary(params: {
  from?: string;
  to?: string;
  userId?: string;
}) {
  let start: Date;
  let end: Date;

  if (params.from && params.to) {
    start = parseDateOnly(params.from);
    end = parseDateOnly(params.to);
  } else {
    const def = getDefaultMonthRange();
    start = def.start;
    end = def.end;
  }

  const endExclusive = new Date(end);
  endExclusive.setDate(endExclusive.getDate() + 1);

  // base filter theo loại + thời gian + trạng thái thanh toán + post tồn + có bán máy
  const baseWhere: any = {
    type: InvoiceType.SALES,
    paymentStatus: "PAID",          // chỉ HĐ đã thanh toán đủ
    issueDate: {
      gte: start,
      lt: endExclusive,
    },
    // chỉ HĐ đã post tồn (có ít nhất 1 movement)
    movements: { some: {} },
    // chỉ HĐ có ít nhất 1 dòng là MÁY
    lines: {
      some: {
        item: {
          kind: "MACHINE",         // nếu dùng enum ItemKind thì đổi thành ItemKind.MACHINE
        },
      },
    },
  };

  // nếu có userId => chỉ hóa đơn mà user này là sale hoặc tech
  let invoiceWhere: any = baseWhere;
  if (params.userId) {
    invoiceWhere = {
      ...baseWhere,
      OR: [{ saleUserId: params.userId }, { techUserId: params.userId }],
    };
  }

  // 1. Tổng doanh thu + số hóa đơn
  // ⚠️ Dùng subtotal thay vì total để loại VAT khỏi doanh thu
  const totalAgg = await prisma.invoice.aggregate({
    where: invoiceWhere,
    _sum: { subtotal: true },
    _count: { _all: true },
  });

  const totalRevenue =
    totalAgg._sum.subtotal !== null && totalAgg._sum.subtotal !== undefined
      ? Number(totalAgg._sum.subtotal.toString())
      : 0;
  const invoiceCount = totalAgg._count._all || 0;

  // 2. Doanh thu theo saleUser (subtotal)
  const saleGroups = await prisma.invoice.groupBy({
    by: ["saleUserId"],
    where: {
      ...invoiceWhere,
      saleUserId: { not: null },
    },
    _sum: { subtotal: true },
    _count: { _all: true },
  });

  // 3. Doanh thu theo techUser (subtotal)
  const techGroups = await prisma.invoice.groupBy({
    by: ["techUserId"],
    where: {
      ...invoiceWhere,
      techUserId: { not: null },
    },
    _sum: { subtotal: true },
    _count: { _all: true },
  });

  // Lấy danh sách userId để join tên
  const saleUserIds = saleGroups
    .map((g) => g.saleUserId)
    .filter((id): id is string => !!id);
  const techUserIds = techGroups
    .map((g) => g.techUserId)
    .filter((id): id is string => !!id);

  const allUserIds = Array.from(new Set([...saleUserIds, ...techUserIds]));

  const users = allUserIds.length
    ? await prisma.user.findMany({
        where: { id: { in: allUserIds } },
      })
    : [];

  const userMap = new Map<string, (typeof users)[number]>();
  users.forEach((u) => userMap.set(u.id, u));

  let bySaleUser: RevenueUserStat[] = saleGroups
    .filter((g) => g.saleUserId)
    .map((g) => {
      const u = g.saleUserId ? userMap.get(g.saleUserId) : undefined;
      const total =
        g._sum.subtotal !== null && g._sum.subtotal !== undefined
          ? Number(g._sum.subtotal.toString())
          : 0;
      return {
        userId: g.saleUserId!,
        username: u?.username || "(unknown)",
        fullName: (u as any)?.fullName || null,
        totalRevenue: total,
        invoiceCount: g._count._all,
      };
    })
    .sort((a, b) => b.totalRevenue - a.totalRevenue);

  let byTechUser: RevenueUserStat[] = techGroups
    .filter((g) => g.techUserId)
    .map((g) => {
      const u = g.techUserId ? userMap.get(g.techUserId) : undefined;
      const total =
        g._sum.subtotal !== null && g._sum.subtotal !== undefined
          ? Number(g._sum.subtotal.toString())
          : 0;
      return {
        userId: g.techUserId!,
        username: u?.username || "(unknown)",
        fullName: (u as any)?.fullName || null,
        totalRevenue: total,
        invoiceCount: g._count._all,
      };
    })
    .sort((a, b) => b.totalRevenue - a.totalRevenue);

  // 🔒 Nếu truyền userId (staff) => chỉ để lại đúng dòng của user đó
  if (params.userId) {
    bySaleUser = bySaleUser.filter((u) => u.userId === params.userId);
    byTechUser = byTechUser.filter((u) => u.userId === params.userId);
  }

  // 4. Top 10 sản phẩm theo doanh thu (InvoiceLine)
  //    Ở đây vẫn dùng amount (tiền hàng) nên đã khớp với subtotal
  const topItemGroups = await prisma.invoiceLine.groupBy({
    by: ["itemId"],
    where: {
      invoice: invoiceWhere,           // filter theo invoice (đã có PAID + post tồn + có máy)
      item: {
        kind: "MACHINE",               // chỉ sản phẩm là máy
      },
    },
    _sum: {
      amount: true,
      qty: true,
    },
    orderBy: {
      _sum: {
        amount: "desc",
      },
    },
    take: 10,
  });

  const itemIds = topItemGroups
    .map((g) => g.itemId)
    .filter((id): id is string => !!id);

  const items = itemIds.length
    ? await prisma.item.findMany({
        where: { id: { in: itemIds } },
      })
    : [];

  const itemMap = new Map<string, (typeof items)[number]>();
  items.forEach((it) => itemMap.set(it.id, it));

  const topProducts: RevenueProductStat[] = topItemGroups.map((g) => {
    const item = itemMap.get(g.itemId);
    const revenue =
      g._sum.amount !== null && g._sum.amount !== undefined
        ? Number(g._sum.amount.toString())
        : 0;
    const qty =
      g._sum.qty !== null && g._sum.qty !== undefined
        ? Number(g._sum.qty.toString())
        : 0;

    return {
      itemId: g.itemId,
      sku: item?.sku ?? null,
      name: item?.name ?? null,
      qty,
      revenue,
    };
  });

  const fromStr = start.toISOString().slice(0, 10);
  const toStr = end.toISOString().slice(0, 10);

  const summary: RevenueSummary = {
    from: fromStr,
    to: toStr,
    currency: "VND",
    totalRevenue,
    invoiceCount,
    bySaleUser,
    byTechUser,
    topProducts,
  };

  return summary;
}
