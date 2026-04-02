import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

/**
 * ===============================
 * 🔥 LIST CUSTOMERS (CSKH)
 * ===============================
 */
export async function getCustomers(params: {
  userId?: string;
  onlyMine?: boolean;
  page?: number;
  pageSize?: number;
}) {
  const {
    userId,
    onlyMine = true,
    page = 1,
    pageSize = 20,
  } = params;

  const shouldFilter = onlyMine && userId;

  const customers = await prisma.partner.findMany({
    where: shouldFilter
      ? {
          invoices: {
            some: {
              saleUserId: userId,
              type: "SALES",
            },
          },
        }
      : {},

    orderBy: { updatedAt: "desc" },
    skip: (page - 1) * pageSize,
    take: pageSize,

    include: {
      invoices: {
        where: { type: "SALES" },
        orderBy: { issueDate: "desc" }, // 🔥 IMPORTANT
        select: {
          total: true,
          saleUserId: true,
          issueDate: true,
        },
      },

      customerActivities: {
        orderBy: { createdAt: "desc" },
        take: 1,
      },

      customerNotes: {
        orderBy: { createdAt: "desc" },
        take: 1,
      },
    },
  });

  return customers.map((c) => {
    const totalRevenue = c.invoices.reduce(
      (sum, inv) => sum + Number(inv.total ?? 0),
      0
    );

    const orderCount = c.invoices.length;

    const ownerId = c.invoices[0]?.saleUserId || null;

    const lastActivity = c.customerActivities?.[0] || null;
    const lastActivityAt = lastActivity?.createdAt || null;

    const lastNote = c.customerNotes?.[0]?.content || null;

    // 🔥 NEW: dùng invoice thay vì activity
    const lastInvoiceDate =
      c.invoices?.[0]?.issueDate || null;

    let daysSinceLastInvoice = 999;

    if (lastInvoiceDate) {
      const diffMs =
        Date.now() - new Date(lastInvoiceDate).getTime();
      daysSinceLastInvoice = diffMs / 86400000;
    }

    // 👉 rule mới
    const needCare = daysSinceLastInvoice > 30;

    return {
      id: c.id,
      name: c.name,
      phone: c.phone,
      email: c.email,
      taxCode: c.taxCode,
      address: c.address,

      ownerId,

      totalRevenue,
      orderCount,

      lastActivityAt,
      lastActivityType: lastActivity?.type || null,

      lastNote,

      // 🔥 NEW FIELDS
      lastInvoiceDate,
      daysSinceLastInvoice,

      needCare,
    };
  });
}

/**
 * ===============================
 * 🔥 DETAIL CUSTOMER
 * ===============================
 */
export async function getCustomerDetail(id: string) {
  const c = await prisma.partner.findUnique({
    where: { id },

    include: {
      invoices: {
        where: { type: "SALES" },
        orderBy: { issueDate: "desc" },

        include: {
          lines: {
            include: {
              item: true,
            },
          },
        },
      },

      customerActivities: {
        orderBy: { createdAt: "desc" },
      },

      customerNotes: {
        orderBy: { createdAt: "desc" },
      },
    },
  });

  if (!c) return null;

  return {
    id: c.id,
    name: c.name,
    phone: c.phone,
    email: c.email,
    address: c.address,
    taxCode: c.taxCode,

    invoices: c.invoices || [],
    customerActivities: c.customerActivities || [],
    customerNotes: c.customerNotes || [],
  };
}

/**
 * ===============================
 * 🔥 UPDATE CUSTOMER (NEW)
 * ===============================
 */
export async function updateCustomer(
  id: string,
  data: {
    phone?: string;
    email?: string;
    taxCode?: string;
    address?: string;
    name?: string;
  }
) {
  const updated = await prisma.partner.update({
    where: { id },

    data: {
      ...(data.phone !== undefined && { phone: data.phone }),
      ...(data.email !== undefined && { email: data.email }),
      ...(data.taxCode !== undefined && { taxCode: data.taxCode }),
      ...(data.address !== undefined && { address: data.address }),
      ...(data.name !== undefined && { name: data.name }),
    },
  });

  return updated;
}