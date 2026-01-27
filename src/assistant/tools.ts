// src/assistant/tools.ts
import { prisma } from "../tool/prisma";

export type ItemKind = "MACHINE" | "PART";

export type SearchStockParams = {
  sku?: string;
  name?: string;
  locationCode?: string;
  onlyPositive?: boolean;
  kind?: ItemKind;
  limit?: number;
};

function normalizeSkuVariantsForTools(raw: string) {
  const s = raw.trim().replace(/\s+/g, "");
  const vars = new Set<string>();
  vars.add(s);
  vars.add(s.replace(/_/g, "-"));

  const m = s.match(/^([A-Za-z]+)(\d+)$/);
  if (m) vars.add(`${m[1]}-${m[2]}`);

  return Array.from(vars);
}

export async function searchStock(params: SearchStockParams) {
  const {
    sku,
    name,
    locationCode,
    onlyPositive = false,
    kind,
    limit = 50,
  } = params;

  const skuOr: any[] = [];
  if (sku) {
    for (const v of normalizeSkuVariantsForTools(sku)) {
      skuOr.push({ sku: { contains: v, mode: "insensitive" as const } });
    }
  }

  const rows = await prisma.stock.findMany({
    where: {
      ...(onlyPositive ? { qty: { gt: 0 } } : {}),
      ...(locationCode
        ? { location: { code: { equals: locationCode, mode: "insensitive" } } }
        : {}),
      item: {
        ...(skuOr.length ? { OR: skuOr } : {}),
        ...(name ? { name: { contains: name, mode: "insensitive" } } : {}),
        ...(kind ? { kind } : {}),
      },
    },
    select: {
      qty: true,
      avgCost: true,
      updatedAt: true,
      item: {
        select: {
          id: true,
          sku: true,
          name: true,
          kind: true,
          unit: { select: { code: true } },
        },
      },
      location: { select: { id: true, code: true, name: true } },
    },
    orderBy: [{ updatedAt: "desc" }],
    take: limit,
  });

  return rows.map((r) => ({
    itemId: r.item.id,
    sku: r.item.sku,
    name: r.item.name,
    kind: r.item.kind as ItemKind,
    unit: r.item.unit?.code || "",
    qty: r.qty?.toString?.() ?? String(r.qty),
    avgCost: r.avgCost?.toString?.() ?? String(r.avgCost),
    location: r.location,
    updatedAt: r.updatedAt?.toISOString?.() ?? String(r.updatedAt),
  }));
}

export type SearchInvoicesParams = {
  code?: string;
  partnerText?: string;
  from?: string; // yyyy-mm-dd
  to?: string; // yyyy-mm-dd
  limit?: number;
};

export async function searchInvoices(params: SearchInvoicesParams) {
  const { code, partnerText, from, to, limit = 50 } = params;

  const fromDate = from ? new Date(from + "T00:00:00.000Z") : undefined;
  const toDate = to ? new Date(to + "T23:59:59.999Z") : undefined;

  const rows = await prisma.invoice.findMany({
    where: {
      ...(code ? { code: { contains: code } } : {}),
      ...(partnerText
        ? {
            OR: [
              { partnerName: { contains: partnerText, mode: "insensitive" } },
              { partnerPhone: { contains: partnerText, mode: "insensitive" } },
              { partnerCode: { contains: partnerText, mode: "insensitive" } },
            ],
          }
        : {}),
      ...(fromDate || toDate
        ? {
            issueDate: {
              ...(fromDate ? { gte: fromDate } : {}),
              ...(toDate ? { lte: toDate } : {}),
            },
          }
        : {}),
    },
    select: {
      id: true,
      code: true,
      codeYear: true,
      type: true,
      issueDate: true,
      partnerName: true,
      total: true,
      status: true,
      paymentStatus: true,
      paidAmount: true,
    },
    orderBy: [{ issueDate: "desc" }],
    take: limit,
  });

  return rows.map((r) => ({
    ...r,
    issueDate: r.issueDate.toISOString(),
    total: r.total?.toString?.() ?? String(r.total),
    paidAmount: r.paidAmount?.toString?.() ?? String(r.paidAmount),
  }));
}
