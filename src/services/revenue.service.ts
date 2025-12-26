// src/services/revenue.service.ts
import { PrismaClient, Prisma, InvoiceStatus, InvoiceType } from "@prisma/client";

const prisma = new PrismaClient();

/** ---------------- helpers ---------------- **/
function toDate(s?: string) {
  if (!s) return null;
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d;
}

function n(x: any): number {
  if (x == null) return 0;
  if (typeof x === "number") return x;
  const v = Number(String(x));
  return Number.isFinite(v) ? v : 0;
}

function revenueSign(t: InvoiceType) {
  if (t === "SALES") return 1;
  if (t === "SALES_RETURN") return -1;
  return 0;
}

// ✅ Prisma-safe conditional SQL
const sqlEmpty = Prisma.sql``;
const sqlIf = (cond: any, frag: Prisma.Sql) => (cond ? frag : sqlEmpty);

// ✅ Cast enum param để Postgres không lỗi enum = text
const INV_STATUS_APPROVED = Prisma.sql`${InvoiceStatus.APPROVED}::"InvoiceStatus"`;
const INV_TYPE_SALES = Prisma.sql`${InvoiceType.SALES}::"InvoiceType"`;
const INV_TYPE_SALES_RETURN = Prisma.sql`${InvoiceType.SALES_RETURN}::"InvoiceType"`;

/** ---------------- types ---------------- **/
export type RevenueQuery = {
  from?: string;
  to?: string;
  groupBy?: "day" | "week" | "month";
  staffRole?: "SALE" | "TECH";
  staffUserId?: string;
  receiveAccountId?: string;
};

type StaffRow = {
  userId: string; // có thể là id thật hoặc "__NAME__:..."
  name: string;
  role: "SALE" | "TECH";

  /**
   * ✅ Doanh thu cá nhân theo dòng tiền, quy về NET (chưa VAT)
   * - NORMAL: quy đổi gross -> net theo tỷ lệ subtotalNet/total
   * - WARRANTY_HOLD: không cộng vào personalRevenue ngay, chỉ bonus khi NORMAL thu đủ "need"
   * => khi thu đủ need, nhân viên hưởng 100% giá trị hóa đơn (NET, không VAT)
   */
  personalRevenue: number;

  /** ✅ NET thu từ NORMAL (để hiển thị cùng hệ quy chiếu với doanh thu) */
  collectedNormal: number;

  /** ✅ BONUS NET (phần BH treo quy đổi net) */
  bonusWarranty: number;

  /** ✅ GROSS tiền thực thu (NORMAL + WARRANTY_HOLD, có thể gồm VAT) */
  collectedGross?: number;
};

/**
 * ✅ Fix quan trọng:
 * Một số dữ liệu bị lưu sai subtotal (bằng gross), dẫn tới net = gross.
 * Quy ước tính subtotalNet:
 * - Nếu có VAT và |(subtotal + tax) - total| lệch đáng kể -> subtotalNet = total - tax
 * - Else ưu tiên subtotal nếu > 0
 * - Nếu subtotal = 0 -> fallback SUM(InvoiceLine.amount)
 */
async function getStaffPersonalRevenue(params: {
  from?: Date;
  to?: Date;
  trunc: "day" | "week" | "month";
  staffRole: "SALE" | "TECH";
  staffUserId?: string;
  receiveAccountId?: string;
}) {
  const { from, to, staffRole, staffUserId, receiveAccountId } = params;

  if (staffRole === "SALE") {
    const rows: Array<{
      userId: string;
      name: string;
      personal: any; // NET
      collected_gross: any; // GROSS (NORMAL + HOLD)
      collected_net: any; // NET (NORMAL only)
      bonus: any; // NET
    }> = await prisma.$queryRaw`
      WITH inv AS (
        SELECT
          i."id",
          i."type",

          -- staffId có thể NULL (data cũ)
          i."saleUserId" AS "staffId",
          COALESCE(NULLIF(i."saleUserName", ''), NULLIF(u."username", ''), 'Unknown') AS "staffName",

          i."receiveAccountId",

          COALESCE(i."subtotal",0) AS subtotal_raw,
          COALESCE(i."tax",0)      AS tax_raw,
          COALESCE(i."total",0)    AS total,

          -- fallback subtotal theo dòng hàng
          (
            SELECT COALESCE(SUM(il."amount"),0)
            FROM "InvoiceLine" il
            WHERE il."invoiceId" = i."id"
          ) AS line_subtotal,

          COALESCE(i."warrantyHoldAmount",0) AS hold,
          (COALESCE(i."total",0) - COALESCE(i."warrantyHoldAmount",0)) AS need,

          -- ✅ subtotalNet (chưa VAT)
          (
            CASE
              WHEN COALESCE(i."total",0) > 0
                   AND COALESCE(i."tax",0) > 0
                   AND ABS((COALESCE(i."subtotal",0) + COALESCE(i."tax",0)) - COALESCE(i."total",0)) > 0.01
                THEN GREATEST(COALESCE(i."total",0) - COALESCE(i."tax",0), 0)
              WHEN COALESCE(i."subtotal",0) > 0
                THEN COALESCE(i."subtotal",0)
              ELSE (
                SELECT COALESCE(SUM(il."amount"),0)
                FROM "InvoiceLine" il
                WHERE il."invoiceId" = i."id"
              )
            END
          ) AS subtotal_net
        FROM "Invoice" i
        LEFT JOIN "User" u ON u."id" = i."saleUserId"
        WHERE
          i."status" = ${INV_STATUS_APPROVED}
          AND i."type" IN (${INV_TYPE_SALES}, ${INV_TYPE_SALES_RETURN})
          AND i."approvedAt" IS NOT NULL
          ${sqlIf(receiveAccountId, Prisma.sql`AND i."receiveAccountId" = ${receiveAccountId}`)}
          ${sqlIf(staffUserId, Prisma.sql`AND i."saleUserId" = ${staffUserId}`)}
      ),
      pay AS (
        SELECT
          pa."invoiceId",
          p."date" AS pay_date,
          p."id"   AS pay_id,
          pa."kind"::text AS kind,
          COALESCE(pa."amount",0) AS amt
        FROM "PaymentAllocation" pa
        JOIN "Payment" p ON p."id" = pa."paymentId"
        WHERE pa."kind"::text IN ('NORMAL','WARRANTY_HOLD')
      ),
      seq AS (
        SELECT
          inv."id" AS invoice_id,
          inv."type",

          inv."staffId",
          inv."staffName",

          inv.subtotal_net,
          inv.total,
          inv.hold,
          inv.need,

          pay.pay_date,
          pay.pay_id,
          pay.kind,
          pay.amt,

          -- ✅ chỉ cộng dồn NORMAL để xác định đủ need (không tính BH treo)
          SUM(
            CASE WHEN pay.kind = 'NORMAL' THEN pay.amt ELSE 0 END
          ) OVER (
            PARTITION BY inv."id"
            ORDER BY pay.pay_date, pay.pay_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS cum_amt_normal
        FROM inv
        JOIN pay ON pay."invoiceId" = inv."id"
      ),
      hit AS (
        SELECT
          s.invoice_id,
          MIN(s.pay_date) AS hit_date
        FROM seq s
        WHERE s.hold > 0 AND s.need > 0 AND s.cum_amt_normal >= s.need
        GROUP BY s.invoice_id
      ),
      cash AS (
        SELECT
          COALESCE(s."staffId", ('__NAME__:' || s."staffName")) AS userId,
          MAX(s."staffName") AS name,

          -- ✅ GROSS thực thu = NORMAL + WARRANTY_HOLD
          COALESCE(SUM(s.amt),0) AS collected_gross,

          -- ✅ NET thu từ NORMAL (loại VAT theo subtotalNet/total)
          COALESCE(SUM(
            CASE
              WHEN s.kind = 'NORMAL' AND COALESCE(s.total,0) > 0
                THEN (s.amt * (s.subtotal_net / NULLIF(s.total,0)))
              ELSE 0
            END
          ),0) AS collected_net
        FROM seq s
        WHERE 1=1
          ${sqlIf(from, Prisma.sql`AND s.pay_date >= ${from}`)}
          ${sqlIf(to, Prisma.sql`AND s.pay_date <= ${to}`)}
        GROUP BY COALESCE(s."staffId", ('__NAME__:' || s."staffName"))
      ),
      bonus AS (
        SELECT
          COALESCE(i."saleUserId", ('__NAME__:' || COALESCE(NULLIF(i."saleUserName", ''), 'Unknown'))) AS userId,
          MAX(COALESCE(NULLIF(i."saleUserName", ''), NULLIF(u."username", ''), 'Unknown')) AS name,

          -- ✅ BONUS NET = holdGross * (subtotalNet/total)
          COALESCE(SUM(
            CASE
              WHEN COALESCE(i."total",0) <= 0 THEN 0
              ELSE (
                -- subtotalNet same rule as above
                (
                  CASE
                    WHEN COALESCE(i."total",0) > 0
                         AND COALESCE(i."tax",0) > 0
                         AND ABS((COALESCE(i."subtotal",0) + COALESCE(i."tax",0)) - COALESCE(i."total",0)) > 0.01
                      THEN GREATEST(COALESCE(i."total",0) - COALESCE(i."tax",0), 0)
                    WHEN COALESCE(i."subtotal",0) > 0
                      THEN COALESCE(i."subtotal",0)
                    ELSE (
                      SELECT COALESCE(SUM(il."amount"),0)
                      FROM "InvoiceLine" il
                      WHERE il."invoiceId" = i."id"
                    )
                  END
                ) / NULLIF(COALESCE(i."total",0),0)
              ) * (
                CASE i."type"
                  WHEN 'SALES' THEN COALESCE(i."warrantyHoldAmount",0)
                  WHEN 'SALES_RETURN' THEN -COALESCE(i."warrantyHoldAmount",0)
                  ELSE 0
                END
              )
            END
          ),0) AS bonus_net
        FROM hit h
        JOIN "Invoice" i ON i."id" = h.invoice_id
        LEFT JOIN "User" u ON u."id" = i."saleUserId"
        WHERE 1=1
          ${sqlIf(from, Prisma.sql`AND h.hit_date >= ${from}`)}
          ${sqlIf(to, Prisma.sql`AND h.hit_date <= ${to}`)}
          ${sqlIf(receiveAccountId, Prisma.sql`AND i."receiveAccountId" = ${receiveAccountId}`)}
          ${sqlIf(staffUserId, Prisma.sql`AND i."saleUserId" = ${staffUserId}`)}
        GROUP BY COALESCE(i."saleUserId", ('__NAME__:' || COALESCE(NULLIF(i."saleUserName", ''), 'Unknown')))
      )
      SELECT
        COALESCE(c.userId, b.userId) AS "userId",
        COALESCE(c.name, b.name) AS name,
        COALESCE(c.collected_gross,0) AS collected_gross,
        COALESCE(c.collected_net,0) AS collected_net,
        COALESCE(b.bonus_net,0) AS bonus,
        (COALESCE(c.collected_net,0) + COALESCE(b.bonus_net,0)) AS personal
      FROM cash c
      FULL JOIN bonus b ON b.userId = c.userId
      ORDER BY personal DESC
      LIMIT 50
    `;

    const out: StaffRow[] = rows.map((r) => ({
      userId: String(r.userId),
      name: String(r.name || "Unknown"),
      role: "SALE",
      personalRevenue: n(r.personal), // NET
      collectedNormal: n(r.collected_net), // NET (NORMAL)
      bonusWarranty: n(r.bonus), // NET
      collectedGross: n(r.collected_gross), // GROSS (NORMAL + HOLD)
    }));

    return out;
  }

  // TECH
  const rows: Array<{
    userId: string;
    name: string;
    personal: any; // NET
    collected_gross: any; // GROSS (NORMAL + HOLD)
    collected_net: any; // NET (NORMAL)
    bonus: any; // NET
  }> = await prisma.$queryRaw`
    WITH inv AS (
      SELECT
        i."id",
        i."type",

        i."techUserId" AS "staffId",
        COALESCE(NULLIF(i."techUserName", ''), NULLIF(u."username", ''), 'Unknown') AS "staffName",

        i."receiveAccountId",

        COALESCE(i."subtotal",0) AS subtotal_raw,
        COALESCE(i."tax",0)      AS tax_raw,
        COALESCE(i."total",0)    AS total,

        (
          SELECT COALESCE(SUM(il."amount"),0)
          FROM "InvoiceLine" il
          WHERE il."invoiceId" = i."id"
        ) AS line_subtotal,

        COALESCE(i."warrantyHoldAmount",0) AS hold,
        (COALESCE(i."total",0) - COALESCE(i."warrantyHoldAmount",0)) AS need,

        (
          CASE
            WHEN COALESCE(i."total",0) > 0
                 AND COALESCE(i."tax",0) > 0
                 AND ABS((COALESCE(i."subtotal",0) + COALESCE(i."tax",0)) - COALESCE(i."total",0)) > 0.01
              THEN GREATEST(COALESCE(i."total",0) - COALESCE(i."tax",0), 0)
            WHEN COALESCE(i."subtotal",0) > 0
              THEN COALESCE(i."subtotal",0)
            ELSE (
              SELECT COALESCE(SUM(il."amount"),0)
              FROM "InvoiceLine" il
              WHERE il."invoiceId" = i."id"
            )
          END
        ) AS subtotal_net
      FROM "Invoice" i
      LEFT JOIN "User" u ON u."id" = i."techUserId"
      WHERE
        i."status" = ${INV_STATUS_APPROVED}
        AND i."type" IN (${INV_TYPE_SALES}, ${INV_TYPE_SALES_RETURN})
        AND i."approvedAt" IS NOT NULL
        ${sqlIf(receiveAccountId, Prisma.sql`AND i."receiveAccountId" = ${receiveAccountId}`)}
        ${sqlIf(staffUserId, Prisma.sql`AND i."techUserId" = ${staffUserId}`)}
    ),
    pay AS (
      SELECT
        pa."invoiceId",
        p."date" AS pay_date,
        p."id"   AS pay_id,
        pa."kind"::text AS kind,
        COALESCE(pa."amount",0) AS amt
      FROM "PaymentAllocation" pa
      JOIN "Payment" p ON p."id" = pa."paymentId"
      WHERE pa."kind"::text IN ('NORMAL','WARRANTY_HOLD')
    ),
    seq AS (
      SELECT
        inv."id" AS invoice_id,
        inv."type",

        inv."staffId",
        inv."staffName",

        inv.subtotal_net,
        inv.total,
        inv.hold,
        inv.need,

        pay.pay_date,
        pay.pay_id,
        pay.kind,
        pay.amt,

        SUM(
          CASE WHEN pay.kind = 'NORMAL' THEN pay.amt ELSE 0 END
        ) OVER (
          PARTITION BY inv."id"
          ORDER BY pay.pay_date, pay.pay_id
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_amt_normal
      FROM inv
      JOIN pay ON pay."invoiceId" = inv."id"
    ),
    hit AS (
      SELECT
        s.invoice_id,
        MIN(s.pay_date) AS hit_date
      FROM seq s
      WHERE s.hold > 0 AND s.need > 0 AND s.cum_amt_normal >= s.need
      GROUP BY s.invoice_id
    ),
    cash AS (
      SELECT
        COALESCE(s."staffId", ('__NAME__:' || s."staffName")) AS userId,
        MAX(s."staffName") AS name,

        COALESCE(SUM(s.amt),0) AS collected_gross,

        COALESCE(SUM(
          CASE
            WHEN s.kind = 'NORMAL' AND COALESCE(s.total,0) > 0
              THEN (s.amt * (s.subtotal_net / NULLIF(s.total,0)))
            ELSE 0
          END
        ),0) AS collected_net
      FROM seq s
      WHERE 1=1
        ${sqlIf(from, Prisma.sql`AND s.pay_date >= ${from}`)}
        ${sqlIf(to, Prisma.sql`AND s.pay_date <= ${to}`)}
      GROUP BY COALESCE(s."staffId", ('__NAME__:' || s."staffName"))
    ),
    bonus AS (
      SELECT
        COALESCE(i."techUserId", ('__NAME__:' || COALESCE(NULLIF(i."techUserName", ''), 'Unknown'))) AS userId,
        MAX(COALESCE(NULLIF(i."techUserName", ''), NULLIF(u."username", ''), 'Unknown')) AS name,

        COALESCE(SUM(
          CASE
            WHEN COALESCE(i."total",0) <= 0 THEN 0
            ELSE (
              (
                CASE
                  WHEN COALESCE(i."total",0) > 0
                       AND COALESCE(i."tax",0) > 0
                       AND ABS((COALESCE(i."subtotal",0) + COALESCE(i."tax",0)) - COALESCE(i."total",0)) > 0.01
                    THEN GREATEST(COALESCE(i."total",0) - COALESCE(i."tax",0), 0)
                  WHEN COALESCE(i."subtotal",0) > 0
                    THEN COALESCE(i."subtotal",0)
                  ELSE (
                    SELECT COALESCE(SUM(il."amount"),0)
                    FROM "InvoiceLine" il
                    WHERE il."invoiceId" = i."id"
                  )
                END
              ) / NULLIF(COALESCE(i."total",0),0)
            ) * (
              CASE i."type"
                WHEN 'SALES' THEN COALESCE(i."warrantyHoldAmount",0)
                WHEN 'SALES_RETURN' THEN -COALESCE(i."warrantyHoldAmount",0)
                ELSE 0
              END
            )
          END
        ),0) AS bonus_net
      FROM hit h
      JOIN "Invoice" i ON i."id" = h.invoice_id
      LEFT JOIN "User" u ON u."id" = i."techUserId"
      WHERE 1=1
        ${sqlIf(from, Prisma.sql`AND h.hit_date >= ${from}`)}
        ${sqlIf(to, Prisma.sql`AND h.hit_date <= ${to}`)}
        ${sqlIf(receiveAccountId, Prisma.sql`AND i."receiveAccountId" = ${receiveAccountId}`)}
        ${sqlIf(staffUserId, Prisma.sql`AND i."techUserId" = ${staffUserId}`)}
      GROUP BY COALESCE(i."techUserId", ('__NAME__:' || COALESCE(NULLIF(i."techUserName", ''), 'Unknown')))
    )
    SELECT
      COALESCE(c.userId, b.userId) AS "userId",
      COALESCE(c.name, b.name) AS name,
      COALESCE(c.collected_gross,0) AS collected_gross,
      COALESCE(c.collected_net,0) AS collected_net,
      COALESCE(b.bonus_net,0) AS bonus,
      (COALESCE(c.collected_net,0) + COALESCE(b.bonus_net,0)) AS personal
    FROM cash c
    FULL JOIN bonus b ON b.userId = c.userId
    ORDER BY personal DESC
    LIMIT 50
  `;

  const out: StaffRow[] = rows.map((r) => ({
    userId: String(r.userId),
    name: String(r.name || "Unknown"),
    role: "TECH",
    personalRevenue: n(r.personal), // NET
    collectedNormal: n(r.collected_net), // NET (NORMAL)
    bonusWarranty: n(r.bonus), // NET
    collectedGross: n(r.collected_gross), // GROSS (NORMAL + HOLD)
  }));

  return out;
}

export async function getRevenueDashboard(q: RevenueQuery) {
  const from = toDate(q.from) ?? undefined;
  const to = toDate(q.to) ?? undefined;

  const groupBy = q.groupBy ?? "day";
  const trunc = groupBy === "month" ? "month" : groupBy === "week" ? "week" : "day";

  /** =========================
   * KPI: invoice-level (company revenue) (APPROVED + approvedAt)
   * ========================= */
  const invWhere: Prisma.InvoiceWhereInput = {
    status: InvoiceStatus.APPROVED,
    type: { in: [InvoiceType.SALES, InvoiceType.SALES_RETURN] },
    approvedAt: { not: null },
  };

  if (from || to) {
    invWhere.approvedAt = { not: null };
    if (from) (invWhere.approvedAt as any).gte = from;
    if (to) (invWhere.approvedAt as any).lte = to;
  }

  if (q.receiveAccountId) invWhere.receiveAccountId = q.receiveAccountId;

  if (q.staffRole && q.staffUserId) {
    if (q.staffRole === "SALE") invWhere.saleUserId = q.staffUserId;
    if (q.staffRole === "TECH") invWhere.techUserId = q.staffUserId;
  }

  const invRows = await prisma.invoice.findMany({
    where: invWhere,
    select: {
      id: true,
      type: true,
      subtotal: true,
      tax: true,
      total: true,
      paidAmount: true, // NORMAL collected (gross)
    },
  });

  let netRevenue = 0; // subtotalNet signed (không VAT)
  let netVat = 0;
  let netTotal = 0;

  // ✅ netCollected: quy về NET để cùng hệ quy chiếu với netRevenue
  let netCollected = 0;

  for (const r of invRows) {
    const s = revenueSign(r.type);
    const subtotalRaw = n(r.subtotal);
    const tax = n(r.tax);
    const total = n(r.total);
    const paidGross = n(r.paidAmount);

    // ✅ Fix: nếu có VAT mà subtotalRaw bị “dính gross” -> dùng total - tax
    let subtotalNet = subtotalRaw;
    if (total > 0 && tax > 0 && Math.abs((subtotalRaw + tax) - total) > 0.01) {
      subtotalNet = Math.max(total - tax, 0);
    }

    netRevenue += s * subtotalNet;
    netVat += s * tax;
    netTotal += s * total;

    const paidNet = total > 0 ? paidGross * (subtotalNet / total) : 0;
    netCollected += s * paidNet;
  }

  const orderCount = invRows.length;

  /** =========================
   * KPI: Net COGS theo MovementLine (OUT +, IN -)
   * ========================= */
  const cogsAgg: Array<{ cogs: any }> = await prisma.$queryRaw`
    SELECT COALESCE(SUM(
      CASE m."type"
        WHEN 'OUT' THEN COALESCE(ml."costTotal",0)
        WHEN 'IN'  THEN -COALESCE(ml."costTotal",0)
        ELSE 0
      END
    ),0) AS cogs
    FROM "Invoice" i
    JOIN "Movement" m ON m."invoiceId" = i."id"
    JOIN "MovementLine" ml ON ml."movementId" = m."id"
    WHERE
      i."status" = ${INV_STATUS_APPROVED}
      AND i."type" IN (${INV_TYPE_SALES}, ${INV_TYPE_SALES_RETURN})
      AND i."approvedAt" IS NOT NULL
      ${sqlIf(from, Prisma.sql`AND i."approvedAt" >= ${from}`)}
      ${sqlIf(to, Prisma.sql`AND i."approvedAt" <= ${to}`)}
      ${sqlIf(q.receiveAccountId, Prisma.sql`AND i."receiveAccountId" = ${q.receiveAccountId}`)}
      ${sqlIf(q.staffRole === "SALE" && q.staffUserId, Prisma.sql`AND i."saleUserId" = ${q.staffUserId}`)}
      ${sqlIf(q.staffRole === "TECH" && q.staffUserId, Prisma.sql`AND i."techUserId" = ${q.staffUserId}`)}
  `;

  const netCogs = n(cogsAgg?.[0]?.cogs);
  const grossProfit = netRevenue - netCogs;
  const marginPct = netRevenue !== 0 ? (grossProfit / netRevenue) * 100 : 0;

  /** =========================
   * Trend: company revenue (approvedAt) + cogs
   * - revenue dùng subtotalNet (fix VAT lệch)
   * ========================= */
  const trend: Array<{ t: any; revenue: any; cogs: any }> = await prisma.$queryRaw`
    WITH inv AS (
      SELECT
        i."id",
        i."approvedAt",
        i."type",
        COALESCE(i."subtotal",0) AS subtotal_raw,
        COALESCE(i."tax",0)      AS tax_raw,
        COALESCE(i."total",0)    AS total,
        i."saleUserId",
        i."techUserId",
        i."receiveAccountId",
        (
          CASE
            WHEN COALESCE(i."total",0) > 0
                 AND COALESCE(i."tax",0) > 0
                 AND ABS((COALESCE(i."subtotal",0) + COALESCE(i."tax",0)) - COALESCE(i."total",0)) > 0.01
              THEN GREATEST(COALESCE(i."total",0) - COALESCE(i."tax",0), 0)
            ELSE COALESCE(i."subtotal",0)
          END
        ) AS subtotal_net
      FROM "Invoice" i
      WHERE
        i."status" = ${INV_STATUS_APPROVED}
        AND i."type" IN (${INV_TYPE_SALES}, ${INV_TYPE_SALES_RETURN})
        AND i."approvedAt" IS NOT NULL
        ${sqlIf(from, Prisma.sql`AND i."approvedAt" >= ${from}`)}
        ${sqlIf(to, Prisma.sql`AND i."approvedAt" <= ${to}`)}
        ${sqlIf(q.receiveAccountId, Prisma.sql`AND i."receiveAccountId" = ${q.receiveAccountId}`)}
        ${sqlIf(q.staffRole === "SALE" && q.staffUserId, Prisma.sql`AND i."saleUserId" = ${q.staffUserId}`)}
        ${sqlIf(q.staffRole === "TECH" && q.staffUserId, Prisma.sql`AND i."techUserId" = ${q.staffUserId}`)}
    ),
    rev AS (
      SELECT
        date_trunc(${trunc}, inv."approvedAt") AS t,
        COALESCE(SUM(
          CASE inv."type"
            WHEN 'SALES' THEN COALESCE(inv.subtotal_net,0)
            WHEN 'SALES_RETURN' THEN -COALESCE(inv.subtotal_net,0)
            ELSE 0
          END
        ),0) AS revenue
      FROM inv
      GROUP BY 1
    ),
    cogs AS (
      SELECT
        date_trunc(${trunc}, inv."approvedAt") AS t,
        COALESCE(SUM(
          CASE m."type"
            WHEN 'OUT' THEN COALESCE(ml."costTotal",0)
            WHEN 'IN'  THEN -COALESCE(ml."costTotal",0)
            ELSE 0
          END
        ),0) AS cogs
      FROM inv
      JOIN "Movement" m ON m."invoiceId" = inv."id"
      JOIN "MovementLine" ml ON ml."movementId" = m."id"
      GROUP BY 1
    )
    SELECT
      COALESCE(rev.t, cogs.t) AS t,
      COALESCE(rev.revenue, 0) AS revenue,
      COALESCE(cogs.cogs, 0) AS cogs
    FROM rev
    FULL JOIN cogs ON cogs.t = rev.t
    ORDER BY 1 ASC
  `;

  const trendOut = (trend || []).map((r: any) => {
    const revenue = n(r.revenue);
    const cogs = n(r.cogs);
    return {
      date: new Date(r.t).toISOString(),
      revenue,
      cogs,
      profit: revenue - cogs,
    };
  });

  /** =========================
   * By Product (company revenue) - giữ theo InvoiceLine.amount
   * ========================= */
  const byProduct: Array<{ itemId: string; name: string; revenue: any; cogs: any }> = await prisma.$queryRaw`
    WITH inv AS (
      SELECT
        i."id",
        i."approvedAt",
        i."type",
        i."saleUserId",
        i."techUserId",
        i."receiveAccountId"
      FROM "Invoice" i
      WHERE
        i."status" = ${INV_STATUS_APPROVED}
        AND i."type" IN (${INV_TYPE_SALES}, ${INV_TYPE_SALES_RETURN})
        AND i."approvedAt" IS NOT NULL
        ${sqlIf(from, Prisma.sql`AND i."approvedAt" >= ${from}`)}
        ${sqlIf(to, Prisma.sql`AND i."approvedAt" <= ${to}`)}
        ${sqlIf(q.receiveAccountId, Prisma.sql`AND i."receiveAccountId" = ${q.receiveAccountId}`)}
        ${sqlIf(q.staffRole === "SALE" && q.staffUserId, Prisma.sql`AND i."saleUserId" = ${q.staffUserId}`)}
        ${sqlIf(q.staffRole === "TECH" && q.staffUserId, Prisma.sql`AND i."techUserId" = ${q.staffUserId}`)}
    ),
    rev AS (
      SELECT
        il."itemId" AS "itemId",
        COALESCE(MAX(il."itemName"), '') AS name,
        COALESCE(SUM(
          CASE inv."type"
            WHEN 'SALES' THEN COALESCE(il."amount",0)
            WHEN 'SALES_RETURN' THEN -COALESCE(il."amount",0)
            ELSE 0
          END
        ),0) AS revenue
      FROM inv
      JOIN "InvoiceLine" il ON il."invoiceId" = inv."id"
      GROUP BY il."itemId"
    ),
    cogs AS (
      SELECT
        ml."itemId" AS "itemId",
        COALESCE(SUM(
          CASE m."type"
            WHEN 'OUT' THEN COALESCE(ml."costTotal",0)
            WHEN 'IN'  THEN -COALESCE(ml."costTotal",0)
            ELSE 0
          END
        ),0) AS cogs
      FROM inv
      JOIN "Movement" m ON m."invoiceId" = inv."id"
      JOIN "MovementLine" ml ON ml."movementId" = m."id"
      GROUP BY ml."itemId"
    )
    SELECT
      rev."itemId",
      rev.name,
      rev.revenue,
      COALESCE(cogs.cogs,0) AS cogs
    FROM rev
    LEFT JOIN cogs ON cogs."itemId" = rev."itemId"
    ORDER BY rev.revenue DESC
    LIMIT 50
  `;

  const byProductOut = (byProduct || []).map((r: any) => {
    const revenue = n(r.revenue);
    const cogs = n(r.cogs);
    const profit = revenue - cogs;
    return {
      itemId: r.itemId,
      name: r.name || "Unknown",
      revenue,
      cogs,
      profit,
      marginPct: revenue !== 0 ? (profit / revenue) * 100 : 0,
    };
  });

  /** =========================
   * By Staff: doanh thu cá nhân theo Payment.date + bonus hold khi đủ need (NET)
   * ========================= */
  const staffSalePersonal = await getStaffPersonalRevenue({
    from,
    to,
    trunc,
    staffRole: "SALE",
    staffUserId: q.staffRole === "SALE" ? q.staffUserId : undefined,
    receiveAccountId: q.receiveAccountId,
  });

  const staffTechPersonal = await getStaffPersonalRevenue({
    from,
    to,
    trunc,
    staffRole: "TECH",
    staffUserId: q.staffRole === "TECH" ? q.staffUserId : undefined,
    receiveAccountId: q.receiveAccountId,
  });

  const mapToLegacyStaffShape = (rows: StaffRow[]) =>
    rows.map((r) => ({
      userId: r.userId,
      name: r.name,
      role: r.role,
      revenue: r.personalRevenue, // ✅ NET (Doanh thu chưa VAT, đã cộng bonus BH nếu đủ need)
      collectedNormal: r.collectedNormal, // NET (NORMAL)
      collectedGross: r.collectedGross ?? 0, // ✅ GROSS thực thu (NORMAL + HOLD)
      bonusWarranty: r.bonusWarranty, // NET
      cogs: 0,
      profit: 0,
      marginPct: 0,
    }));

  return {
    kpis: {
      netRevenue,
      grossProfit,
      marginPct,
      orderCount,
      netVat,
      netTotal,
      netCollected,
      netCogs,
    },
    trend: trendOut,
    byProduct: byProductOut,
    byStaff: {
      sale: mapToLegacyStaffShape(staffSalePersonal),
      tech: mapToLegacyStaffShape(staffTechPersonal),
    },
  };
}
