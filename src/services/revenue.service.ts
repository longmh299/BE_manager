/** src/services/revenue.service.ts **/
import { PrismaClient, Prisma, InvoiceStatus, InvoiceType } from "@prisma/client";

const prisma = new PrismaClient();

/** ---------------- helpers ---------------- **/

function parseDateLoose(s?: string) {
  if (!s) return null;
  const str = String(s).trim();

  // yyyy-mm-dd  -> parse LOCAL (tránh lệch TZ)
  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
    const [y, m, d] = str.split("-").map((x) => Number(x));
    const dt = new Date(y, (m || 1) - 1, d || 1, 0, 0, 0, 0);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }

  // mm/dd/yyyy hoặc m/d/yyyy -> parse LOCAL
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(str)) {
    const [mm, dd, yyyy] = str.split("/").map((x) => Number(x));
    const dt = new Date(yyyy, (mm || 1) - 1, dd || 1, 0, 0, 0, 0);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }

  const d = new Date(str);
  return Number.isNaN(d.getTime()) ? null : d;
}

function toDateStart(s?: string) {
  const str = String(s || "").trim();
  const d = parseDateLoose(str);
  if (!d) return null;

  // nếu là date-only thì set startOfDay
  if (/^\d{4}-\d{2}-\d{2}$/.test(str) || /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(str)) {
    return new Date(d.getFullYear(), d.getMonth(), d.getDate(), 0, 0, 0, 0);
  }
  return d;
}

function toDateEnd(s?: string) {
  const str = String(s || "").trim();
  const d = parseDateLoose(str);
  if (!d) return null;

  // nếu là date-only thì set endOfDay
  if (/^\d{4}-\d{2}-\d{2}$/.test(str) || /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(str)) {
    return new Date(d.getFullYear(), d.getMonth(), d.getDate(), 23, 59, 59, 999);
  }
  return d;
}

function n(x: any): number {
  if (x == null) return 0;
  if (typeof x === "number") return x;
  const v = Number(String(x));
  return Number.isFinite(v) ? v : 0;
}

function revenueSign(t: InvoiceType | string) {
  if (t === "SALES") return 1;
  if (t === "SALES_RETURN") return -1;
  return 0;
}

const sqlEmpty = Prisma.sql``;
const sqlIf = (cond: any, frag: Prisma.Sql) => (cond ? frag : sqlEmpty);

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

  includeStaffInvoices?: boolean;
};

type StaffRow = {
  userId: string;
  name: string;
  role: "SALE" | "TECH";

  personalRevenue: number; // NET
  collectedNormal: number; // NET (NORMAL)
  bonusWarranty: number; // NET
  collectedGross?: number; // GROSS
};

export type StaffInvoiceRow = {
  invoiceId: string;
  code: string;
  issueDate: string;
  partnerName: string;

  net: number;
  vat: number;
  gross: number;

  need: number;

  paidNormal: number; // GROSS NORMAL
  paidNormalGross: number; // alias

  dsDate: string;
  dsNet: number;
};

/** =========================
 * calcNetSafe
 * Fix case: subtotal=0, tax=0, sum(lines)=0 nhưng total>0 => NET = total
 * ========================= */
function calcNetSafe(params: { subtotalRaw: number; tax: number; total: number; lineNetSum: number }): number {
  const { subtotalRaw, tax, total, lineNetSum } = params;

  // trường hợp lệch: subtotal+tax != total => ưu tiên total-tax
  if (total > 0 && tax > 0 && Math.abs(subtotalRaw + tax - total) > 0.01) {
    return Math.max(total - tax, 0);
  }

  if (subtotalRaw > 0) return subtotalRaw;

  if (lineNetSum > 0) return lineNetSum;

  // ✅ fallback cuối: nếu có total thì lấy total-tax (tax có thể =0 => net=total)
  if (total > 0) return Math.max(total - tax, 0);

  return 0;
}

/** =========================================================
 * getStaffPersonalRevenue
 * ✅ tính doanh số theo ds_date (ngày đủ need) giống popup
 * ✅ FIX: SALES_RETURN phải trừ kể cả khi không có payment NORMAL
 *      - SALES: giữ logic ds_date = ngày đủ tiền
 *      - SALES_RETURN: ds_date = issueDate (không phụ thuộc payment)
 * ========================================================= */
async function getStaffPersonalRevenue(params: {
  from?: Date;
  to?: Date;
  trunc: "day" | "week" | "month";
  staffRole: "SALE" | "TECH";
  staffUserId?: string;
  receiveAccountId?: string;
}) {
  const { from, to, staffRole, staffUserId, receiveAccountId } = params;

  const isNameKey = String(staffUserId || "").startsWith("__NAME__:");
  const staffNameOnly = isNameKey ? String(staffUserId).slice("__NAME__:".length) : undefined;

  const staffIdField = staffRole === "SALE" ? Prisma.sql`i."saleUserId"` : Prisma.sql`i."techUserId"`;
  const staffNameField =
    staffRole === "SALE"
      ? Prisma.sql`COALESCE(NULLIF(i."saleUserName", ''), NULLIF(u."username", ''), 'Unknown')`
      : Prisma.sql`COALESCE(NULLIF(i."techUserName", ''), NULLIF(u."username", ''), 'Unknown')`;
  const staffJoinField = staffRole === "SALE" ? Prisma.sql`i."saleUserId"` : Prisma.sql`i."techUserId"`;

  const rows: Array<{ userId: string; name: string; personal: any }> = await prisma.$queryRaw`
    WITH inv AS (
      SELECT
        i."id" AS invoice_id,
        i."type",
        i."issueDate" AS issue_date,
        ${staffIdField} AS staff_id,
        ${staffNameField} AS staff_name,
        i."receiveAccountId",
        COALESCE(i."subtotal",0) AS subtotal_raw,
        COALESCE(i."tax",0)      AS vat,
        COALESCE(i."total",0)    AS gross,
        COALESCE(i."warrantyHoldAmount",0) AS hold,
        (
          CASE
            WHEN COALESCE(i."total",0) > 0
                 AND COALESCE(i."tax",0) > 0
                 AND ABS((COALESCE(i."subtotal",0) + COALESCE(i."tax",0)) - COALESCE(i."total",0)) > 0.01
              THEN GREATEST(COALESCE(i."total",0) - COALESCE(i."tax",0), 0)
            WHEN COALESCE(i."subtotal",0) > 0
              THEN COALESCE(i."subtotal",0)
            ELSE (
              SELECT COALESCE(NULLIF(SUM(il."amount"),0), COALESCE(i."total",0) - COALESCE(i."tax",0), 0)
              FROM "InvoiceLine" il
              WHERE il."invoiceId" = i."id"
            )
          END
        ) AS net,
        CASE
          WHEN COALESCE(i."warrantyHoldAmount",0) > 0
            THEN GREATEST(COALESCE(i."total",0) - COALESCE(i."warrantyHoldAmount",0), 0)
          ELSE COALESCE(i."total",0)
        END AS need
      FROM "Invoice" i
      LEFT JOIN "User" u ON u."id" = ${staffJoinField}
      WHERE
        i."status" = ${INV_STATUS_APPROVED}
        AND i."type" IN (${INV_TYPE_SALES}, ${INV_TYPE_SALES_RETURN})
        AND i."approvedAt" IS NOT NULL
        ${sqlIf(receiveAccountId, Prisma.sql`AND i."receiveAccountId" = ${receiveAccountId}`)}
        ${
          staffUserId
            ? isNameKey
              ? Prisma.sql`AND ${staffIdField} IS NULL AND ${staffNameField} = ${staffNameOnly}`
              : Prisma.sql`AND ${staffIdField} = ${staffUserId}`
            : sqlEmpty
        }
    ),
    pay AS (
      SELECT
        pa."invoiceId" AS invoice_id,
        p."date" AS pay_date,
        p."id"   AS pay_id,
        COALESCE(pa."amount",0) AS amt
      FROM "PaymentAllocation" pa
      JOIN "Payment" p ON p."id" = pa."paymentId"
      WHERE pa."kind"::text = 'NORMAL'
    ),
    seq AS (
      SELECT
        inv.invoice_id,
        inv."type",
        inv.staff_id,
        inv.staff_name,
        inv.net,
        inv.need,
        inv.issue_date,
        pay.pay_date,
        pay.pay_id,
        pay.amt,
        SUM(pay.amt) OVER (
          PARTITION BY inv.invoice_id
          ORDER BY pay.pay_date, pay.pay_id
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_amt
      FROM inv
      JOIN pay ON pay.invoice_id = inv.invoice_id
    ),
    ds AS (
      -- SALES: giữ logic cũ (ds_date = ngày đủ need theo NORMAL)
      SELECT
        s.invoice_id,
        MIN(s.pay_date) AS ds_date,
        MAX(s."type") AS type,
        MAX(s.staff_id) AS staff_id,
        MAX(s.staff_name) AS staff_name,
        MAX(s.net) AS net
      FROM seq s
      WHERE s.cum_amt >= s.need
        AND s."type" = 'SALES'
      GROUP BY s.invoice_id

      UNION ALL

      -- SALES_RETURN: trừ ngay theo issueDate (không phụ thuộc payment)
      SELECT
        inv.invoice_id,
        inv.issue_date AS ds_date,
        inv."type" AS type,
        inv.staff_id,
        inv.staff_name,
        inv.net
      FROM inv
      WHERE inv."type" = 'SALES_RETURN'
    )
    SELECT
      COALESCE(d.staff_id, ('__NAME__:' || d.staff_name)) AS "userId",
      MAX(d.staff_name) AS name,
      COALESCE(SUM(
        CASE d.type
          WHEN 'SALES' THEN COALESCE(d.net,0)
          WHEN 'SALES_RETURN' THEN -COALESCE(d.net,0)
          ELSE 0
        END
      ),0) AS personal
    FROM ds d
    WHERE 1=1
      ${sqlIf(from, Prisma.sql`AND d.ds_date >= ${from}`)}
      ${sqlIf(to, Prisma.sql`AND d.ds_date <= ${to}`)}
    GROUP BY COALESCE(d.staff_id, ('__NAME__:' || d.staff_name))
    ORDER BY personal DESC
    LIMIT 50
  `;

  return (rows || []).map((r) => ({
    userId: String(r.userId),
    name: String(r.name || "Unknown"),
    role: staffRole,
    personalRevenue: n(r.personal),
    collectedNormal: n(r.personal),
    bonusWarranty: 0,
    collectedGross: 0,
  }));
}

/** =========================================================
 * getStaffInvoices (popup)
 * ✅ FIX: include SALES_RETURN (ds_date = issueDate)
 * ========================================================= */
async function getStaffInvoices(params: {
  from?: Date;
  to?: Date;
  staffRole: "SALE" | "TECH";
  staffUserId: string;
  receiveAccountId?: string;
}) {
  const { from, to, staffRole, staffUserId, receiveAccountId } = params;

  const isNameKey = String(staffUserId || "").startsWith("__NAME__:");
  const staffNameOnly = isNameKey ? String(staffUserId).slice("__NAME__:".length) : undefined;

  const staffIdField = staffRole === "SALE" ? Prisma.sql`i."saleUserId"` : Prisma.sql`i."techUserId"`;
  const staffNameField =
    staffRole === "SALE"
      ? Prisma.sql`COALESCE(NULLIF(i."saleUserName", ''), NULLIF(u."username", ''), 'Unknown')`
      : Prisma.sql`COALESCE(NULLIF(i."techUserName", ''), NULLIF(u."username", ''), 'Unknown')`;
  const staffJoinField = staffRole === "SALE" ? Prisma.sql`i."saleUserId"` : Prisma.sql`i."techUserId"`;

  const rows: any[] = await prisma.$queryRaw`
    WITH inv AS (
      SELECT
        i."id" AS invoice_id,
        i."code" AS code,
        i."issueDate" AS issue_date,
        COALESCE(i."partnerName",'') AS partner_name,
        i."type",
        ${staffIdField} AS staff_id,
        ${staffNameField} AS staff_name,
        i."receiveAccountId",
        COALESCE(i."subtotal",0) AS subtotal_raw,
        COALESCE(i."tax",0)      AS vat,
        COALESCE(i."total",0)    AS gross,
        COALESCE(i."warrantyHoldAmount",0) AS hold,
        (
          CASE
            WHEN COALESCE(i."total",0) > 0
                 AND COALESCE(i."tax",0) > 0
                 AND ABS((COALESCE(i."subtotal",0) + COALESCE(i."tax",0)) - COALESCE(i."total",0)) > 0.01
              THEN GREATEST(COALESCE(i."total",0) - COALESCE(i."tax",0), 0)
            WHEN COALESCE(i."subtotal",0) > 0
              THEN COALESCE(i."subtotal",0)
            ELSE (
              SELECT COALESCE(NULLIF(SUM(il."amount"),0), COALESCE(i."total",0) - COALESCE(i."tax",0), 0)
              FROM "InvoiceLine" il
              WHERE il."invoiceId" = i."id"
            )
          END
        ) AS net,
        CASE
          WHEN COALESCE(i."warrantyHoldAmount",0) > 0 THEN GREATEST(COALESCE(i."total",0) - COALESCE(i."warrantyHoldAmount",0), 0)
          ELSE COALESCE(i."total",0)
        END AS need
      FROM "Invoice" i
      LEFT JOIN "User" u ON u."id" = ${staffJoinField}
      WHERE
        i."status" = ${INV_STATUS_APPROVED}
        AND i."type" IN (${INV_TYPE_SALES}, ${INV_TYPE_SALES_RETURN})
        AND i."approvedAt" IS NOT NULL
        ${sqlIf(receiveAccountId, Prisma.sql`AND i."receiveAccountId" = ${receiveAccountId}`)}
        ${
          staffUserId
            ? isNameKey
              ? Prisma.sql`AND ${staffIdField} IS NULL AND ${staffNameField} = ${staffNameOnly}`
              : Prisma.sql`AND ${staffIdField} = ${staffUserId}`
            : sqlEmpty
        }
    ),
    pay AS (
      SELECT
        pa."invoiceId" AS invoice_id,
        p."date" AS pay_date,
        p."id"   AS pay_id,
        COALESCE(pa."amount",0) AS amt
      FROM "PaymentAllocation" pa
      JOIN "Payment" p ON p."id" = pa."paymentId"
      WHERE pa."kind"::text = 'NORMAL'
    ),
    seq AS (
      SELECT
        inv.*,
        pay.pay_date,
        pay.pay_id,
        pay.amt,
        SUM(pay.amt) OVER (
          PARTITION BY inv.invoice_id
          ORDER BY pay.pay_date, pay.pay_id
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_amt
      FROM inv
      JOIN pay ON pay.invoice_id = inv.invoice_id
    ),
    ds AS (
      -- SALES: đủ need theo NORMAL
      SELECT
        s.invoice_id,
        MIN(s.pay_date) AS ds_date,
        MAX(s.code) AS code,
        MAX(s.issue_date) AS issue_date,
        MAX(s.partner_name) AS partner_name,
        MAX(s.type) AS type,
        MAX(s.net) AS net,
        MAX(s.vat) AS vat,
        MAX(s.gross) AS gross,
        MAX(s.need) AS need
      FROM seq s
      WHERE s.cum_amt >= s.need
        AND s.type = 'SALES'
      GROUP BY s.invoice_id

      UNION ALL

      -- SALES_RETURN: show ngay theo issueDate
      SELECT
        inv.invoice_id,
        inv.issue_date AS ds_date,
        inv.code,
        inv.issue_date,
        inv.partner_name,
        inv.type,
        inv.net,
        inv.vat,
        inv.gross,
        inv.need
      FROM inv
      WHERE inv.type = 'SALES_RETURN'
    ),
    paid AS (
      SELECT
        invoice_id,
        COALESCE(SUM(amt),0) AS paid_normal
      FROM pay
      GROUP BY invoice_id
    )
    SELECT
      d.invoice_id,
      d.code,
      d.issue_date,
      d.partner_name,
      d.net,
      d.vat,
      d.gross,
      d.need,
      COALESCE(p.paid_normal,0) AS paid_normal,
      d.ds_date,
      CASE d.type
        WHEN 'SALES' THEN d.net
        WHEN 'SALES_RETURN' THEN -d.net
        ELSE 0
      END AS ds_net
    FROM ds d
    LEFT JOIN paid p ON p.invoice_id = d.invoice_id
    WHERE 1=1
      ${sqlIf(from, Prisma.sql`AND d.ds_date >= ${from}`)}
      ${sqlIf(to, Prisma.sql`AND d.ds_date <= ${to}`)}
    ORDER BY d.ds_date DESC, d.code DESC
    LIMIT 300
  `;

  const out: StaffInvoiceRow[] = (rows || []).map((r: any) => {
    const paidNormal = n(r.paid_normal);
    return {
      invoiceId: String(r.invoice_id),
      code: String(r.code || ""),
      issueDate: r.issue_date ? new Date(r.issue_date).toISOString().slice(0, 10) : "",
      partnerName: String(r.partner_name || ""),
      net: n(r.net),
      vat: n(r.vat),
      gross: n(r.gross),
      need: n(r.need),
      paidNormal,
      paidNormalGross: paidNormal,
      dsDate: r.ds_date ? new Date(r.ds_date).toISOString().slice(0, 10) : "",
      dsNet: n(r.ds_net),
    };
  });

  return out;
}

export async function getRevenueDashboard(q: RevenueQuery) {
  const from = toDateStart(q.from) ?? undefined;
  const to = toDateEnd(q.to) ?? undefined;

  const groupBy = q.groupBy ?? "day";
  const trunc = groupBy === "month" ? "month" : groupBy === "week" ? "week" : "day";

  /** ========================= KPI company =========================
   * ✅ Lọc theo issueDate (ngày hoá đơn)
   * ✅ Trả thêm breakdown SALES vs SALES_RETURN
   * ✅ Trả grossCollected + alias để FE pick chắc chắn
   * ============================================================ */

  // Dùng SQL để lấy line sum luôn (tránh NET=0 khi subtotal=0, lines=0 nhưng total>0)
  type KpiInvRow = {
    id: string;
    type: any;
    subtotal_raw: any;
    tax: any;
    total: any;
    paid_gross: any;
    line_net_sum: any;
  };

  const invRows: KpiInvRow[] = await prisma.$queryRaw`
    SELECT
      i."id" AS id,
      i."type" AS type,
      COALESCE(i."subtotal",0) AS subtotal_raw,
      COALESCE(i."tax",0)      AS tax,
      COALESCE(i."total",0)    AS total,
      COALESCE(i."paidAmount",0) AS paid_gross,
      (
        SELECT COALESCE(SUM(il."amount"),0)
        FROM "InvoiceLine" il
        WHERE il."invoiceId" = i."id"
      ) AS line_net_sum
    FROM "Invoice" i
    WHERE
      i."status" = ${INV_STATUS_APPROVED}
      AND i."type" IN (${INV_TYPE_SALES}, ${INV_TYPE_SALES_RETURN})
      AND i."approvedAt" IS NOT NULL
      ${sqlIf(from, Prisma.sql`AND i."issueDate" >= ${from}`)}
      ${sqlIf(to, Prisma.sql`AND i."issueDate" <= ${to}`)}
      ${sqlIf(q.receiveAccountId, Prisma.sql`AND i."receiveAccountId" = ${q.receiveAccountId}`)}
      ${sqlIf(q.staffRole === "SALE" && q.staffUserId, Prisma.sql`AND i."saleUserId" = ${q.staffUserId}`)}
      ${sqlIf(q.staffRole === "TECH" && q.staffUserId, Prisma.sql`AND i."techUserId" = ${q.staffUserId}`)}
  `;

  // breakdown
  let salesNet = 0,
    salesVat = 0,
    salesGross = 0,
    salesCollectedNet = 0,
    salesCollectedGross = 0;

  let returnNet = 0,
    returnVat = 0,
    returnGross = 0,
    returnCollectedNet = 0,
    returnCollectedGross = 0;

  // tổng signed (để tương thích FE cũ)
  let netRevenue = 0;
  let netVat = 0;
  let netTotal = 0;
  let netCollected = 0;
  let grossCollected = 0;

  for (const r of invRows || []) {
    const type = String(r.type);
    const s = revenueSign(type);

    const subtotalRaw = n(r.subtotal_raw);
    const tax = n(r.tax);
    const total = n(r.total);
    const paidGross = n(r.paid_gross);
    const lineNetSum = n(r.line_net_sum);

    const subtotalNet = calcNetSafe({ subtotalRaw, tax, total, lineNetSum });

    // signed totals (cũ)
    netRevenue += s * subtotalNet;
    netVat += s * tax;
    netTotal += s * total;

    // collected gross signed
    grossCollected += s * paidGross;

    // collected net (quy đổi theo tỷ lệ net/total)
    const paidNet = total > 0 ? paidGross * (subtotalNet / total) : 0;
    netCollected += s * paidNet;

    // breakdown (luôn trả dương cho RETURN để FE show "Tổng tiền trả" dễ)
    if (type === "SALES") {
      salesNet += subtotalNet;
      salesVat += tax;
      salesGross += total;

      salesCollectedGross += paidGross;
      salesCollectedNet += paidNet;
    } else if (type === "SALES_RETURN") {
      returnNet += subtotalNet;
      returnVat += tax;
      returnGross += total;

      returnCollectedGross += paidGross;
      returnCollectedNet += paidNet;
    }
  }

  const orderCount = (invRows || []).length;

  /** ========================= COGS =========================
   * ✅ lọc theo issueDate
   * ======================================================== */
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
      ${sqlIf(from, Prisma.sql`AND i."issueDate" >= ${from}`)}
      ${sqlIf(to, Prisma.sql`AND i."issueDate" <= ${to}`)}
      ${sqlIf(q.receiveAccountId, Prisma.sql`AND i."receiveAccountId" = ${q.receiveAccountId}`)}
      ${sqlIf(q.staffRole === "SALE" && q.staffUserId, Prisma.sql`AND i."saleUserId" = ${q.staffUserId}`)}
      ${sqlIf(q.staffRole === "TECH" && q.staffUserId, Prisma.sql`AND i."techUserId" = ${q.staffUserId}`)}
  `;

  const netCogs = n(cogsAgg?.[0]?.cogs);
  const grossProfit = netRevenue - netCogs;
  const marginPct = netRevenue !== 0 ? (grossProfit / netRevenue) * 100 : 0;

  /** ========================= Trend =========================
   * ✅ trục thời gian = issueDate
   * ======================================================== */
  const trend: Array<{ t: any; revenue: any; cogs: any }> = await prisma.$queryRaw`
    WITH inv AS (
      SELECT
        i."id",
        i."issueDate" AS date_key,
        i."type",
        COALESCE(i."subtotal",0) AS subtotal_raw,
        COALESCE(i."tax",0)      AS tax_raw,
        COALESCE(i."total",0)    AS total,
        (
          CASE
            WHEN COALESCE(i."total",0) > 0
                 AND COALESCE(i."tax",0) > 0
                 AND ABS((COALESCE(i."subtotal",0) + COALESCE(i."tax",0)) - COALESCE(i."total",0)) > 0.01
              THEN GREATEST(COALESCE(i."total",0) - COALESCE(i."tax",0), 0)
            WHEN COALESCE(i."subtotal",0) > 0
              THEN COALESCE(i."subtotal",0)
            ELSE (
              SELECT COALESCE(NULLIF(SUM(il."amount"),0), COALESCE(i."total",0) - COALESCE(i."tax",0), 0)
              FROM "InvoiceLine" il
              WHERE il."invoiceId" = i."id"
            )
          END
        ) AS subtotal_net
      FROM "Invoice" i
      WHERE
        i."status" = ${INV_STATUS_APPROVED}
        AND i."type" IN (${INV_TYPE_SALES}, ${INV_TYPE_SALES_RETURN})
        AND i."approvedAt" IS NOT NULL
        ${sqlIf(from, Prisma.sql`AND i."issueDate" >= ${from}`)}
        ${sqlIf(to, Prisma.sql`AND i."issueDate" <= ${to}`)}
        ${sqlIf(q.receiveAccountId, Prisma.sql`AND i."receiveAccountId" = ${q.receiveAccountId}`)}
        ${sqlIf(q.staffRole === "SALE" && q.staffUserId, Prisma.sql`AND i."saleUserId" = ${q.staffUserId}`)}
        ${sqlIf(q.staffRole === "TECH" && q.staffUserId, Prisma.sql`AND i."techUserId" = ${q.staffUserId}`)}
    ),
    rev AS (
      SELECT
        date_trunc(${trunc}, inv.date_key) AS t,
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
        date_trunc(${trunc}, inv.date_key) AS t,
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

  /** ========================= By Product =========================
   * ✅ lọc theo issueDate + có qty signed
   * ============================================================= */
  const byProduct: Array<{ itemId: string; name: string; qty: any; revenue: any; cogs: any }> = await prisma.$queryRaw`
    WITH inv AS (
      SELECT
        i."id",
        i."issueDate" AS date_key,
        i."type",
        i."saleUserId",
        i."techUserId",
        i."receiveAccountId"
      FROM "Invoice" i
      WHERE
        i."status" = ${INV_STATUS_APPROVED}
        AND i."type" IN (${INV_TYPE_SALES}, ${INV_TYPE_SALES_RETURN})
        AND i."approvedAt" IS NOT NULL
        ${sqlIf(from, Prisma.sql`AND i."issueDate" >= ${from}`)}
        ${sqlIf(to, Prisma.sql`AND i."issueDate" <= ${to}`)}
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
            WHEN 'SALES' THEN COALESCE(il."qty",0)
            WHEN 'SALES_RETURN' THEN -COALESCE(il."qty",0)
            ELSE 0
          END
        ),0) AS qty,
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
      rev.qty,
      rev.revenue,
      COALESCE(cogs.cogs,0) AS cogs
    FROM rev
    LEFT JOIN cogs ON cogs."itemId" = rev."itemId"
    ORDER BY rev.revenue DESC
    LIMIT 50
  `;

  const byProductOut = (byProduct || []).map((r: any) => {
    const qty = n(r.qty);
    const revenue = n(r.revenue);
    const cogs = n(r.cogs);
    const profit = revenue - cogs;
    return {
      itemId: r.itemId,
      name: r.name || "Unknown",
      qty,
      revenue,
      cogs,
      profit,
      marginPct: revenue !== 0 ? (profit / revenue) * 100 : 0,
    };
  });

  /** ========================= By Staff ========================= */
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

  const filterRealStaff = (rows: StaffRow[]) =>
    rows.filter((r) => {
      const uid = String(r.userId || "");
      const name = String(r.name || "");
      if (!uid) return false;
      if (uid.startsWith("__NAME__:")) return false;
      if (name.trim().toLowerCase() === "unknown") return false;
      return true;
    });

  const mapToLegacyStaffShape = (rows: StaffRow[]) =>
    rows.map((r) => ({
      userId: r.userId,
      name: r.name,
      role: r.role,
      revenue: r.personalRevenue,
      collectedNormal: r.collectedNormal,
      collectedGross: r.collectedGross ?? 0,
      bonusWarranty: r.bonusWarranty,
      cogs: 0,
      profit: 0,
      marginPct: 0,
    }));

  let staffInvoices: StaffInvoiceRow[] | undefined = undefined;
  if (q.includeStaffInvoices && q.staffRole && q.staffUserId) {
    staffInvoices = await getStaffInvoices({
      from,
      to,
      staffRole: q.staffRole,
      staffUserId: q.staffUserId,
      receiveAccountId: q.receiveAccountId,
    });
  }

  return {
    kpis: {
      // tổng signed (FE đang dùng)
      netRevenue,
      grossProfit,
      marginPct,
      orderCount,
      netVat,
      netTotal,
      netCollected,
      netCogs,

      // ✅ grossCollected + alias keys để FE pick chắc chắn
      grossCollected,
      collectedGross: grossCollected,
      paidGross: grossCollected,
      paidTotal: grossCollected,
      totalCollected: grossCollected,

      // ✅ breakdown để FE show “Tổng tiền trả”
      salesNet,
      salesVat,
      salesGross,
      salesCollectedNet,
      salesCollectedGross,

      returnNet,
      returnVat,
      returnGross,
      returnCollectedNet,
      returnCollectedGross,
    },
    trend: trendOut,
    byProduct: byProductOut,
    byStaff: {
      sale: mapToLegacyStaffShape(filterRealStaff(staffSalePersonal)),
      tech: mapToLegacyStaffShape(filterRealStaff(staffTechPersonal)),
    },
    staffInvoices,
  };
}
