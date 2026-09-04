// src/services/analytics.service.ts
// Gọi Google Analytics Data API (GA4) bằng Service Account để lấy thống kê
// lượt truy cập web thật cho domain đã gắn GA4.
import { BetaAnalyticsDataClient } from "@google-analytics/data";
import { config } from "../config";

function httpError(statusCode: number, message: string) {
  const err: any = new Error(message);
  err.statusCode = statusCode;
  return err;
}

let cachedClient: BetaAnalyticsDataClient | null = null;

function getClient(): BetaAnalyticsDataClient {
  if (cachedClient) return cachedClient;

  if (config.ga.credentialsJson) {
    let credentials: any;
    try {
      credentials = JSON.parse(config.ga.credentialsJson);
    } catch {
      throw httpError(
        500,
        "GA4_CREDENTIALS_JSON trong .env không phải JSON hợp lệ (dán nguyên văn nội dung file key.json vào, viết trên 1 dòng)."
      );
    }
    cachedClient = new BetaAnalyticsDataClient({ credentials });
  } else {
    // Fallback: dùng biến môi trường chuẩn của Google
    // GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
    cachedClient = new BetaAnalyticsDataClient();
  }

  return cachedClient;
}

function getPropertyPath(): string {
  const propertyId = config.ga.propertyId;
  if (!propertyId) {
    throw httpError(
      500,
      "Thiếu GA4_PROPERTY_ID trong .env — cần điền Property ID (dạng số) của Google Analytics 4."
    );
  }
  return `properties/${propertyId}`;
}

export type AnalyticsRange = {
  from: string; // "YYYY-MM-DD" hoặc từ khoá GA4 như "7daysAgo", "today"
  to: string;
};

function ymd(raw?: string | null): string {
  // GA4 trả date dạng "YYYYMMDD" -> convert "YYYY-MM-DD" cho FE dễ hiển thị
  if (!raw || raw.length !== 8) return raw || "";
  return `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}`;
}

function num(v: any): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function pctChange(current: number, previous: number): number | null {
  if (!previous) return current > 0 ? 100 : 0;
  return Math.round(((current - previous) / previous) * 1000) / 10; // 1 chữ số thập phân
}

/**
 * Tổng quan: tổng người dùng, phiên, lượt xem trang, thời gian trung bình, tỉ lệ tương tác...
 * So sánh với kỳ trước đó (cùng độ dài) nếu compare=true.
 */
async function fetchTotals(range: AnalyticsRange) {
  const client = getClient();
  const [resp] = await client.runReport({
    property: getPropertyPath(),
    dateRanges: [{ startDate: range.from, endDate: range.to }],
    metrics: [
      { name: "activeUsers" },
      { name: "newUsers" },
      { name: "sessions" },
      { name: "screenPageViews" },
      { name: "averageSessionDuration" },
      { name: "engagementRate" },
    ],
  });

  const row = resp.rows?.[0];
  const v = (i: number) => num(row?.metricValues?.[i]?.value);

  return {
    activeUsers: v(0),
    newUsers: v(1),
    sessions: v(2),
    screenPageViews: v(3),
    avgSessionDurationSec: v(4),
    engagementRate: v(5), // 0..1
  };
}

/** Số liệu theo ngày để vẽ biểu đồ xu hướng truy cập */
async function fetchTimeseries(range: AnalyticsRange) {
  const client = getClient();
  const [resp] = await client.runReport({
    property: getPropertyPath(),
    dateRanges: [{ startDate: range.from, endDate: range.to }],
    dimensions: [{ name: "date" }],
    metrics: [
      { name: "activeUsers" },
      { name: "sessions" },
      { name: "screenPageViews" },
    ],
    orderBys: [{ dimension: { dimensionName: "date" } }],
  });

  return (resp.rows || []).map((r) => ({
    date: ymd(r.dimensionValues?.[0]?.value),
    activeUsers: num(r.metricValues?.[0]?.value),
    sessions: num(r.metricValues?.[1]?.value),
    screenPageViews: num(r.metricValues?.[2]?.value),
  }));
}

/** Top trang được xem nhiều nhất */
async function fetchTopPages(range: AnalyticsRange, limit = 10) {
  const client = getClient();
  const [resp] = await client.runReport({
    property: getPropertyPath(),
    dateRanges: [{ startDate: range.from, endDate: range.to }],
    dimensions: [{ name: "pagePath" }, { name: "pageTitle" }],
    metrics: [{ name: "screenPageViews" }, { name: "activeUsers" }],
    orderBys: [
      { metric: { metricName: "screenPageViews" }, desc: true },
    ],
    limit,
  });

  return (resp.rows || []).map((r) => ({
    path: r.dimensionValues?.[0]?.value || "",
    title: r.dimensionValues?.[1]?.value || "",
    views: num(r.metricValues?.[0]?.value),
    users: num(r.metricValues?.[1]?.value),
  }));
}

/** Nguồn truy cập (organic, direct, social, referral...) */
async function fetchTrafficSources(range: AnalyticsRange) {
  const client = getClient();
  const [resp] = await client.runReport({
    property: getPropertyPath(),
    dateRanges: [{ startDate: range.from, endDate: range.to }],
    dimensions: [{ name: "sessionDefaultChannelGroup" }],
    metrics: [{ name: "sessions" }, { name: "activeUsers" }],
    orderBys: [{ metric: { metricName: "sessions" }, desc: true }],
  });

  return (resp.rows || []).map((r) => ({
    channel: r.dimensionValues?.[0]?.value || "(not set)",
    sessions: num(r.metricValues?.[0]?.value),
    users: num(r.metricValues?.[1]?.value),
  }));
}

/** Thiết bị: desktop / mobile / tablet */
async function fetchDevices(range: AnalyticsRange) {
  const client = getClient();
  const [resp] = await client.runReport({
    property: getPropertyPath(),
    dateRanges: [{ startDate: range.from, endDate: range.to }],
    dimensions: [{ name: "deviceCategory" }],
    metrics: [{ name: "activeUsers" }],
    orderBys: [{ metric: { metricName: "activeUsers" }, desc: true }],
  });

  return (resp.rows || []).map((r) => ({
    device: r.dimensionValues?.[0]?.value || "(not set)",
    users: num(r.metricValues?.[0]?.value),
  }));
}

/** Dịch từ ngày YYYY-MM-DD -> kỳ trước liền kề cùng độ dài (để so sánh %) */
function previousRangeOf(range: AnalyticsRange): AnalyticsRange | null {
  // Chỉ tính được khi from/to là ngày cụ thể (không phải "7daysAgo", "today"...)
  const isYmd = (s: string) => /^\d{4}-\d{2}-\d{2}$/.test(s);
  if (!isYmd(range.from) || !isYmd(range.to)) return null;

  const from = new Date(range.from + "T00:00:00");
  const to = new Date(range.to + "T00:00:00");
  const days = Math.round((to.getTime() - from.getTime()) / 86400000) + 1;

  const prevTo = new Date(from);
  prevTo.setDate(prevTo.getDate() - 1);
  const prevFrom = new Date(prevTo);
  prevFrom.setDate(prevFrom.getDate() - (days - 1));

  const fmt = (d: Date) =>
    `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(
      d.getDate()
    ).padStart(2, "0")}`;

  return { from: fmt(prevFrom), to: fmt(prevTo) };
}

export async function getAnalyticsOverview(opts: {
  from: string;
  to: string;
  compare?: boolean;
}) {
  const range: AnalyticsRange = { from: opts.from, to: opts.to };

  const [totals, timeseries, topPages, sources, devices] = await Promise.all([
    fetchTotals(range),
    fetchTimeseries(range),
    fetchTopPages(range, 10),
    fetchTrafficSources(range),
    fetchDevices(range),
  ]);

  let previousTotals: Awaited<ReturnType<typeof fetchTotals>> | null = null;
  let changes: Record<string, number | null> | null = null;

  if (opts.compare) {
    const prevRange = previousRangeOf(range);
    if (prevRange) {
      previousTotals = await fetchTotals(prevRange);
      changes = {
        activeUsers: pctChange(totals.activeUsers, previousTotals.activeUsers),
        sessions: pctChange(totals.sessions, previousTotals.sessions),
        screenPageViews: pctChange(
          totals.screenPageViews,
          previousTotals.screenPageViews
        ),
        engagementRate: pctChange(
          totals.engagementRate,
          previousTotals.engagementRate
        ),
      };
    }
  }

  return {
    range,
    totals,
    previousTotals,
    changes,
    timeseries,
    topPages,
    sources,
    devices,
  };
}