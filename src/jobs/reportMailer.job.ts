// src/jobs/reportMailer.job.ts
//
// Tự động gửi báo cáo qua email:
// - Ngày 1 mỗi tháng: báo cáo tháng trước (chi tiết bán hàng + xuất nhập tồn).
// - Ngày 1 mỗi quý (1/1, 1/4, 1/7, 1/10): báo cáo tổng hợp doanh thu quý trước.
//
// Theo đúng phong cách periodAutoLock.job.ts đã có: kiểm tra định kỳ bằng
// setInterval thay vì thêm thư viện cron mới, dùng file marker để tránh gửi
// trùng nếu server khởi động lại nhiều lần trong cùng 1 ngày.

import fs from "fs";
import path from "path";
import { sendMonthlyReport, sendQuarterlyReport } from "../services/reportMailer.service";

const CHECK_INTERVAL_MS = Number(process.env.REPORT_MAIL_CHECK_INTERVAL_MS || 60 * 60 * 1000); // 1h
const REPORT_MAIL_ENABLED = String(process.env.REPORT_MAIL_ENABLED ?? "1") !== "0";

const MARKER_FILE = path.join(process.cwd(), "data", "report-mail-state.json");

function readMarker(): { lastMonthSent?: string; lastQuarterSent?: string } {
  try {
    const raw = fs.readFileSync(MARKER_FILE, "utf-8");
    return JSON.parse(raw);
  } catch {
    return {};
  }
}

function writeMarker(state: { lastMonthSent?: string; lastQuarterSent?: string }) {
  try {
    fs.mkdirSync(path.dirname(MARKER_FILE), { recursive: true });
    fs.writeFileSync(MARKER_FILE, JSON.stringify(state, null, 2), "utf-8");
  } catch (e) {
    console.error("[REPORT-MAIL] Không ghi được marker file:", e);
  }
}

function pad2(n: number) {
  return String(n).padStart(2, "0");
}

function currentMonthKey(d: Date) {
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}`;
}

function currentQuarterKey(d: Date) {
  const q = Math.floor(d.getUTCMonth() / 3) + 1;
  return `${d.getUTCFullYear()}-Q${q}`;
}

function isQuarterStartMonth(d: Date) {
  return [0, 3, 6, 9].includes(d.getUTCMonth()); // tháng 1, 4, 7, 10 (0-based)
}

async function checkAndSendOnce() {
  const now = new Date();
  const isFirstDayOfMonth = now.getUTCDate() === 1;
  if (!isFirstDayOfMonth) return;

  const state = readMarker();
  const monthKey = currentMonthKey(now);
  const quarterKey = currentQuarterKey(now);

  if (state.lastMonthSent !== monthKey) {
    try {
      const r = await sendMonthlyReport(now);
      console.log(`📧 [REPORT-MAIL] Đã gửi báo cáo tháng ${r.label}`);
      state.lastMonthSent = monthKey;
      writeMarker(state);
    } catch (e) {
      console.error("[REPORT-MAIL] Lỗi gửi báo cáo tháng:", e);
    }
  }

  if (isQuarterStartMonth(now) && state.lastQuarterSent !== quarterKey) {
    try {
      const r = await sendQuarterlyReport(now);
      console.log(`📧 [REPORT-MAIL] Đã gửi báo cáo ${r.label}`);
      state.lastQuarterSent = quarterKey;
      writeMarker(state);
    } catch (e) {
      console.error("[REPORT-MAIL] Lỗi gửi báo cáo quý:", e);
    }
  }
}

export function startReportMailerJob() {
  if (!REPORT_MAIL_ENABLED) {
    console.log("🕒 [REPORT-MAIL] Job bị tắt (REPORT_MAIL_ENABLED=0)");
    return;
  }

  checkAndSendOnce().catch((e) => console.error("[REPORT-MAIL] error:", e));

  setInterval(() => {
    checkAndSendOnce().catch((e) => console.error("[REPORT-MAIL] error:", e));
  }, CHECK_INTERVAL_MS);

  console.log(`🕒 [REPORT-MAIL] Job enabled (kiểm tra mỗi ${Math.round(CHECK_INTERVAL_MS / 60000)} phút)`);
}

// Cho phép gọi thủ công để test ngay, không cần đợi đến ngày 1
export async function sendMonthlyReportNow() {
  return sendMonthlyReport(new Date());
}
export async function sendQuarterlyReportNow() {
  return sendQuarterlyReport(new Date());
}