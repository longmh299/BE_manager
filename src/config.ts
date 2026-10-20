import 'dotenv/config';
import { randomBytes } from 'crypto';

// ✅ bảo mật: KHÔNG dùng khoá mặc định 'change_me' (ai biết là tự ký được token admin).
// Thiếu / yếu JWT_SECRET -> dùng khoá ngẫu nhiên cho lần chạy này (mọi người phải đăng nhập lại khi server khởi động lại)
// và báo lỗi to trong log để đặt JWT_SECRET trên Render.
function jwtSecret() {
  const s = (process.env.JWT_SECRET || '').trim();
  if (s.length >= 32 && s !== 'change_me') return s;
  console.error(
    '🚨 JWT_SECRET chưa đặt hoặc quá ngắn (cần >= 32 ký tự). Đang dùng khoá NGẪU NHIÊN tạm thời — ' +
      'mỗi lần server khởi động lại mọi người phải đăng nhập lại. Hãy đặt JWT_SECRET trên Render.'
  );
  return randomBytes(48).toString('hex');
}

export const config = {
  port: Number(process.env.PORT || 4000),
  jwtSecret: jwtSecret(),

  // ✅ Google Analytics 4 (GA4) — thống kê truy cập web thật
  ga: {
    // Property ID dạng số của GA4, ví dụ: 123456789 (KHÔNG phải Measurement ID "G-XXXX")
    propertyId: process.env.GA4_PROPERTY_ID || '',
    // Nội dung JSON của Service Account key (dán nguyên văn 1 dòng vào .env), dùng khi
    // KHÔNG set GOOGLE_APPLICATION_CREDENTIALS (đường dẫn file)
    credentialsJson: process.env.GA4_CREDENTIALS_JSON || '',
  },
};