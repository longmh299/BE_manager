import 'dotenv/config';

export const config = {
  port: Number(process.env.PORT || 4000),
  jwtSecret: process.env.JWT_SECRET || 'change_me',

  // ✅ Google Analytics 4 (GA4) — thống kê truy cập web thật
  ga: {
    // Property ID dạng số của GA4, ví dụ: 123456789 (KHÔNG phải Measurement ID "G-XXXX")
    propertyId: process.env.GA4_PROPERTY_ID || '',
    // Nội dung JSON của Service Account key (dán nguyên văn 1 dòng vào .env), dùng khi
    // KHÔNG set GOOGLE_APPLICATION_CREDENTIALS (đường dẫn file)
    credentialsJson: process.env.GA4_CREDENTIALS_JSON || '',
  },
};