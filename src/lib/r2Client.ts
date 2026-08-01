// src/lib/r2Client.ts
import { S3Client, DeleteObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";

// ✅ Cần set 4 biến môi trường này trên Render (Settings -> Environment):
//   R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID || "";
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID || "";
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY || "";
export const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME || "";

export const r2 = new S3Client({
  region: "auto",
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
});

/** URL để trình duyệt PUT thẳng file lên R2 (không đi qua backend -> không lo giới hạn RAM/timeout của Render) */
export async function getUploadUrl(key: string, contentType: string, expiresInSeconds = 3600) {
  const cmd = new PutObjectCommand({
    Bucket: R2_BUCKET_NAME,
    Key: key,
    ContentType: contentType,
  });
  return getSignedUrl(r2, cmd, { expiresIn: expiresInSeconds });
}

/** URL để trình duyệt tải file trực tiếp từ R2 (không qua backend -> không lo RAM khi file vài GB) */
export async function getDownloadUrl(key: string, fileName: string, expiresInSeconds = 600) {
  const cmd = new GetObjectCommand({
    Bucket: R2_BUCKET_NAME,
    Key: key,
    ResponseContentDisposition: `attachment; filename*=UTF-8''${encodeURIComponent(fileName)}`,
  });
  return getSignedUrl(r2, cmd, { expiresIn: expiresInSeconds });
}

/** ✅ URL để xem trước video ngay trong trang (thẻ <video>) — KHÔNG ép tải xuống như getDownloadUrl,
 * để trình duyệt phát trực tiếp (stream có hỗ trợ tua nhờ Range request chuẩn S3). */
export async function getPreviewUrl(key: string, expiresInSeconds = 600) {
  const cmd = new GetObjectCommand({
    Bucket: R2_BUCKET_NAME,
    Key: key,
    ResponseContentDisposition: "inline",
  });
  return getSignedUrl(r2, cmd, { expiresIn: expiresInSeconds });
}

export async function deleteObject(key: string) {
  await r2.send(new DeleteObjectCommand({ Bucket: R2_BUCKET_NAME, Key: key }));
}

/** Kiểm tra file đã thật sự upload xong lên R2 chưa (dùng khi FE báo "complete") */
export async function objectExists(key: string): Promise<boolean> {
  try {
    await r2.send(new HeadObjectCommand({ Bucket: R2_BUCKET_NAME, Key: key }));
    return true;
  } catch {
    return false;
  }
}