// src/bigintJsonPatch.ts
//
// ✅ FIX: Node.js JSON.stringify() không tự serialize được kiểu BigInt.
// Nhiều model trong schema dùng BigInt cho fileSize (MachineVideo, MachineImage,
// QuoteDocument...) để hỗ trợ file nặng >2GB. Khi Prisma trả object có field
// BigInt và Express gọi res.json() (tức JSON.stringify) trên đó, Node throw:
//   "TypeError: Do not know how to serialize a BigInt"
// -> route đó trả về lỗi 500 -> FE nuốt lỗi âm thầm -> người dùng thấy danh
// sách trống trơn như chưa từng có dữ liệu, dù DB và file trên storage vẫn còn.
//
// File này PHẢI được import đầu tiên (trước khi import "./app" hay bất kỳ
// route/service nào) để patch có hiệu lực trước khi bất kỳ JSON.stringify nào
// chạy. An toàn để convert sang Number vì dung lượng file (bytes) không bao
// giờ vượt Number.MAX_SAFE_INTEGER (~9 triệu TB).
(BigInt.prototype as any).toJSON = function (this: bigint) {
  return Number(this);
};