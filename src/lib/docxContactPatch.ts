// src/lib/docxContactPatch.ts
// ✅ Tự động thay email/SĐT trong file .docx báo giá theo đúng người đang tải
// xuống, KHÔNG đụng gì khác trong file (layout, ảnh, bảng biểu giữ nguyên).
//
// Cách hoạt động: .docx thực chất là 1 file ZIP chứa các file XML. Header/footer
// của file MCBROTHER đang có sẵn dòng "Email: xxx" và "Mobile: xxx" (thông tin
// của người soạn/sửa file lần cuối). Ta chỉ tìm đúng 2 mẫu "Email:" / "Mobile:"
// trong các file XML liên quan, thay phần giá trị ngay sau đó — không sửa gì
// khác trong file.
//
// CHỈ áp dụng cho file .docx (định dạng ZIP/XML, sửa trực tiếp được, an toàn).
// File .doc cũ (định dạng nhị phân OLE) hoặc .pdf sẽ được bỏ qua, trả về file
// gốc y nguyên — sửa trực tiếp bytes nhị phân .doc dễ làm hỏng file.
import JSZip from "jszip";

const DOCX_MIME =
  "application/vnd.openxmlformats-officedocument.wordprocessingml.document";

export function isDocx(mimeType: string, fileName: string): boolean {
  return mimeType === DOCX_MIME || /\.docx$/i.test(fileName);
}

/**
 * Thay "Email: ..." và "Mobile: ..." trong file .docx bằng giá trị mới.
 * Trả về buffer đã patch + cờ báo có tìm thấy mẫu để thay không (để biết
 * file này có đang dùng đúng format công ty hay không, phục vụ audit log).
 * Nếu không có gì để thay (thiếu email/phone, hoặc file không khớp mẫu),
 * trả nguyên buffer gốc.
 */
export async function patchDocxContact(
  buffer: Buffer,
  contact: { email?: string | null; phone?: string | null }
): Promise<{ buffer: Buffer; replacedEmail: boolean; replacedPhone: boolean }> {
  const email = contact.email?.trim();
  const phone = contact.phone?.trim();

  if (!email && !phone) {
    return { buffer, replacedEmail: false, replacedPhone: false };
  }

  const zip = await JSZip.loadAsync(buffer);

  // header có thể là header1/2/3.xml (trang đầu/trang chẵn khác nhau);
  // thử luôn document.xml + footer phòng trường hợp thông tin nằm chỗ khác.
  const headerFiles = Object.keys(zip.files).filter((name) =>
    /^word\/header\d*\.xml$/.test(name)
  );
  const footerFiles = Object.keys(zip.files).filter((name) =>
    /^word\/footer\d*\.xml$/.test(name)
  );
  const candidateFiles = [...headerFiles, "word/document.xml", ...footerFiles].filter(
    (name) => !!zip.files[name]
  );

  let replacedEmail = false;
  let replacedPhone = false;

  for (const name of candidateFiles) {
    let xml = await zip.files[name].async("string");
    const before = xml;

    // ⚠️ Dừng đúng ở khoảng trắng/dấu "<" gần nhất — KHÔNG dùng [^<]* tham lam,
    // vì "Email:" và "Mobile:" thường nằm chung 1 khối text trong file gốc;
    // tham lam sẽ nuốt luôn phần Mobile khi thay Email (đã kiểm chứng thực tế).
    if (email) {
      const re = /(Email:\s*)([^\s<]+)/i;
      if (re.test(xml)) {
        xml = xml.replace(re, (_m, label: string) => {
          replacedEmail = true;
          return `${label}${email}`;
        });
      }
    }
    if (phone) {
      const re = /(Mobile:\s*)([^\s<]+)/i;
      if (re.test(xml)) {
        xml = xml.replace(re, (_m, label: string) => {
          replacedPhone = true;
          return `${label}${phone}`;
        });
      }
    }

    if (xml !== before) zip.file(name, xml);
  }

  if (!replacedEmail && !replacedPhone) {
    // không tìm thấy mẫu nào để thay -> trả nguyên file gốc, tránh sinh ra file
    // đã "generate lại" không cần thiết (dù nội dung như cũ, đóng gói lại ZIP
    // có thể lệch byte-for-byte so với bản gốc)
    return { buffer, replacedEmail: false, replacedPhone: false };
  }

  const out = await zip.generateAsync({ type: "nodebuffer" });
  return { buffer: out, replacedEmail, replacedPhone };
}