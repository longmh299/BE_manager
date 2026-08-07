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
 * Thay "Email: ..." và "Mobile:"/"hot line:"/"Hotline:" trong file .docx bằng giá
 * trị mới. Trả về buffer đã patch + cờ báo có tìm thấy mẫu để thay không.
 * Nếu không có gì để thay (thiếu email/phone, hoặc file không khớp mẫu nào),
 * trả nguyên buffer gốc.
 *
 * ⚠️ Xử lý 2 tình huống thực tế đã gặp:
 * 1. Word tự nhận diện email là hyperlink -> nhãn "Email:" và địa chỉ email nằm ở
 *    2 khối XML tách biệt (do hyperlink tạo khối riêng) -> thử khớp CÙNG khối
 *    trước, không được thì thử khớp kiểu "nhảy qua các thẻ hyperlink" tới khối kế.
 * 2. Nhãn số điện thoại không đồng nhất giữa các file ("Mobile:" hay "hot line:"
 *    hay "Hotline:") -> chấp nhận nhiều biến thể nhãn, và LUÔN CHUẨN HOÁ nhãn hiển
 *    thị ra file kết quả thành "Mobile:" bất kể file gốc ghi kiểu gì. Nếu công ty
 *    dùng thêm cách ghi nào khác nữa (vd "SĐT:", "ĐT:"), cần bổ sung thêm vào
 *    PHONE_LABEL_PATTERN.
 */
const PHONE_LABEL_PATTERN = "Mobile|Hot\\s*Line";
// nhãn khác có thể xuất hiện gần đó trong cùng khối text, dùng để KHÔNG nuốt lố
// sang phần nhãn kế tiếp khi giá trị được phép chứa khoảng trắng (SĐT có dấu cách)
const ANY_LABEL_PATTERN = `Email|${PHONE_LABEL_PATTERN}`;

function sameRunPattern(labelPattern: string) {
  // Giá trị: mọi ký tự KHÔNG phải '<', cho phép có khoảng trắng bên trong (SĐT dạng
  // "0985 545757"), nhưng DỪNG LẠI nếu sắp gặp 1 nhãn khác trong cùng khối văn bản
  // (tránh nuốt nhầm nhãn kế tiếp — lỗi thực tế đã gặp khi Email/Mobile chung 1 dòng).
  return new RegExp(`(${labelPattern}\\s*:\\s*)((?:(?!${ANY_LABEL_PATTERN})[^<])*)`, "i");
}

// Trường hợp Word biến email thành hyperlink: "Email:" đóng khối ngay
// (</w:t></w:r>), sau đó là các thẻ <w:hyperlink>/<w:r>/<w:rPr>... rồi mới tới
// khối <w:t> thật sự chứa địa chỉ email — "nhảy qua" các thẻ đó để tới đúng chỗ.
const EMAIL_HYPERLINK_PATTERN =
  /(Email\s*:\s*<\/w:t>[\s\S]*?<w:t[^>]*>)([^<]*)(<\/w:t>)/i;

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

    if (email) {
      const re = sameRunPattern("Email");
      const m = re.exec(xml);
      if (m && m[2].trim()) {
        // ✅ tìm thấy ngay trong cùng khối, có giá trị thật để thay
        xml = xml.replace(re, (_full, label: string) => {
          replacedEmail = true;
          return `${label}${email}`;
        });
      } else if (EMAIL_HYPERLINK_PATTERN.test(xml)) {
        // ✅ Email bị tách khối do Word tự gắn hyperlink -> nhảy qua các thẻ để tới đúng chỗ
        xml = xml.replace(EMAIL_HYPERLINK_PATTERN, (_full, open: string, _old: string, close: string) => {
          replacedEmail = true;
          return `${open}${email}${close}`;
        });
      }
    }

    if (phone) {
      const re = sameRunPattern(PHONE_LABEL_PATTERN);
      const m = re.exec(xml);
      if (m && m[2].trim()) {
        // ✅ Ép nhãn hiển thị thành "Mobile:" luôn, bất kể file gốc ghi là
        // "hot line:", "Hotline:"... — theo đúng yêu cầu chuẩn hoá nhãn.
        xml = xml.replace(re, () => {
          replacedPhone = true;
          return `Mobile: ${phone}`;
        });
      }
    }

    if (xml !== before) zip.file(name, xml);
  }

  if (!replacedEmail && !replacedPhone) {
    return { buffer, replacedEmail: false, replacedPhone: false };
  }

  const out = await zip.generateAsync({ type: "nodebuffer" });
  return { buffer: out, replacedEmail, replacedPhone };
}