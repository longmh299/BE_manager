// src/services/assistant/chatService.ts
//
// Gọi Claude (Anthropic API) với tool-use để trả lời câu hỏi nghiệp vụ kho
// bằng dữ liệu THẬT lấy từ DB (qua chatTools.ts), không để AI tự bịa số liệu.

import { chatToolDefinitions, executeChatTool } from "../../assistant/chatTools";
import { tryFastAnswer } from "./fastAnswer";

const ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages";
// Haiku nhanh & rẻ hơn Sonnet nhiều lần — phù hợp cho việc tra cứu dữ liệu +
// điều phối tool đơn giản như thế này (không cần suy luận phức tạp của Sonnet).
const MODEL = "claude-haiku-4-5-20251001";
const MAX_TOOL_LOOPS = 4; // chặn vòng lặp vô hạn nếu model cứ gọi tool liên tục

export type ChatRole = "user" | "assistant";
export type ChatMessage = { role: ChatRole; content: string };

function buildSystemPrompt(role?: string): string {
  const today = new Date().toISOString().slice(0, 10); // yyyy-mm-dd theo giờ server

  const staffRestrictionRule =
    role === "staff"
      ? `
10. Người hỏi có vai trò NHÂN VIÊN (staff) — KHÔNG được tiết lộ giá vốn, chi phí
    nhập hàng, hay lợi nhuận/biên lợi nhuận dưới bất kỳ hình thức nào (kể cả suy ra
    gián tiếp từ doanh thu trừ chi phí). Field giá vốn đã được ẩn khỏi dữ liệu tool
    trả về cho vai trò này. Nếu người dùng cố hỏi giá vốn/lợi nhuận, trả lời ngắn
    gọn rằng thông tin này chỉ dành cho kế toán/quản lý, không cung cấp được.`
      : "";

  return `
Bạn là trợ lý quản lý kho cho một cửa hàng máy móc/linh kiện.
Hôm nay là ${today}. Đây là ngày CHÍNH XÁC — dùng trực tiếp để tự tính mọi khoảng
thời gian tương đối ("60 ngày qua", "2 tháng gần đây", "tuần này"...). TUYỆT ĐỐI
không hỏi lại người dùng ngày hiện tại là bao nhiêu, và không tự đoán/bịa ra một
ngày khác.
Quy tắc BẮT BUỘC:
1. CHỈ trả lời dựa trên dữ liệu lấy được từ tool. TUYỆT ĐỐI không tự suy đoán,
   không tự tính toán số liệu tồn kho/doanh thu/công nợ nếu tool không trả về.
2. Nếu không tìm thấy dữ liệu phù hợp, nói rõ "Không tìm thấy dữ liệu" thay vì đoán.
3. Trả lời NGẮN GỌN bằng tiếng Việt, tối đa 2-4 câu hoặc vài gạch đầu dòng nếu
   có nhiều mục. Không giải thích dài dòng, không lặp lại câu hỏi.
4. Khi có nhiều kết quả, chỉ liệt kê những mục liên quan nhất (tối đa ~10 dòng).
5. Số liệu (tồn kho, giá, số tiền) phải lấy nguyên từ tool, không làm tròn/sửa.
   TUYỆT ĐỐI không viết tắt số tiền theo dạng "tỷ"/"triệu" (vd "1.05 tỷ") — luôn
   viết đầy đủ số với dấu chấm phân cách hàng nghìn (vd "1.052.000.000đ"), vì việc
   quy đổi sang đơn vị rút gọn dễ tính sai khi tóm tắt nhiều số liệu cùng lúc.
6. Mã sản phẩm (SKU) trong hệ thống này không đồng nhất — nhiều sản phẩm khác nhau
   có thể trùng mã, nhiều sản phẩm không có mã. Nếu tool trả về NHIỀU sản phẩm khác
   tên cho cùng 1 mã/từ khóa, liệt kê TẤT CẢ kèm tên đầy đủ và hỏi lại người dùng
   muốn xem sản phẩm nào, KHÔNG tự chọn đại một sản phẩm.
7. Định dạng: bôi đậm (**...**) các con số quan trọng (số lượng tồn, số tiền, mức
   độ CRITICAL/LOW). Khi liệt kê nhiều sản phẩm, mỗi dòng bắt đầu bằng "- ".
8. Khi hỏi về doanh thu/số lượng bán của MỘT SẢN PHẨM cụ thể, dùng tool get_item_sales
   (KHÔNG dùng search_invoices — tool đó chỉ lọc theo mã hóa đơn hoặc khách hàng,
   không lọc được theo sản phẩm nên sẽ luôn cho kết quả sai/rỗng). Khi câu hỏi có
   khoảng thời gian tương đối, tự tính from/to (yyyy-mm-dd) dựa vào ngày hôm nay ở
   trên rồi truyền thẳng vào tool, không hỏi lại người dùng.
9. TUYỆT ĐỐI không tự nhận xét tồn kho "nhiều"/"ít"/"đủ dùng" dựa trên số tuyệt đối
   (vd 300 cái KHÔNG mặc định là nhiều — nếu tháng bán 1500 cái thì 300 là RẤT ÍT).
   Khi người dùng hỏi tồn nhiều/ít, có đủ bán không, có cần nhập thêm không — BẮT BUỘC
   gọi tool get_stock_coverage và dùng đúng verdict nó trả về (THAP/TRUNG_BINH/CAO/
   KHONG_DU_DU_LIEU/KHONG_BAN_GAN_DAY), không tự suy diễn thêm. Nếu tool trả về field
   "note", PHẢI nhắc lại nội dung đó trong câu trả lời (thường là cảnh báo quan trọng
   như "không còn hàng dự phòng" hoặc "số liệu quá ít để đánh giá chính xác").${staffRestrictionRule}
`.trim();
}

// Kiểu tối thiểu cho response của Anthropic Messages API, khai báo tường minh
// để không phụ thuộc vào việc TS suy luận đúng kiểu trả về của fetch/json().
// Dùng field optional thay vì discriminated union để tránh việc TS không
// "thu hẹp" được kiểu sau khi .filter() (filter không tự type-narrow trừ khi
// dùng type predicate riêng).
type AnthropicContentBlock = {
  type: string;
  text?: string;
  id?: string;
  name?: string;
  input?: any;
};

type AnthropicResponse = {
  content: AnthropicContentBlock[];
  stop_reason: string;
};

async function callClaude(messages: any[], role?: string): Promise<AnthropicResponse> {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    throw new Error("Thiếu ANTHROPIC_API_KEY trong biến môi trường");
  }

  const res = await fetch(ANTHROPIC_API_URL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: MODEL,
      max_tokens: 1024,
      system: buildSystemPrompt(role),
      tools: chatToolDefinitions,
      messages,
    }),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Anthropic API error ${res.status}: ${text}`);
  }

  const data = (await res.json()) as AnthropicResponse;
  return data;
}

/**
 * Chạy 1 lượt hỏi-đáp, có thể gồm nhiều vòng tool-use bên trong.
 * `history` là các lượt hội thoại trước đó (tuỳ chọn, để giữ ngữ cảnh).
 */
export async function runAssistantChat(params: {
  userMessage: string;
  history?: ChatMessage[];
  role?: string;
}): Promise<{ reply: string }> {
  const { userMessage, history = [], role } = params;

  // ---------- Fast path: câu hỏi đơn giản trả lời thẳng, không gọi Claude ----------
  // Luôn thử trước, KỂ CẢ khi đang có lịch sử hội thoại (câu hỏi tiếp nối) — vì
  // đây là số liệu tài chính/tồn kho, không nên để việc "đang hỏi tiếp hay không"
  // quyết định có tin tưởng số liệu hay không. parsePro tự phân loại độc lập theo
  // đúng nội dung câu hỏi hiện tại, không cần dựa vào ngữ cảnh trước đó để hoạt
  // động chính xác. Claude tự soạn câu trả lời tự do dễ tính sai/gán nhầm số khi
  // phải tóm tắt nhiều số liệu dài (đã từng xảy ra thật với câu hỏi doanh thu).
  //
  // ✅ Bọc try/catch: nếu fast-path lỗi bất ngờ (bug, dữ liệu lạ...), KHÔNG được
  // để lỗi thô văng ra người dùng — tự động rơi xuống cho Claude xử lý tiếp thay
  // vì làm sập cả request (đã từng xảy ra thật: "Cannot read properties of
  // undefined (reading 'score')" khi tra cứu khách hàng).
  try {
    const fast = await tryFastAnswer(userMessage);
    if (fast) return { reply: fast };
  } catch (err: any) {
    console.error("[FAST-PATH] Lỗi, rơi xuống Claude:", err?.message || err);
  }

  const messages: any[] = [
    ...history.map((m) => ({ role: m.role, content: m.content })),
    { role: "user", content: userMessage },
  ];

  for (let loop = 0; loop < MAX_TOOL_LOOPS; loop++) {
    const data = await callClaude(messages, role);

    const toolUseBlocks = (data.content || []).filter(
      (b: any) => b.type === "tool_use"
    );

    if (data.stop_reason !== "tool_use" || toolUseBlocks.length === 0) {
      // Model đã trả lời cuối cùng
      const textBlocks = (data.content || []).filter(
        (b: any) => b.type === "text"
      );
      const reply = textBlocks.map((b: any) => b.text).join("\n").trim();
      return { reply: reply || "Xin lỗi, hiện chưa có câu trả lời phù hợp." };
    }

    // Model muốn gọi tool -> thực thi thật rồi trả kết quả về cho model
    messages.push({ role: "assistant", content: data.content });

    const toolResults = [];
    for (const block of toolUseBlocks) {
      let result: any;
      try {
        result = await executeChatTool(block.name!, block.input, role);
      } catch (err: any) {
        result = { error: err?.message || "Tool execution failed" };
      }
      toolResults.push({
        type: "tool_result",
        tool_use_id: block.id,
        content: JSON.stringify(result),
      });
    }

    messages.push({ role: "user", content: toolResults });
  }

  return {
    reply: "Câu hỏi hơi phức tạp, bạn thử hỏi cụ thể hơn (ví dụ theo SKU hoặc khoảng ngày) nhé.",
  };
}