// src/assistant/chatTools.ts
//
// Khai báo các "tool" mà Claude được phép gọi, và hàm dispatcher thực thi
// chúng bằng cách gọi lại đúng các service/hàm đã có sẵn trong repo.
//
// QUAN TRỌNG: Claude KHÔNG được tự tính số liệu — nó chỉ được lấy dữ liệu
// thật qua các tool này rồi diễn đạt lại bằng lời. Điều đó đảm bảo câu trả
// lời luôn khớp với dữ liệu thật trong DB.

import { prisma } from "../tool/prisma";
import { searchStock, searchInvoices, getItemSalesSummary, getStockCoverage, getNegativeStockItems, getCustomerInfo, getInvoiceDetail, getRecentSalePrices } from "./tools";
import { buildLowStockAlerts } from "../services/assistant/alerts/lowStock.service";

// ---------- 1. Tool schema (theo format Anthropic tool-use) ----------

export const chatToolDefinitions = [
  {
    name: "search_stock",
    description:
      "Tra cứu tồn kho theo SKU, tên sản phẩm, mã kho hoặc loại (máy/linh kiện). " +
      "Dùng khi người dùng hỏi 'còn hàng không', 'tồn kho bao nhiêu'. KHÔNG dùng tool " +
      "này cho câu hỏi về GIÁ — tool này không có giá bán, dùng get_item_sales.",
    input_schema: {
      type: "object",
      properties: {
        sku: { type: "string", description: "Mã SKU, ví dụ JL-660" },
        name: { type: "string", description: "Tên sản phẩm hoặc một phần tên" },
        locationCode: { type: "string", description: "Mã kho/vị trí" },
        onlyPositive: {
          type: "boolean",
          description: "Chỉ lấy các dòng còn tồn > 0",
        },
        kind: {
          type: "string",
          enum: ["MACHINE", "PART"],
          description: "Lọc theo loại: máy (MACHINE) hoặc linh kiện (PART)",
        },
        limit: { type: "number", description: "Số dòng tối đa, mặc định 50" },
      },
    },
  },
  {
    name: "search_invoices",
    description:
      "Tra cứu hóa đơn theo mã hóa đơn, tên/SĐT khách hàng, hoặc khoảng ngày. " +
      "Dùng khi người dùng hỏi về đơn hàng, doanh số theo ngày, lịch sử mua hàng của khách.",
    input_schema: {
      type: "object",
      properties: {
        code: { type: "string", description: "Mã hóa đơn hoặc một phần mã" },
        partnerText: {
          type: "string",
          description: "Tên, SĐT hoặc mã khách hàng/đối tác",
        },
        from: { type: "string", description: "Ngày bắt đầu, định dạng yyyy-mm-dd" },
        to: { type: "string", description: "Ngày kết thúc, định dạng yyyy-mm-dd" },
        limit: { type: "number", description: "Số dòng tối đa, mặc định 50" },
      },
    },
  },
  {
    name: "get_low_stock_alerts",
    description:
      "Lấy danh sách sản phẩm (máy/linh kiện) đang sắp hết hàng hoặc đã hết hàng, " +
      "kèm mức độ nghiêm trọng (CRITICAL/LOW) và số lượng gợi ý nhập thêm. " +
      "Dùng khi người dùng hỏi 'sản phẩm nào sắp hết', 'cần nhập gì', 'cảnh báo tồn kho'.",
    input_schema: {
      type: "object",
      properties: {
        kind: {
          type: "string",
          enum: ["MACHINE", "PART", "ALL"],
          description: "Lọc theo loại, mặc định ALL",
        },
      },
    },
  },
  {
    name: "get_item_sales",
    description:
      "Tính doanh thu và số lượng ĐÃ BÁN của MỘT sản phẩm cụ thể (theo tên hoặc SKU) " +
      "trong một khoảng thời gian. Dùng khi người dùng hỏi 'doanh thu của [sản phẩm] " +
      "trong [khoảng thời gian]', 'bán được bao nhiêu cái [sản phẩm]'. KHÔNG dùng tool " +
      "này cho câu hỏi về GIÁ — dùng get_recent_sale_prices. " +
      "KHÔNG dùng search_invoices cho câu hỏi kiểu này vì search_invoices chỉ lọc theo " +
      "mã hóa đơn hoặc tên/SĐT khách hàng, không lọc được theo sản phẩm.",
    input_schema: {
      type: "object",
      properties: {
        itemQuery: {
          type: "string",
          description: "Tên hoặc SKU sản phẩm cần tính doanh thu",
        },
        from: { type: "string", description: "Ngày bắt đầu, định dạng yyyy-mm-dd" },
        to: { type: "string", description: "Ngày kết thúc, định dạng yyyy-mm-dd" },
      },
      required: ["itemQuery"],
    },
  },
  {
    name: "get_stock_coverage",
    description:
      "Đánh giá tồn kho của MỘT sản phẩm là nhiều hay ít, dựa trên TỐC ĐỘ BÁN THẬT " +
      "(không phải số tuyệt đối). Dùng khi người dùng hỏi 'tồn X còn nhiều/ít không', " +
      "'X có đủ bán không', 'có cần nhập thêm X không'. BẮT BUỘC dùng tool này thay vì " +
      "tự đánh giá nhiều/ít bằng mắt thường qua con số tồn kho.",
    input_schema: {
      type: "object",
      properties: {
        itemQuery: {
          type: "string",
          description: "Tên hoặc SKU sản phẩm cần đánh giá",
        },
      },
      required: ["itemQuery"],
    },
  },
  {
    name: "get_negative_stock",
    description:
      "Liệt kê các sản phẩm đang có tồn kho ÂM (nhỏ hơn 0) — dấu hiệu lỗi dữ liệu " +
      "(bán vượt quá số lượng tồn thực có). Dùng khi người dùng hỏi 'có sản phẩm " +
      "nào tồn âm/âm kho/tồn < 0 không'. Đây là câu hỏi liệt kê theo điều kiện " +
      "trên toàn bộ danh mục, KHÔNG phải tra cứu 1 sản phẩm cụ thể — không dùng " +
      "search_stock cho câu hỏi kiểu này.",
    input_schema: {
      type: "object",
      properties: {
        kind: {
          type: "string",
          enum: ["MACHINE", "PART"],
          description: "Lọc theo loại, bỏ trống nếu muốn xem cả 2",
        },
      },
    },
  },
  {
    name: "get_customer_info",
    description:
      "Tra cứu 1 khách hàng theo tên/SĐT/mã — trả về tổng đã mua, còn nợ bao nhiêu, " +
      "các hóa đơn gần đây, và ghi chú CSKH gần đây (nếu có). Dùng khi người dùng " +
      "hỏi 'khách X đã mua gì', 'khách X còn nợ bao nhiêu', 'lịch sử mua hàng của X'.",
    input_schema: {
      type: "object",
      properties: {
        customerQuery: {
          type: "string",
          description: "Tên, SĐT hoặc mã khách hàng cần tra cứu",
        },
      },
      required: ["customerQuery"],
    },
  },
  {
    name: "get_invoice_detail",
    description:
      "Lấy chi tiết TỪNG DÒNG SẢN PHẨM bên trong 1 hóa đơn cụ thể, kèm GIÁ BÁN của " +
      "từng dòng (không phải chỉ tổng tiền cả hóa đơn). Dùng khi người dùng hỏi " +
      "'hóa đơn [mã] bán [sản phẩm] giá bao nhiêu', 'hóa đơn [mã] gồm những sản phẩm " +
      "gì, giá từng cái', hoặc bất kỳ câu hỏi nào cần xem GIÁ BÁN của 1 sản phẩm CỤ " +
      "THỂ trong 1 hóa đơn cụ thể. KHÔNG dùng search_invoices cho việc này vì tool đó " +
      "chỉ trả về tổng tiền cả hóa đơn, không có giá bán từng dòng sản phẩm.",
    input_schema: {
      type: "object",
      properties: {
        code: { type: "string", description: "Mã hóa đơn (hoặc một phần mã)" },
      },
      required: ["code"],
    },
  },
  {
    name: "get_recent_sale_prices",
    description:
      "Lấy danh sách CÁC LẦN BÁN GẦN NHẤT (giá, số lượng, ngày, mã hóa đơn) của MỘT " +
      "sản phẩm — dùng khi người dùng hỏi GIÁ BÁN của 1 sản phẩm KHÔNG kèm mã hóa đơn " +
      "cụ thể (vd 'giá bán [sản phẩm] bao nhiêu', 'sản phẩm X giá nhiêu'). Hệ thống " +
      "này KHÔNG có khái niệm giá niêm yết cố định — trả về NGUYÊN các lần bán thật " +
      "gần đây để người dùng tự so sánh, KHÔNG tự gộp/tính trung bình các mức giá đó " +
      "lại thành 1 con số duy nhất. Nếu chỉ có 1 lần bán, nói rõ đó là lần bán gần " +
      "nhất/duy nhất. Nếu mảng recentSales rỗng, nghĩa là sản phẩm chưa từng bán được " +
      "lần nào — nói rõ điều đó, không suy diễn giá.",
    input_schema: {
      type: "object",
      properties: {
        itemQuery: {
          type: "string",
          description: "Tên hoặc SKU sản phẩm cần tra giá",
        },
        limit: {
          type: "number",
          description: "Số lần bán gần nhất muốn xem, mặc định 5",
        },
      },
      required: ["itemQuery"],
    },
  },
] as const;

export type ChatToolName = (typeof chatToolDefinitions)[number]["name"];

// ---------- 2. Dispatcher: thực thi tool thật ----------

export async function executeChatTool(name: string, input: any, role?: string): Promise<any> {
  switch (name) {
    case "search_stock": {
      const rows = await searchStock({
        sku: input?.sku,
        name: input?.name,
        locationCode: input?.locationCode,
        onlyPositive: input?.onlyPositive,
        kind: input?.kind,
        limit: input?.limit,
      });

      // ✅ Nhân viên (staff) không được xem giá vốn — đây là dữ liệu nội bộ nhạy
      // cảm (lộ giá vốn = lộ biên lợi nhuận công ty). Ẩn field này TẠI ĐÂY (trước
      // khi dữ liệu đến tay Claude) để đảm bảo chắc chắn không lộ ra dù Claude có
      // vô tình nhắc lại hay không — không dựa vào việc "dặn AI đừng nói".
      if (role === "staff") {
        return rows.map((r: any) => {
          const { avgCost, ...rest } = r;
          return rest;
        });
      }
      return rows;
    }

    case "search_invoices": {
      return await searchInvoices({
        code: input?.code,
        partnerText: input?.partnerText,
        from: input?.from,
        to: input?.to,
        limit: input?.limit,
      });
    }

    case "get_low_stock_alerts": {
      const rows = await buildLowStockAlerts(prisma);
      const kind = input?.kind;
      if (kind && kind !== "ALL") {
        return rows.filter((r: any) => r.kind === kind);
      }
      return rows;
    }

    case "get_item_sales": {
      return await getItemSalesSummary({
        itemQuery: input?.itemQuery,
        from: input?.from,
        to: input?.to,
      });
    }

    case "get_stock_coverage": {
      return await getStockCoverage({ itemQuery: input?.itemQuery });
    }

    case "get_negative_stock": {
      return await getNegativeStockItems({ kind: input?.kind });
    }

    case "get_customer_info": {
      return await getCustomerInfo(input?.customerQuery);
    }

    case "get_invoice_detail": {
      return await getInvoiceDetail({ code: input?.code });
    }

    case "get_recent_sale_prices": {
      return await getRecentSalePrices({ itemQuery: input?.itemQuery, limit: input?.limit });
    }

    default:
      return { error: `Unknown tool: ${name}` };
  }
}