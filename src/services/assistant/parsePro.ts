export type Intent =
  | "GET_STOCK"              // hỏi tồn
  | "LOW_STOCK"              // sắp hết
  | "OUT_OF_STOCK"           // hết hàng
  | "NEGATIVE_STOCK"         // tồn âm (< 0) — dấu hiệu lỗi dữ liệu, bán vượt tồn kho
  | "CUSTOMER_INFO"          // tra cứu khách hàng: đã mua gì, còn nợ gì
  | "GET_INVOICES_BY_DATE"   // hóa đơn theo ngày
  | "GET_REVENUE"            // doanh thu/số lượng bán của 1 sản phẩm
  | "UNKNOWN";

export type ItemKindPref = "MACHINE" | "PART" | "ALL";

export type ParseResult = {
  intent: Intent;
  confidence: number;           // 0..1
  normalized: string;           // normalized no-diacritics
  raw: string;
  entities: {
    skus?: string[];
    queryText?: string;         // phần user gõ để tìm theo tên
    kindPref?: ItemKindPref;
    warehouseCode?: string;
    threshold?: number;
    date?: { from?: string; to?: string; exact?: string; preset?: "today"|"yesterday"|"this_week"|"this_month" };
  };
  debug?: any;
};

// --- normalize helpers ---
const DIACRITIC_MAP: Record<string, string> = {
  à:"a",á:"a",ạ:"a",ả:"a",ã:"a",â:"a",ầ:"a",ấ:"a",ậ:"a",ẩ:"a",ẫ:"a",ă:"a",ằ:"a",ắ:"a",ặ:"a",ẳ:"a",ẵ:"a",
  è:"e",é:"e",ẹ:"e",ẻ:"e",ẽ:"e",ê:"e",ề:"e",ế:"e",ệ:"e",ể:"e",ễ:"e",
  ì:"i",í:"i",ị:"i",ỉ:"i",ĩ:"i",
  ò:"o",ó:"o",ọ:"o",ỏ:"o",õ:"o",ô:"o",ồ:"o",ố:"o",ộ:"o",ổ:"o",ỗ:"o",ơ:"o",ờ:"o",ớ:"o",ợ:"o",ở:"o",ỡ:"o",
  ù:"u",ú:"u",ụ:"u",ủ:"u",ũ:"u",ư:"u",ừ:"u",ứ:"u",ự:"u",ử:"u",ữ:"u",
  ỳ:"y",ý:"y",ỵ:"y",ỷ:"y",ỹ:"y",
  đ:"d",
};

function removeDiacritics(s: string) {
  return s.replace(/[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]/g, (c) => DIACRITIC_MAP[c] || c);
}

function normalizeText(raw: string) {
  const lower = raw.trim().toLowerCase();
  const noDia = removeDiacritics(lower);
  // thay các ký tự phân cách bằng space
  const cleaned = noDia
    .replace(/[\u2013\u2014]/g, "-")
    .replace(/[^a-z0-9\-\/\s:<>=]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return cleaned;
}

function dashifySku(s: string) {
  // jl600 -> jl-600, frd1000 -> frd-1000 (nếu cần)
  const m = s.match(/^([a-z]+)(\d.+)$/i);
  if (m && !s.includes("-")) return `${m[1]}-${m[2]}`.toUpperCase();
  return s.toUpperCase();
}

function extractSkus(normalized: string) {
  // ✅ Ghép các cặp "chữ + số" gõ rời rạc (vd: "jl 600", "jl-600") thành 1 mã hoàn
  // chỉnh (JL-600) TRƯỚC khi tách token thường — nếu không, phần chữ ngắn (vd "jl")
  // dễ bị bỏ sót và chỉ còn lại số ("600") trơ trọi, dẫn đến khớp nhầm với mọi mã
  // khác chỉ vì tình cờ cùng chứa số đó (DZ-600, SF-600, SFTD-600...).
  const combined: string[] = [];
  const pairRe = /\b([a-z]{1,4})[\s-]+(\d{2,6}[a-z0-9]*)\b/g;
  let pm: RegExpExecArray | null;
  while ((pm = pairRe.exec(normalized))) {
    combined.push(`${pm[1]}-${pm[2]}`.toUpperCase());
  }

  // bắt token kiểu JL-660 / FRD1000 / PCX-20 / ST-608...
  const tokens = normalized.match(/[a-z0-9][a-z0-9\-_]{2,30}/g) || [];
  const stop = new Set(["ton","tonkho","con","bao","nhieu","may","linh","kien","phu","tung","kho","hoa","don","nhap","xuat","hang","sap","het","het"]);
  const skus = tokens
    // ✅ chỉ coi là SKU nếu token có chứa ít nhất 1 chữ số — tránh nhầm từ tiếng Việt
    // thường (dây, hàn, nhiệt, ...) thành mã sản phẩm, vì SKU thật luôn có số
    // (JL-660, PCX20, K8, 45079...).
    .filter((t) => /\d/.test(t))
    // ✅ loại token TOÀN CHỮ SỐ dài hơn 7 ký tự — đây gần như chắc chắn là số điện
    // thoại (9-10 số), không phải mã sản phẩm, tránh bị so khớp mờ bừa bãi (số dài
    // luôn "chứa" được các mã ngắn như "5", "20"... gây khớp sai hoàn toàn).
    .filter((t) => !(/^\d+$/.test(t) && t.length > 7))
    .map(t => t.toUpperCase())
    .filter(t => !stop.has(t.toLowerCase()))
    .map(dashifySku);

  // mã đã ghép (combined) ưu tiên đứng trước vì đầy đủ thông tin hơn
  return Array.from(new Set([...combined, ...skus])).slice(0, 10);
}

function extractWarehouse(normalized: string) {
  // "kho kho-01" / "kho hcm"
  const m = normalized.match(/\bkho\s+([a-z0-9\-_]{2,20})\b/);
  return m?.[1]?.toUpperCase();
}

function extractThreshold(normalized: string) {
  // "duoi 10" | "< 5" | "it hon 3"
  const m1 = normalized.match(/\b(duoi|<|it hon|nho hon)\s*(\d{1,6})\b/);
  if (m1) return Number(m1[2]);
  const m2 = normalized.match(/\b<=\s*(\d{1,6})\b/);
  if (m2) return Number(m2[1]);
  return undefined;
}

function extractDate(normalized: string) {
  // "X thang gan day" / "X ngay gan day" -> khoảng ngày cụ thể tính từ hôm nay
  const mRange = normalized.match(/\b(\d{1,2})\s*(thang|ngay)\s*gan\s*day\b/);
  if (mRange) {
    const n = Number(mRange[1]);
    const unit = mRange[2];
    const now = new Date();
    const from = new Date(now);
    if (unit === "thang") from.setMonth(from.getMonth() - n);
    else from.setDate(from.getDate() - n);
    return { from: from.toISOString().slice(0, 10), to: now.toISOString().slice(0, 10) };
  }

  // preset
  if (/\bhom nay\b/.test(normalized)) return { preset: "today" as const };
  if (/\bhom qua\b/.test(normalized)) return { preset: "yesterday" as const };
  if (/\btuan nay\b/.test(normalized)) return { preset: "this_week" as const };
  if (/\bthang nay\b/.test(normalized)) return { preset: "this_month" as const };

  // dd/mm or d/m (không cố suy luận năm ở parser, để service xử lý)
  const m = normalized.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/);
  if (m) {
    const dd = m[1].padStart(2, "0");
    const mm = m[2].padStart(2, "0");
    const yy = m[3] ? (m[3].length === 2 ? `20${m[3]}` : m[3]) : undefined;
    return { exact: yy ? `${yy}-${mm}-${dd}` : `${mm}-${dd}` }; // service sẽ gắn năm nếu thiếu
  }
  return undefined;
}

// --- scoring intents ---
type Score = { intent: Intent; score: number; reasons: string[] };

function scoreIntents(n: string, skus: string[], threshold?: number) : Score[] {
  const scores: Score[] = [
    { intent: "GET_STOCK", score: 0, reasons: [] },
    { intent: "LOW_STOCK", score: 0, reasons: [] },
    { intent: "OUT_OF_STOCK", score: 0, reasons: [] },
    { intent: "NEGATIVE_STOCK", score: 0, reasons: [] },
    { intent: "GET_INVOICES_BY_DATE", score: 0, reasons: [] },
    { intent: "GET_REVENUE", score: 0, reasons: [] },
  ];

  const has = (re: RegExp) => re.test(n);
  const add = (intent: Intent, pts: number, why: string) => {
    const s = scores.find(x => x.intent === intent)!;
    s.score += pts;
    s.reasons.push(`${pts>=0?"+":""}${pts}:${why}`);
  };

  // tồn âm (< 0) — câu hỏi liệt kê theo điều kiện, khác hẳn tra 1 sản phẩm cụ thể.
  // Điểm cao để luôn thắng GET_STOCK (vốn cũng được cộng điểm vì có chữ "tồn"/"máy").
  if (has(/\bton\s*<\s*0\b/) || has(/\bam\b/) || has(/\bton am\b/) || has(/\bnho hon 0\b/))
    add("NEGATIVE_STOCK", 10, "negative stock keyword");

  // stock keywords
  if (has(/\bton\b/) || has(/\bton kho\b/) || has(/\bcon bao nhieu\b/)) add("GET_STOCK", 6, "stock keyword");
  if (has(/\bmay\b/)) add("GET_STOCK", 3, "mentions 'may' (often stock check)");
  if (skus.length) add("GET_STOCK", 4, "has sku token");

  // low stock
  if (has(/\bsap het\b/) || has(/\bgan het\b/) || has(/\bcon it\b/)) add("LOW_STOCK", 6, "low-stock keyword");
  if (threshold != null) add("LOW_STOCK", 5, "has threshold");
  // low-stock usually no exact sku required
  if (!skus.length && (has(/\bhang\b/) || has(/\bm(ay|a)y\b/))) add("LOW_STOCK", 1, "inventory context");

  // out of stock
  if (has(/\bhet hang\b/) || has(/\bkhong con\b/) || has(/\bout of stock\b/)) add("OUT_OF_STOCK", 7, "out-of-stock keyword");

  // invoices
  if (has(/\bhoa don\b/) || has(/\binvoice\b/)) add("GET_INVOICES_BY_DATE", 8, "invoice keyword");
  if (has(/\bngay\b/) || has(/\bhom nay\b/) || has(/\bhom qua\b/)) add("GET_INVOICES_BY_DATE", 2, "date hint");

  // doanh thu / doanh số của 1 sản phẩm — điểm cao để không bị "may"+sku (GET_STOCK)
  // lấn át khi câu hỏi có cả tên máy lẫn từ "doanh thu"
  if (has(/\bdoanh thu\b/) || has(/\bdoanh so\b/) || has(/\bban duoc\b/) || has(/\bban chay\b/))
    add("GET_REVENUE", 9, "revenue keyword");

  // tra cứu khách hàng: đã mua gì / còn nợ gì / lịch sử mua hàng / theo SĐT
  if (
    has(/\bkhach hang\b/) ||
    has(/\bcon no cua\b/) ||
    has(/\bda mua\b/) ||
    has(/\blich su mua\b/) ||
    has(/\bmua gi\b/) ||
    has(/\bsdt\b/) ||
    has(/\bso dien thoai\b/) ||
    has(/\bmua lan nao\b/)
  )
    add("CUSTOMER_INFO", 8, "customer lookup keyword");

  return scores.sort((a,b)=>b.score-a.score);
}

export function parsePro(raw: string): ParseResult {
  const normalized = normalizeText(raw);
  const skus = extractSkus(normalized);
  const warehouseCode = extractWarehouse(normalized);
  const threshold = extractThreshold(normalized);
  const date = extractDate(normalized);

  // kind preference
  let kindPref: ItemKindPref = "ALL";
  if (/\bmay\b/.test(normalized)) kindPref = "MACHINE";
  if (/\blinh kien\b/.test(normalized) || /\bphu tung\b/.test(normalized)) kindPref = "PART";

  const scored = scoreIntents(normalized, skus, threshold);
  const best = scored[0];

  // confidence heuristic
  const confidence = Math.max(0, Math.min(1, best.score / 10));

  // queryText fallback: nếu không có sku mà có "ton/may ..." thì lấy phần sau làm query
  const FILLER_WORDS = new Set([
    "con", "nhieu", "ko", "khong", "het", "sap", "du", "a", "ha", "nhe", "the", "vay", "nay",
  ]);
  let queryText: string | undefined;
  if (skus.length === 0) {
    const m = normalized.match(/\b(ton|may|hoa don|khach hang|khach|con no cua|da mua|lich su mua)\b\s*(.+)$/);
    if (m?.[2]) {
      const cleaned = m[2]
        .split(" ")
        .filter((w) => w && !FILLER_WORDS.has(w))
        .join(" ")
        .trim();
      queryText = cleaned || m[2].trim();
    }
  }

  // Nếu best score quá thấp -> UNKNOWN
  const intent: Intent = best.score >= 4 ? best.intent : "UNKNOWN";

  // Với câu hỏi tra cứu khách hàng, tên khách có thể nằm TRƯỚC cụm từ khóa (cách
  // nói tự nhiên: "công ty X đã mua gì", "khách A còn nợ bao nhiêu") — khác với
  // GET_STOCK/GET_REVENUE nơi tên luôn nằm SAU từ khóa ("tồn X", "doanh thu của X").
  // Nên xử lý riêng, ưu tiên phần đứng trước từ khóa nếu có.
  if (intent === "CUSTOMER_INFO") {
    // Ưu tiên cao nhất: nếu câu có 1 chuỗi số dài kiểu số điện thoại (9-11 số),
    // dùng thẳng nó — đáng tin cậy hơn nhiều so với đoán vị trí trong câu, vì số
    // điện thoại có thể nằm bất kỳ đâu trong câu (đầu/giữa/cuối).
    const phoneMatch = normalized.match(/\b(\d{9,11})\b/);
    if (phoneMatch?.[1]) {
      queryText = phoneMatch[1];
    } else {
      const beforeMatch = normalized.match(/^(.*?)\b(da mua|con no cua|mua gi|lich su mua|mua lan nao)\b/);
      let beforeText = beforeMatch?.[1]?.trim();
      if (beforeText) {
        // bỏ các từ mở đầu câu thường gặp, không phải một phần tên khách hàng
        beforeText = beforeText.replace(/^(con|thi|xem|cho toi biet|cho hoi|ban)\s+/, "").trim();
      }
      if (beforeText) {
        queryText = beforeText;
      } else {
        // fallback: tên đứng SAU "khach hang"/"khach" (vd "khách Nguyễn Văn A")
        const afterMatch = normalized.match(/\b(khach hang|khach)\b\s*(.+)$/);
        if (afterMatch?.[2]) queryText = afterMatch[2].trim();
      }
    }
  }

  return {
    intent,
    confidence,
    normalized,
    raw,
    entities: {
      skus,
      queryText,
      kindPref,
      warehouseCode,
      threshold,
      date,
    },
    debug: { scored },
  };
}