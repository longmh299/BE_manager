// src/routes/stockcounts.routes.ts
import { Router } from "express";
import { requireAuth, requireRole } from "../middlewares/auth";
import {
  listStockCounts,
  createStockCountWithLines,
  getStockCountDetail,
  updateStockCountLine,
  postStockCount,
} from "../services/stockcounts.service";
import * as XLSX from "xlsx";
import { buildReport, sendXlsx, exportSubtitle, stampName, KIND_VN } from "../lib/excelReport"; // ✅ khuôn Excel dùng chung
import { getUser } from "../middlewares/auth";
import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();
const r = Router();

// tất cả route kiểm kê đều yêu cầu đăng nhập
r.use(requireAuth);

function getUserId(req: any): string | undefined {
  return req.user?.id || req.userId;
}

function getUserRole(req: any): string | undefined {
  return req.user?.role || req.userRole;
}

function buildReqMeta(req: any) {
  return {
    ip: req.ip,
    userAgent: req.headers?.["user-agent"],
    path: req.originalUrl || req.url,
    method: req.method,
  };
}

/**
 * Service stockcounts hiện dùng AuditCtx = { userId: string; userRole?: string } | undefined
 * => route đảm bảo có userId để audit chạy ổn định
 */
function requireUserOr401(req: any, res: any) {
  const userId = getUserId(req);
  if (!userId) {
    res.status(401).json({ ok: false, message: "Chưa đăng nhập." });
    return null;
  }
  return { userId, userRole: getUserRole(req) };
}

/**
 * GET /stock-counts?locationId=&status=&q=&page=&pageSize=
 * Danh sách phiếu kiểm kê
 */
r.get("/", async (req, res, next) => {
  try {
    const { locationId, status, q, page, pageSize } = req.query as any;
    const pageNum = Number(page) || 1;
    const sizeNum = Number(pageSize) || 20;

    const result = await listStockCounts({
      locationId: locationId || undefined,
      status: status || undefined,
      q: q || undefined,
      page: pageNum,
      pageSize: sizeNum,
    });

    res.json({ ok: true, ...result });
  } catch (e) {
    next(e);
  }
});

/**
 * POST /stock-counts  (accountant|admin)
 * Body: { locationId, refNo?, note?, includeZero? }
 * -> Tạo phiếu kiểm kê draft + sinh dòng cho toàn bộ item
 */
r.post("/", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const { locationId, refNo, note, includeZero } = req.body as {
      locationId: string;
      refNo?: string;
      note?: string;
      includeZero?: boolean;
    };

    const audit = requireUserOr401(req, res);
    if (!audit) return;

    const data = await createStockCountWithLines(
      {
        locationId,
        refNo,
        note,
        includeZero,
      },
      audit
    );

    res.json({ ok: true, data });
  } catch (e) {
    next(e);
  }
});

/**
 * GET /stock-counts/:id
 * -> Chi tiết phiếu kiểm kê + bookQty + diff
 */
r.get("/:id", async (req, res, next) => {
  try {
    const data = await getStockCountDetail(req.params.id);
    res.json({ ok: true, data });
  } catch (e) {
    next(e);
  }
});

/**
 * PUT /stock-counts/lines/:lineId  (accountant|admin)
 * Body: { countedQty }
 * -> Cập nhật số thực đếm cho 1 dòng
 */
r.put(
  "/lines/:lineId",
  requireRole("accountant", "admin"),
  async (req, res, next) => {
    try {
      const { countedQty } = req.body as { countedQty?: string | number };

      const audit = requireUserOr401(req, res);
      if (!audit) return;

      const data = await updateStockCountLine(
        req.params.lineId,
        { countedQty },
        audit
      );

      res.json({ ok: true, data });
    } catch (e) {
      next(e);
    }
  }
);

/**
 * POST /stock-counts/:id/post  (accountant|admin)
 * -> Post kiểm kê:
 *    - Tính chênh lệch
 *    - Tạo Movement ADJUST + MovementLine
 *    - Cập nhật bảng Stock
 *    - Đổi status = "posted"
 */
r.post("/:id/post", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const { movementRefNo, movementNote } = req.body as {
      movementRefNo?: string;
      movementNote?: string;
    };

    const audit = requireUserOr401(req, res);
    if (!audit) return;

    const result = await postStockCount(
      req.params.id,
      { movementRefNo, movementNote },
      audit
    );

    res.json({ ok: true, ...result });
  } catch (e) {
    next(e);
  }
});

/**
 * DELETE /stock-counts/:id  (accountant|admin)
 * -> Xoá phiếu kiểm kê DEMO / nháp
 *    - Chỉ cho xoá khi status = "draft"
 *
 * NOTE: route này đang xoá thẳng Prisma (không qua service),
 * mình chỉ cắm thêm audit log nhẹ để truy vết.
 *
 * ✅ FIX: AuditLog schema chỉ có { userId, action, entity, entityId, meta }
 * => nhét userRole/before/after/requestMeta vào meta.
 */
r.delete("/:id", requireRole("accountant", "admin"), async (req, res, next) => {
  try {
    const audit = requireUserOr401(req, res);
    if (!audit) return;

    const id = req.params.id;

    const sc = await prisma.stockCount.findUnique({ where: { id } });
    if (!sc) {
      return res.status(404).json({ ok: false, message: "StockCount not found" });
    }
    if (String(sc.status).toLowerCase() === "posted") {
      return res.status(400).json({
        ok: false,
        message: "Không thể xoá phiếu kiểm kê đã post",
      });
    }

    await prisma.stockCount.delete({ where: { id } });

    // ✅ audit nhẹ (không ảnh hưởng nghiệp vụ) - KHÔNG throw nếu fail
    try {
      await prisma.auditLog.create({
        data: {
          userId: audit.userId,
          action: "STOCKCOUNT_DELETE",
          entity: "StockCount",
          entityId: id,
          meta: {
            userRole: audit.userRole ?? null,
            before: {
              id: sc.id,
              refNo: sc.refNo,
              status: sc.status,
              locationId: sc.locationId,
              note: sc.note ?? null,
              createdAt: (sc as any).createdAt ?? null,
            },
            after: null,
            request: buildReqMeta(req),
          },
        },
      });
    } catch (err) {
      console.error("[AUDIT_LOG_FAILED]", err);
    }

    res.json({ ok: true });
  } catch (e) {
    next(e);
  }
});

/**
 * GET /stock-counts/:id/export?kind=PART|MACHINE
 * -> Xuất Excel phiếu kiểm kê:
 *    SKU | Tên hàng | ĐVT | Tồn sổ | Thực đếm | Chênh lệch
 *    - Nếu truyền kind = PART/MACHINE => chỉ export loại đó
 */
r.get("/:id/export", async (req, res, next) => {
  try {
    const { kind } = req.query as any;
    const detail = await getStockCountDetail(req.params.id);

    let lines: any[] = detail.lines as any[];

    if (kind === "PART" || kind === "MACHINE") {
      lines = lines.filter((line) => line.item?.kind === kind);
    }

    // ✅ cùng dữ liệu như trước, chỉ trình bày lại (thêm tên ĐVT — trước đây cột ĐVT luôn trống)
    const units = await prisma.unit.findMany({ select: { id: true, name: true, code: true } }).catch(() => []);
    const unitName = new Map(units.map((u: any) => [u.id, u.name || u.code]));
    const excelRows = lines.map((line: any) => ({
      sku: line.item?.sku ?? "",
      name: line.item?.name ?? "",
      unit: unitName.get(line.item?.unitId) ?? "",
      kind: KIND_VN[line.item?.kind] ?? line.item?.kind ?? "",
      bookQty: line.bookQty ?? "0",
      countedQty: line.countedQty?.toString?.() ?? "0",
      diff: line.diff ?? "0",
    }));
    const d: any = detail;
    const posted = String(d.status || "").toLowerCase() === "posted";
    const buf = await buildReport({
      sheetName: "Kiểm kê",
      title: `PHIẾU KIỂM KÊ ${d.refNo ?? ""}`.trim(),
      subtitle: exportSubtitle(
        getUser(req),
        d.location ? `Kho: ${d.location.name || d.location.code}` : "",
        posted ? "Đã chốt" : "Nháp",
        kind === "PART" ? "Chỉ phụ tùng" : kind === "MACHINE" ? "Chỉ máy" : ""
      ),
      columns: [
        { header: "Mã hàng", key: "sku", width: 14 },
        { header: "Tên hàng", key: "name", width: 46, wrap: true },
        { header: "ĐVT", key: "unit", width: 8, type: "center" },
        { header: "Loại", key: "kind", width: 11, type: "center" },
        { header: "Tồn sổ sách", key: "bookQty", width: 13, type: "qty", total: true },
        { header: "Thực đếm", key: "countedQty", width: 12, type: "qty", total: true },
        { header: "Chênh lệch", key: "diff", width: 12, type: "qty", total: true, redIfNegative: true },
      ],
      rows: excelRows,
      stt: true,
      totals: true,
    });
    sendXlsx(res, buf, stampName(`kiem-ke-${d.refNo || req.params.id}`));
  } catch (e) {
    next(e);
  }
});

export default r;
