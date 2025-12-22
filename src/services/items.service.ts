// src/services/items.service.ts
import { Prisma, PrismaClient, UserRole } from "@prisma/client";
import { buildSkuFrom } from "../utils/sku";

const prisma = new PrismaClient();
type Tx = Prisma.TransactionClient;

type Actor = { id?: string; role?: UserRole };

function httpError(status: number, message: string) {
  const err: any = new Error(message);
  err.statusCode = status;
  return err;
}

async function resolveUnitId(input: { unitId?: string; unitCode?: string }, tx = prisma) {
  if (input.unitId) {
    const u = await tx.unit.findUnique({ where: { id: input.unitId }, select: { id: true } });
    if (!u) throw httpError(400, "unitId không tồn tại");
    return u.id;
  }

  const code = (input.unitCode || "").toString().trim();
  if (code) {
    const u = await tx.unit.findUnique({ where: { code }, select: { id: true } });
    if (!u) throw httpError(400, `Unit code "${code}" không tồn tại`);
    return u.id;
  }

  // fallback: lấy unit pcs (nếu có)
  const pcs = await tx.unit.findUnique({ where: { code: "pcs" }, select: { id: true } });
  if (!pcs) throw httpError(500, 'Thiếu unit mặc định "pcs" (hãy seed Unit)');
  return pcs.id;
}

/**
 * ===========================================
 * SEARCH ITEMS
 * - include unit để FE show name/code
 * - OPTIONAL: ẩn giá vốn nếu actor không phải admin
 * ===========================================
 */
export async function listItems(q?: string, page = 1, pageSize = 20, actor?: Actor) {
  const keyword = q?.trim();
  const isAdmin = actor?.role === "admin";

  // --- 1. Exact match SKU ---
  if (keyword) {
    const exactSku = await prisma.item.findMany({
      where: { sku: { equals: keyword, mode: "insensitive" } },
      orderBy: { kind: "desc" },
      select: isAdmin
        ? {
            id: true,
            sku: true,
            name: true,
            price: true,
            sellPrice: true,
            note: true,
            kind: true,
            isSerialized: true,
            unit: { select: { id: true, code: true, name: true } },
            createdAt: true,
            updatedAt: true,
          }
        : {
            id: true,
            sku: true,
            name: true,
            sellPrice: true,
            note: true,
            kind: true,
            isSerialized: true,
            unit: { select: { id: true, code: true, name: true } },
            createdAt: true,
            updatedAt: true,
          },
    });

    if (exactSku.length > 0) {
      return {
        data: exactSku,
        page: 1,
        pageSize: exactSku.length,
        total: exactSku.length,
      };
    }
  }

  // --- 2. Broad match ---
  const where: Prisma.ItemWhereInput = {};
  if (keyword) {
    where.OR = [
      { sku: { contains: keyword, mode: "insensitive" } },
      { name: { contains: keyword, mode: "insensitive" } },
    ];
  }

  const [rows, total] = await Promise.all([
    prisma.item.findMany({
      where,
      orderBy: [{ kind: "desc" }, { sku: "asc" }, { name: "asc" }],
      skip: (page - 1) * pageSize,
      take: pageSize,
      select: isAdmin
        ? {
            id: true,
            sku: true,
            name: true,
            price: true,
            sellPrice: true,
            note: true,
            kind: true,
            isSerialized: true,
            unit: { select: { id: true, code: true, name: true } },
            createdAt: true,
            updatedAt: true,
          }
        : {
            id: true,
            sku: true,
            name: true,
            sellPrice: true,
            note: true,
            kind: true,
            isSerialized: true,
            unit: { select: { id: true, code: true, name: true } },
            createdAt: true,
            updatedAt: true,
          },
    }),
    prisma.item.count({ where }),
  ]);

  return { data: rows, page, pageSize, total };
}

/**
 * ===========================================
 * SKU GENERATOR
 * ===========================================
 */
async function ensureUniqueSku(baseName: string, tx: Tx | PrismaClient = prisma) {
  let seq = 1;
  while (true) {
    const candidate = buildSkuFrom(baseName || "SP", seq++);
    const found = await tx.item.findFirst({
      where: { sku: candidate },
      select: { id: true },
    });
    if (!found) return candidate;
  }
}

/**
 * ===========================================
 * CREATE ITEM
 * body: { name, sku?, unitId? | unitCode?, price?, sellPrice?, ... }
 * ===========================================
 */
export async function createItem(body: any, actor?: Actor) {
  const name = (body?.name ?? "").toString().trim();
  let sku = (body?.sku ?? "").toString().trim();

  if (!name && !sku) {
    throw httpError(400, "Thiếu name hoặc sku");
  }
  if (!sku) sku = await ensureUniqueSku(name || "SP");

  const unitId = await resolveUnitId(
    { unitId: body?.unitId, unitCode: body?.unitCode || body?.unit },
    prisma
  );

  const isAdmin = actor?.role === "admin";

  return prisma.item.create({
    data: {
      sku,
      name: name || sku,
      unitId,
      price: isAdmin ? Number(body?.price ?? 0) : 0,
      sellPrice: Number(body?.sellPrice ?? 0),
      note: body?.note ?? undefined,
      kind: body?.kind ?? "PART",
      isSerialized: !!body?.isSerialized,
    },
    include: { unit: true },
  });
}

/**
 * ===========================================
 * UPDATE ITEM
 * - unit: update bằng unitId/unitCode
 * - price: chỉ admin được sửa (private)
 * ===========================================
 */
export async function updateItem(id: string, body: any, actor?: Actor) {
  const data: any = {};

  if (body?.sku !== undefined) {
    const sku = (body.sku ?? "").toString().trim();
    data.sku = sku || (await ensureUniqueSku(body?.name || "SP"));
  }
  if (body?.name !== undefined) data.name = String(body.name).trim();

  if (body?.unitId !== undefined || body?.unitCode !== undefined || body?.unit !== undefined) {
    data.unitId = await resolveUnitId(
      { unitId: body?.unitId, unitCode: body?.unitCode || body?.unit },
      prisma
    );
  }

  if (body?.price !== undefined) {
    if (actor?.role !== "admin") {
      throw httpError(403, "Chỉ admin được sửa giá vốn");
    }
    data.price = Number(body.price ?? 0);
  }

  if (body?.sellPrice !== undefined) data.sellPrice = Number(body.sellPrice ?? 0);

  if (body?.note !== undefined) data.note = body.note ? String(body.note) : null;
  if (body?.kind !== undefined) data.kind = body.kind;
  if (body?.isSerialized !== undefined) data.isSerialized = !!body.isSerialized;

  return prisma.item.update({
    where: { id },
    data,
    include: { unit: true },
  });
}

/**
 * ===========================================
 * UPDATE ITEM MASTER (admin UI)
 * - chỉ cho update name + unitId/unitCode
 * - KHÔNG đụng price/sellPrice/kind/isSerialized/sku để ít ảnh hưởng
 * ===========================================
 */
export async function updateItemMaster(id: string, body: any) {
  const data: any = {};

  if (body?.name !== undefined) {
    const name = String(body?.name ?? "").trim();
    if (!name) throw httpError(400, "Tên sản phẩm không được rỗng");
    data.name = name;
  }

  if (body?.unitId !== undefined || body?.unitCode !== undefined || body?.unit !== undefined) {
    data.unitId = await resolveUnitId(
      { unitId: body?.unitId, unitCode: body?.unitCode || body?.unit },
      prisma
    );
  }

  if (Object.keys(data).length === 0) {
    throw httpError(400, "Không có dữ liệu cần cập nhật");
  }

  return prisma.item.update({
    where: { id },
    data,
    include: { unit: true },
  });
}

/**
 * ===========================================
 * REMOVE ITEM
 * ===========================================
 */
export async function removeItem(id: string) {
  return prisma.item.delete({ where: { id } });
}

/**
 * ===========================================================
 * FIND OR CREATE — SUPPORT TRANSACTION (IMPORT MOVEMENT)
 * - unitId: default = pcs
 * ===========================================================
 */
export async function findOrCreateBySkuOrName(sku?: string | null, name?: string | null, tx?: Tx) {
  const db = tx ?? prisma;

  if (!sku && !name) {
    throw new Error("Missing sku or name");
  }

  const item = await db.item.findFirst({
    where: {
      OR: [sku ? { sku } : undefined, name ? { name } : undefined].filter(Boolean) as any,
    },
    include: { unit: true },
  });

  if (item) return item;

  const finalName = name?.trim() || sku!.trim();
  const finalSku = sku?.trim() || (await ensureUniqueSku(finalName, db));

  const unitId = await resolveUnitId({ unitCode: "pcs" }, db as any);

  return db.item.create({
    data: {
      sku: finalSku,
      name: finalName,
      unitId,
      price: 0,
      sellPrice: 0,
      kind: "PART",
      isSerialized: false,
    },
    include: { unit: true },
  });
}
