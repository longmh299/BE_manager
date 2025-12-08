// src/services/items.service.ts
import { Prisma, PrismaClient } from '@prisma/client';
import { buildSkuFrom } from '../utils/sku';

const prisma = new PrismaClient();

/**
 * ✅ SEARCH CHUẨN:
 * - Ưu tiên match SKU chính xác trước
 * - Sau đó mới tìm gần đúng theo name + sku
 * - Luôn ưu tiên MACHINE lên trước PART
 */
export async function listItems(q?: string, page = 1, pageSize = 20) {
  const keyword = q?.trim();

  // ===== 1. ƯU TIÊN MATCH SKU CHÍNH XÁC =====
  if (keyword) {
    const exactSku = await prisma.item.findMany({
      where: {
        sku: {
          equals: keyword,
          mode: 'insensitive',
        },
      },
      orderBy: {
        kind: 'desc', // ✅ MACHINE luôn đứng trên PART
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

  // ===== 2. TÌM GẦN ĐÚNG THEO NAME + SKU =====
  const where: Prisma.ItemWhereInput = {};

  if (keyword) {
    where.OR = [
      {
        sku: {
          contains: keyword,
          mode: 'insensitive' as Prisma.QueryMode,
        },
      },
      {
        name: {
          contains: keyword,
          mode: 'insensitive' as Prisma.QueryMode,
        },
      },
    ];
  }

  const [rows, total] = await Promise.all([
    prisma.item.findMany({
      where,

      // ✅ SORT ĐÚNG NGHIỆP VỤ
      orderBy: [
        { kind: 'desc' }, // ✅ MACHINE > PART
        { sku: 'asc' },   // ✅ Ưu tiên mã
        { name: 'asc' },  // ✅ Sau cùng theo tên
      ],

      skip: (page - 1) * pageSize,
      take: pageSize,
    }),
    prisma.item.count({ where }),
  ]);

  return { data: rows, page, pageSize, total };
}

// ===== ENSURE UNIQUE SKU =====
async function ensureUniqueSku(baseName: string) {
  let seq = 1;
  while (true) {
    const candidate = buildSkuFrom(baseName || 'SP', seq++);

    // dùng findFirst thay vì findUnique để không bị lỗi type
    const found = await prisma.item.findFirst({
      where: { sku: candidate },
      select: { id: true },
    });

    if (!found) return candidate;
  }
}

// ===== CREATE ITEM =====
export async function createItem(body: any) {
  const name: string = (body?.name ?? '').toString().trim();
  let sku: string = (body?.sku ?? '').toString().trim();

  if (!name && !sku) {
    throw Object.assign(new Error('Thiếu name hoặc sku'), { status: 400 });
  }

  if (!sku) {
    sku = await ensureUniqueSku(name || 'SP');
  }

  const unit = (body?.unit ?? 'pcs').toString().trim();
  const price = Number(body?.price ?? 0);
  const sellPrice = Number(body?.sellPrice ?? 0);
  const note = body?.note?.toString();
  const kind = (body?.kind ?? 'PART').toString().toUpperCase() as any;
  const isSerialized = !!body?.isSerialized;

  try {
    const created = await prisma.item.create({
      data: {
        sku,
        name: name || sku,
        unit,
        price: price as any,
        sellPrice: sellPrice as any,
        note: note || undefined,
        // nếu schema chưa có 2 field này thì comment 2 dòng dưới
        // @ts-ignore
        kind,
        // @ts-ignore
        isSerialized,
      },
    });
    return created;
  } catch (e: any) {
    if (
      e instanceof Prisma.PrismaClientKnownRequestError &&
      e.code === 'P2002'
    ) {
      // trường hợp trùng unique field (vd sku nếu bạn để @unique)
      if (!body?.sku) {
        const newSku = await ensureUniqueSku(name || 'SP');
        const created = await prisma.item.create({
          data: {
            sku: newSku,
            name: name || newSku,
            unit,
            price: price as any,
            sellPrice: sellPrice as any,
            note: note || undefined,
            // @ts-ignore
            kind,
            // @ts-ignore
            isSerialized,
          },
        });
        return created;
      }
      throw Object.assign(new Error('Trùng SKU'), { status: 409 });
    }
    throw e;
  }
}

// ===== UPDATE ITEM =====
export async function updateItem(id: string, body: any) {
  const data: any = {};

  if (typeof body?.sku !== 'undefined') {
    const sku = (body.sku ?? '').toString().trim();
    data.sku = sku ? sku : await ensureUniqueSku(body?.name || 'SP');
  }
  if (typeof body?.name !== 'undefined')
    data.name = (body.name ?? '').toString().trim();
  if (typeof body?.unit !== 'undefined')
    data.unit = (body.unit ?? 'pcs').toString().trim();
  if (typeof body?.price !== 'undefined')
    data.price = Number(body.price ?? 0) as any;
  if (typeof body?.sellPrice !== 'undefined')
    data.sellPrice = Number(body.sellPrice ?? 0) as any;
  if (typeof body?.note !== 'undefined')
    data.note = body.note ? String(body.note) : null;

  if (typeof body?.kind !== 'undefined')
    data.kind = String(body.kind).toUpperCase();
  if (typeof body?.isSerialized !== 'undefined')
    data.isSerialized = !!body.isSerialized;

  try {
    return await prisma.item.update({ where: { id }, data });
  } catch (e: any) {
    if (
      e instanceof Prisma.PrismaClientKnownRequestError &&
      e.code === 'P2002'
    ) {
      throw Object.assign(new Error('Trùng SKU'), { status: 409 });
    }
    throw e;
  }
}

// ===== REMOVE ITEM =====
export async function removeItem(id: string) {
  return prisma.item.delete({ where: { id } });
}
