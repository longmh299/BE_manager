// src/routes/partners.routes.ts
import { Router } from 'express';
import { Prisma, PrismaClient } from '@prisma/client';
import { requireAuth, requireRole } from '../middlewares/auth';

const prisma = new PrismaClient();
const r = Router();

r.use(requireAuth);

/** GET /partners?q=&page=&pageSize= */
r.get('/', async (req, res, next) => {
  try {
    const q = (req.query.q as string)?.trim() || '';
    const page = +(req.query.page as string) || 1;
    const pageSize = +(req.query.pageSize as string) || 50;

    const where = q
      ? {
          OR: [
            { code:    { contains: q, mode: 'insensitive' } },
            { name:    { contains: q, mode: 'insensitive' } },
            { taxCode: { contains: q, mode: 'insensitive' } },
            { phone:   { contains: q, mode: 'insensitive' } },
          ],
        }
      : {};

    const [rows, total] = await Promise.all([
      prisma.partner.findMany({
        where,
        orderBy: { updatedAt: 'desc' },
        skip: (page - 1) * pageSize,
        take: pageSize,
      }),
      prisma.partner.count({ where }),
    ]);

    res.json({ ok: true, data: rows, page, pageSize, total });
  } catch (e) { next(e); }
});

/** POST /partners  — tạo thường (có fallback code) */
r.post('/', requireRole('accountant','admin'), async (req, res) => {
  try {
    const codeRaw = (req.body?.code ?? '').toString().trim();
    const nameRaw = (req.body?.name ?? '').toString().trim();
    const taxCode = (req.body?.taxCode ?? '').toString().trim();
    const phone   = (req.body?.phone ?? '').toString().trim();
    const address = (req.body?.address ?? '').toString().trim();

    if (!nameRaw && !taxCode) {
      return res.status(400).json({ ok:false, error:'Thiếu name hoặc taxCode' });
    }

    const name = nameRaw || (taxCode ? `KH ${taxCode}` : 'KH mới');
    const code = codeRaw || taxCode || `P${Date.now()}`; // Fallback luôn có

    console.log('CREATE /partners code=', code);

    const created = await prisma.partner.create({
      data: {
        code,                               // ⬅️ BẮT BUỘC
        name,
        taxCode: taxCode || undefined,
        phone:   phone   || undefined,
        address: address || undefined,
      },
    });

    res.json({ ok:true, data: created });
  } catch (e:any) {
    console.error('POST /partners error:', e);
    if (e instanceof Prisma.PrismaClientKnownRequestError && e.code === 'P2002') {
      return res.status(409).json({ ok:false, error:'Trùng dữ liệu unique (code/taxCode)' });
    }
    res.status(500).json({ ok:false, error:'Server error', detail: e?.message });
  }
});

/** POST /partners/upsert-tax — upsert theo MST (có fallback code) */
r.post('/upsert-tax', requireRole('accountant','admin'), async (req, res) => {
  try {
    const taxCode = (req.body?.taxCode ?? '').toString().trim();
    const nameRaw = (req.body?.name ?? '').toString().trim();
    const phone   = (req.body?.phone ?? '').toString().trim();
    const address = (req.body?.address ?? '').toString().trim();
    const codeRaw = (req.body?.code ?? '').toString().trim();

    if (!taxCode) return res.status(400).json({ ok:false, error:'taxCode là bắt buộc' });

    const found = await prisma.partner.findFirst({ where: { taxCode } });

    if (found) {
      const updated = await prisma.partner.update({
        where: { id: found.id },
        data: {
          ...(nameRaw ? { name: nameRaw } : {}),
          ...(phone   ? { phone } : {}),
          ...(address ? { address } : {}),
          ...(codeRaw ? { code: codeRaw } : {}), // không ép đổi code khi không gửi
        },
      });
      return res.json({ ok:true, data: updated, upsert:'updated' });
    }

    // tạo mới
    const name = nameRaw || `KH ${taxCode}`;
    const code = codeRaw || taxCode || `P${Date.now()}`; // Fallback luôn có

    console.log('CREATE /partners/upsert-tax code=', code);

    const created = await prisma.partner.create({
      data: {
        code,               // ⬅️ BẮT BUỘC
        taxCode,
        name,
        ...(phone   ? { phone } : {}),
        ...(address ? { address } : {}),
      },
    });

    res.json({ ok:true, data: created, upsert:'created' });
  } catch (e:any) {
    console.error('POST /partners/upsert-tax error:', e);
    if (e instanceof Prisma.PrismaClientKnownRequestError) {
      if (e.code === 'P2002') {
        return res.status(409).json({ ok:false, error:'Trùng unique (code/taxCode)' });
      }
      if (e.code === 'P2003') {
        return res.status(400).json({ ok:false, error:'Ràng buộc khóa ngoại' });
      }
    }
    res.status(500).json({ ok:false, error:'Server error', detail: e?.message });
  }
});

/** PUT /partners/:id */
r.put('/:id', requireRole('accountant','admin'), async (req, res) => {
  try {
    const id      = req.params.id;
    const code    = (req.body?.code ?? '').toString().trim();
    const name    = (req.body?.name ?? '').toString().trim();
    const taxCode = (req.body?.taxCode ?? '').toString().trim();
    const phone   = (req.body?.phone ?? '').toString().trim();
    const address = (req.body?.address ?? '').toString().trim();

    const updated = await prisma.partner.update({
      where: { id },
      data: {
        ...(code    ? { code }    : {}),
        ...(name    ? { name }    : {}),
        ...(taxCode ? { taxCode } : {}),
        ...(phone   ? { phone }   : {}),
        ...(address ? { address } : {}),
      },
    });

    res.json({ ok:true, data: updated });
  } catch (e:any) {
    console.error('PUT /partners/:id error:', e);
    if (e instanceof Prisma.PrismaClientKnownRequestError && e.code === 'P2002') {
      return res.status(409).json({ ok:false, error:'Trùng dữ liệu unique' });
    }
    res.status(500).json({ ok:false, error:'Server error', detail: e?.message });
  }
});

/** DELETE /partners/:id */
r.delete('/:id', requireRole('admin'), async (req, res) => {
  try {
    const id = req.params.id;
    const del = await prisma.partner.delete({ where: { id } });
    res.json({ ok:true, data: del });
  } catch (e:any) {
    console.error('DELETE /partners/:id error:', e);
    res.status(500).json({ ok:false, error:'Server error', detail: e?.message });
  }
});

export default r;
