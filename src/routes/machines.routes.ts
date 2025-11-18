import { Router } from "express";
import { PrismaClient, ItemKind } from "@prisma/client";
import { requireAuth } from "../middlewares/auth";

const prisma = new PrismaClient();
const r = Router();

// Bắt buộc đăng nhập cho toàn bộ route machines
r.use(requireAuth);

// ----------------------------------------------------
// Helper: đồng bộ Machine từ Item.kind = MACHINE
// ----------------------------------------------------
async function syncMachinesFromItems() {
  const machineItems = await prisma.item.findMany({
    where: { kind: "MACHINE" as ItemKind },
    select: {
      sku: true,
      name: true,
      note: true,
    },
  });

  if (machineItems.length === 0) {
    return { total: 0 };
  }

  await prisma.$transaction(
    machineItems.map((it) =>
      prisma.machine.upsert({
        where: { code: it.sku }, // code là duy nhất
        create: {
          code: it.sku,
          name: it.name,
          note: it.note ?? null,
        },
        update: {
          name: it.name,
          note: it.note ?? null,
        },
      })
    )
  );

  return { total: machineItems.length };
}

// ----------------------------------------------------
// POST /api/machines/sync-from-items
// Đồng bộ lại danh sách dòng máy từ bảng Item
// ----------------------------------------------------
r.post("/sync-from-items", async (_req, res, next) => {
  try {
    const result = await syncMachinesFromItems();
    res.json({
      ok: true,
      message: "Đã đồng bộ dòng máy từ Item.kind = MACHINE",
      syncedMachines: result.total,
    });
  } catch (err) {
    next(err);
  }
});

// ----------------------------------------------------
// GET /api/machines
// Query: q, page, pageSize
// ----------------------------------------------------
r.get("/", async (req, res, next) => {
  try {
    const q = String(req.query.q || "").trim();
    const pageSize = Number(req.query.pageSize) || 50;
    const page = Number(req.query.page) || 1;

    const where = q
      ? {
          OR: [
            { code: { contains: q, mode: "insensitive" } },
            { name: { contains: q, mode: "insensitive" } },
          ],
        }
      : {};

    const [items, total] = await Promise.all([
      prisma.machine.findMany({
        where,
        skip: (page - 1) * pageSize,
        take: pageSize,
        orderBy: { code: "asc" },
      }),
      prisma.machine.count({ where }),
    ]);

    res.json({ items, total });
  } catch (err) {
    next(err);
  }
});

// ----------------------------------------------------
// GET /api/machines/:id
// Trả về: { ok, data: { machine, parts[] } }
// parts có kèm tồn kho hiện tại (tổng tất cả kho)
// ----------------------------------------------------
r.get("/:id", async (req, res, next) => {
  try {
    const id = String(req.params.id);

    const machine = await prisma.machine.findUnique({
      where: { id },
    });

    if (!machine) {
      return res.status(404).json({ error: "Machine not found" });
    }

    const parts = await prisma.machinePart.findMany({
      where: { machineId: id },
      include: {
        item: true,
      },
      orderBy: {
        item: {
          sku: "asc",
        },
      },
    });

    const itemIds = parts.map((p) => p.itemId);
    let stockByItem: Record<string, number> = {};

    if (itemIds.length > 0) {
      const stocks = await prisma.stock.findMany({
        where: { itemId: { in: itemIds } },
      });

      stockByItem = stocks.reduce((acc, s: any) => {
        const key = String(s.itemId);
        const qty = Number(s.qty || 0);
        acc[key] = (acc[key] || 0) + qty;
        return acc;
      }, {} as Record<string, number>);
    }

    const partsWithStock = parts.map((p) => ({
      id: p.id,
      itemId: p.itemId,
      qtyPerSet: p.qtyPerSet,
      note: p.note,
      item: p.item,
      currentQty: stockByItem[p.itemId] || 0,
    }));

    res.json({
      ok: true,
      data: {
        machine,
        parts: partsWithStock,
      },
    });
  } catch (err) {
    next(err);
  }
});

// ----------------------------------------------------
// POST /api/machines
// Body: { code, name, note? }
// ----------------------------------------------------
r.post("/", async (req, res, next) => {
  try {
    const { code, name, note } = req.body || {};
    if (!code || !String(code).trim() || !name || !String(name).trim()) {
      return res.status(400).json({ error: "code & name are required" });
    }

    const created = await prisma.machine.create({
      data: {
        code: String(code).trim(),
        name: String(name).trim(),
        note: note ? String(note) : null,
      },
    });

    res.status(201).json({ ok: true, data: created });
  } catch (err) {
    next(err);
  }
});

// ----------------------------------------------------
// PUT /api/machines/:id
// Body: { code?, name?, note? }
// ----------------------------------------------------
r.put("/:id", async (req, res, next) => {
  try {
    const id = String(req.params.id);
    const { code, name, note } = req.body || {};

    const data: any = {};
    if (code !== undefined) data.code = String(code).trim();
    if (name !== undefined) data.name = String(name).trim();
    if (note !== undefined) data.note = note ? String(note) : null;

    const updated = await prisma.machine.update({
      where: { id },
      data,
    });

    res.json({ ok: true, data: updated });
  } catch (err) {
    next(err);
  }
});

// ----------------------------------------------------
// DELETE /api/machines/:id
// ----------------------------------------------------
r.delete("/:id", async (req, res, next) => {
  try {
    const id = String(req.params.id);
    await prisma.machine.delete({ where: { id } });
    res.json({ ok: true });
  } catch (err) {
    next(err);
  }
});

// ----------------------------------------------------
// POST /api/machines/:id/parts
// Body: { itemId, qtyPerSet?, note? }
// ----------------------------------------------------
r.post("/:id/parts", async (req, res, next) => {
  try {
    const machineId = String(req.params.id);
    const { itemId, qtyPerSet, note } = req.body || {};

    if (!itemId) {
      return res.status(400).json({ error: "itemId is required" });
    }

    const created = await prisma.machinePart.create({
      data: {
        machineId,
        itemId: String(itemId),
        qtyPerSet:
          qtyPerSet !== undefined && qtyPerSet !== null
            ? Number(qtyPerSet)
            : null,
        note: note ? String(note) : null,
      },
    });

    res.status(201).json({ ok: true, data: created });
  } catch (err: any) {
    if (err?.code === "P2002") {
      // unique (machineId, itemId)
      return res
        .status(400)
        .json({ error: "Linh kiện này đã tồn tại trong dòng máy." });
    }
    next(err);
  }
});

// ----------------------------------------------------
// DELETE /api/machines/:machineId/parts/:machinePartId
// ----------------------------------------------------
r.delete("/:machineId/parts/:machinePartId", async (req, res, next) => {
  try {
    const mpId = String(req.params.machinePartId);
    await prisma.machinePart.delete({ where: { id: mpId } });
    res.json({ ok: true });
  } catch (err) {
    next(err);
  }
});

export default r;
