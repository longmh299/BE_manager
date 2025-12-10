// src/services/invoices.service.ts
import {
  PrismaClient,
  InvoiceType,
  Prisma,
  PaymentStatus,
} from "@prisma/client";

const prisma = new PrismaClient();

/** ========================= Helpers ========================= **/

// Tạo Error có statusCode để route biết là lỗi business (400, 409, ...)
function httpError(status: number, message: string) {
  const err: any = new Error(message);
  err.statusCode = status;
  return err;
}

// Bắt lỗi unique (P2002) cho bảng Invoice.code
function handleUniqueInvoiceError(e: unknown) {
  if (
    e instanceof Prisma.PrismaClientKnownRequestError &&
    e.code === "P2002"
  ) {
    const target = (e.meta as any)?.target;
    const targetStr = Array.isArray(target)
      ? target.join(",")
      : String(target || "");
    // unique nằm trên cột `code`
    if (targetStr.includes("code")) {
      throw httpError(400, "Mã hoá đơn đã tồn tại");
    }
  }
  throw e;
}

async function ensureWarehouse(warehouseId?: string) {
  if (warehouseId) {
    const w = await prisma.location.findUnique({ where: { id: warehouseId } });
    if (!w) throw new Error("Warehouse not found");
    return w;
  }
  // Tự pick 1 kho duy nhất
  const warehouses = await prisma.location.findMany({
    where: { kind: "warehouse" },
  });
  if (warehouses.length === 0) throw new Error("No warehouse found");
  if (warehouses.length > 1)
    throw new Error("Multiple warehouses detected. Please specify warehouseId.");
  return warehouses[0];
}

function toNum(
  d: Prisma.Decimal | number | string | null | undefined
): number {
  if (d == null) return 0;
  if (typeof d === "number") return d;
  return Number(d.toString());
}

/**
 * Tính số tiền thuế dựa trên body:
 * - Ưu tiên taxPercent (thuế %): tax = round(subtotal * taxPercent / 100)
 * - Nếu không có taxPercent, mà có tax (số tiền tuyệt đối) thì dùng tax
 * - Nếu cả 2 đều trống → tax = 0
 */
function calcTaxFromBody(subtotal: number, body: any): number {
  if (body) {
    const rawPercent =
      body.taxPercent !== undefined && body.taxPercent !== null
        ? body.taxPercent
        : undefined;

    if (rawPercent !== undefined && rawPercent !== "") {
      const p = Number(rawPercent);
      if (!isNaN(p) && p > 0) {
        return Math.round((subtotal * p) / 100);
      }
    }

    const rawTax =
      body.tax !== undefined && body.tax !== null ? body.tax : undefined;

    if (rawTax !== undefined && rawTax !== "") {
      const t = Number(rawTax);
      if (!isNaN(t)) return t;
    }
  }

  return 0;
}

/**
 * Recompute subtotal / total cho một invoice:
 * - subtotal = tổng amount của mọi dòng
 * - GIỮ nguyên tax hiện có trên invoice
 * - total = subtotal + tax
 *
 * Dùng cho các API chỉnh line đơn lẻ (add/update/delete line)
 */
async function recomputeInvoiceTotals(invoiceId: string) {
  const [invoice, lines] = await Promise.all([
    prisma.invoice.findUnique({
      where: { id: invoiceId },
      select: { tax: true },
    }),
    prisma.invoiceLine.findMany({ where: { invoiceId } }),
  ]);

  if (!invoice) throw new Error("Invoice not found");

  const subtotal = lines.reduce((s, l) => s + toNum(l.amount), 0);
  const tax = toNum(invoice.tax);
  const total = subtotal + tax;

  return prisma.invoice.update({
    where: { id: invoiceId },
    data: {
      subtotal: new Prisma.Decimal(subtotal),
      total: new Prisma.Decimal(total),
    },
  });
}

/** Atomic upsert + increment tồn kho */
async function applyStockDelta(
  locationId: string,
  itemId: string,
  deltaQty: number
) {
  if (Math.abs(deltaQty) < 1e-12) return;
  await prisma.stock.upsert({
    where: { itemId_locationId: { itemId, locationId } },
    create: { itemId, locationId, qty: new Prisma.Decimal(deltaQty) },
    update: { qty: { increment: new Prisma.Decimal(deltaQty) } },
  });
}

function desiredSignedQty(invoiceType: InvoiceType, qty: number): number {
  // PURCHASE => IN => +; SALES => OUT => -
  return invoiceType === "PURCHASE" ? qty : -qty;
}

function sumByItem<T extends { itemId: string; qty: number }>(rows: T[]) {
  const map = new Map<string, number>();
  rows.forEach((r) => {
    map.set(r.itemId, (map.get(r.itemId) || 0) + r.qty);
  });
  return map;
}

/** ========================= Public APIs ========================= **/

export async function listInvoices(
  q: string | undefined,
  page: number,
  pageSize: number,
  filter: {
    type?: InvoiceType;
    saleUserId?: string;
    techUserId?: string;
    from?: Date;
    to?: Date;
    paymentStatus?: PaymentStatus;
  }
) {
  const where: Prisma.InvoiceWhereInput = {};

  if (q) {
    Object.assign(where, {
      OR: [
        { code: { contains: q, mode: "insensitive" } },
        { partnerName: { contains: q, mode: "insensitive" } },
        { note: { contains: q, mode: "insensitive" } },
      ],
    });
  }

  if (filter?.type) where.type = filter.type;
  if (filter?.saleUserId) where.saleUserId = filter.saleUserId as any;
  if (filter?.techUserId) where.techUserId = filter.techUserId as any;
  if (filter?.from || filter?.to) {
    where.issueDate = {};
    if (filter.from) (where.issueDate as any).gte = filter.from;
    if (filter.to) (where.issueDate as any).lte = filter.to;
  }
  if (filter?.paymentStatus) {
    where.paymentStatus = filter.paymentStatus;
  }

  const [total, rows] = await Promise.all([
    prisma.invoice.count({ where }),
    prisma.invoice.findMany({
      where,
      orderBy: { issueDate: "desc" },
      include: {
        partner: true,
        saleUser: true,
        techUser: true,
        lines: { include: { item: true } },
        movements: true,
      },
      skip: (page - 1) * pageSize,
      take: pageSize,
    }),
  ]);

  return { data: rows, total, page, pageSize };
}

export async function getInvoiceById(id: string) {
  return prisma.invoice.findUnique({
    where: { id },
    include: {
      partner: true,
      saleUser: true,
      techUser: true,
      lines: { include: { item: true } },
      movements: { include: { lines: true } },
    },
  });
}
export async function updateInvoiceNote(id: string, note: string) {
  const updated = await prisma.invoice.update({
    where: { id },
    data: { note },
    select: { id: true, note: true },
  });

  return updated;
}
export async function createInvoice(body: any) {
  const issueDate = body.issueDate ? new Date(body.issueDate) : new Date();

  const safeCode =
    body.code && String(body.code).trim().length > 0
      ? String(body.code).trim()
      : `INV-${Date.now()}`;

  // ===== VALIDATE LINES TRƯỚC, KHÔNG CHO LƯU KHI CHƯA CHỌN SẢN PHẨM =====
  const rawLines: any[] = Array.isArray(body.lines) ? body.lines : [];

  const validLines = rawLines
    .map((l) => ({
      ...l,
      itemId: l.itemId,
      qty: Number(l.qty || 0),
      price: Number(l.price || 0),
    }))
    // Chỉ nhận dòng có itemId (đã chọn từ dropdown) và SL > 0
    .filter((l) => !!l.itemId && l.qty > 0);

  if (!validLines.length) {
    // Lỗi business 400, FE bắt được và show cảnh báo đẹp
    throw httpError(
      400,
      "Hoá đơn phải có ít nhất 1 sản phẩm (hãy chọn sản phẩm từ danh sách)."
    );
  }

  // Tính subtotal từ các dòng
  const subtotal = validLines.reduce((s, l) => s + l.qty * l.price, 0);

  // Tính tax theo taxPercent / tax nếu FE gửi lên
  const tax = calcTaxFromBody(subtotal, body);
  const total = subtotal + tax;

  // Thanh toán
  const paidAmount =
    body.paidAmount != null ? Number(body.paidAmount) || 0 : 0;

  const paymentStatus: PaymentStatus =
    body.paymentStatus ??
    (paidAmount <= 0
      ? "UNPAID"
      : paidAmount >= total
      ? "PAID"
      : "PARTIAL");

  let created;
  try {
    // 1) Tạo hoá đơn
    created = await prisma.invoice.create({
      data: {
        code: safeCode,
        type: body.type ?? "SALES",
        issueDate,

        // các cột FK / id
        partnerId: body.partnerId ?? null,
        saleUserId: body.saleUserId ?? null,
        techUserId: body.techUserId ?? null,

        note: body.note ?? "",

        partnerName: body.partnerName ?? null,
        partnerPhone: body.partnerPhone ?? null,
        partnerTax: body.partnerTax ?? null,
        partnerAddr: body.partnerAddr ?? null,

        currency: body.currency ?? "VND",

        subtotal: new Prisma.Decimal(subtotal),
        tax: new Prisma.Decimal(tax),
        total: new Prisma.Decimal(total),

        paymentStatus,
        paidAmount: new Prisma.Decimal(paidAmount),
      },
    });
  } catch (e) {
    // nếu trùng code thì ném ra lỗi 400 "Mã hoá đơn đã tồn tại"
    handleUniqueInvoiceError(e);
  }

  const createdId = created!.id;

  // 2) Tạo dòng hàng từ validLines
  for (const l of validLines) {
    const amount = l.qty * l.price;
    await prisma.invoiceLine.create({
      data: {
        invoiceId: createdId,
        itemId: l.itemId,
        qty: new Prisma.Decimal(l.qty),
        price: new Prisma.Decimal(l.price),
        amount: new Prisma.Decimal(amount),
        itemName: l.itemName || undefined,
        itemSku: l.itemSku || undefined,
      },
    });
  }

  // 3) Trả về invoice đầy đủ
  return getInvoiceById(createdId);
}

/**
 * Update invoice + thay toàn bộ lines bằng body.lines
 * - Subtotal / tax / total được tính lại theo lines + taxPercent/tax gửi từ FE
 */
export async function updateInvoice(id: string, body: any) {
  try {
    // transaction: update header + replace lines + update totals
    await prisma.$transaction(async (tx) => {
      const data: any = {};

      // cho phép sửa code
      if (body.code !== undefined) {
        const trimmed = String(body.code || "").trim();
        if (trimmed.length > 0) data.code = trimmed;
      }

      if (body.issueDate) {
        data.issueDate = new Date(body.issueDate);
      }

      if (body.type) {
        data.type = body.type as InvoiceType;
      }

      if (body.note !== undefined) {
        data.note = body.note;
      }

      // snapshot thông tin khách hàng
      if (body.partnerName !== undefined) data.partnerName = body.partnerName;
      if (body.partnerPhone !== undefined)
        data.partnerPhone = body.partnerPhone;
      if (body.partnerTax !== undefined) data.partnerTax = body.partnerTax;
      if (body.partnerAddr !== undefined) data.partnerAddr = body.partnerAddr;

      // nhân viên
      if (body.saleUserId !== undefined)
        data.saleUserId = body.saleUserId || null;
      if (body.techUserId !== undefined)
        data.techUserId = body.techUserId || null;

      // thanh toán: cho phép FE truyền thẳng lên
      if (body.paymentStatus) {
        data.paymentStatus = body.paymentStatus as PaymentStatus;
      }
      if (body.paidAmount !== undefined) {
        const paid = Number(body.paidAmount) || 0;
        data.paidAmount = new Prisma.Decimal(paid);
      }

      // Update header trước
      await tx.invoice.update({
        where: { id },
        data,
      });

      // ===== THAY TOÀN BỘ LINES, NHƯNG CHỈ NHẬN DÒNG HỢP LỆ =====
      if (Array.isArray(body.lines)) {
        await tx.invoiceLine.deleteMany({ where: { invoiceId: id } });

        const validLines = body.lines
          .map((l: any) => ({
            ...l,
            itemId: l.itemId,
            qty: Number(l.qty || 0),
            price: Number(l.price || l.unitPrice || 0),
          }))
          .filter((l: any) => !!l.itemId && l.qty > 0);

        if (!validLines.length) {
          throw httpError(
            400,
            "Hoá đơn phải có ít nhất 1 sản phẩm (đã chọn từ danh sách)."
          );
        }

        let subtotal = 0;

        for (const l of validLines) {
          const amount = l.qty * l.price;
          subtotal += amount;

          await tx.invoiceLine.create({
            data: {
              invoiceId: id,
              itemId: l.itemId,
              qty: new Prisma.Decimal(l.qty),
              price: new Prisma.Decimal(l.price),
              amount: new Prisma.Decimal(amount),
              itemName: l.itemName || undefined,
              itemSku: l.itemSku || undefined,
            },
          });
        }

        // Tính lại tax & total từ body (taxPercent / tax), nếu không có -> 0
        const tax = calcTaxFromBody(subtotal, body);
        const total = subtotal + tax;

        // Nếu body không gửi paidAmount mới thì giữ nguyên giá trị cũ
        const invoiceOld = await tx.invoice.findUnique({
          where: { id },
          select: { paidAmount: true, paymentStatus: true },
        });

        let paidAmount =
          body.paidAmount !== undefined
            ? Number(body.paidAmount) || 0
            : toNum(invoiceOld?.paidAmount ?? 0);

        let paymentStatus: PaymentStatus =
          (body.paymentStatus as PaymentStatus | undefined) ??
          (paidAmount <= 0
            ? "UNPAID"
            : paidAmount >= total
            ? "PAID"
            : "PARTIAL");

        await tx.invoice.update({
          where: { id },
          data: {
            subtotal: new Prisma.Decimal(subtotal),
            tax: new Prisma.Decimal(tax),
            total: new Prisma.Decimal(total),
            paidAmount: new Prisma.Decimal(paidAmount),
            paymentStatus,
          },
        });
      }
      // Nếu không gửi body.lines thì giữ nguyên subtotal/tax/total cũ
    });
  } catch (e: any) {
    // Nếu là lỗi business (đã có statusCode) thì ném lại luôn
    if (e && e.statusCode) throw e;
    // Nếu là lỗi trùng mã => 400
    handleUniqueInvoiceError(e);
  }

  // trả về invoice đầy đủ
  return getInvoiceById(id);
}

export async function deleteInvoice(id: string) {
  // Chỉ xoá khi chưa post (không có movement liên kết)
  const hasMv = await prisma.movement.count({ where: { invoiceId: id } });
  if (hasMv > 0) {
    throw new Error(
      "Cannot delete posted invoice. Use HARD delete to rollback stock."
    );
  }
  await prisma.invoiceLine.deleteMany({ where: { invoiceId: id } });
  return prisma.invoice.delete({ where: { id } });
}

export async function addInvoiceLine(invoiceId: string, body: any) {
  const qty = Number(body.qty || 0);
  const price = Number(body.price || 0);
  const amount = qty * price;
  const line = await prisma.invoiceLine.create({
    data: {
      invoiceId,
      itemId: body.itemId,
      qty: new Prisma.Decimal(qty),
      price: new Prisma.Decimal(price),
      amount: new Prisma.Decimal(amount),
      itemName: body.itemName || undefined,
      itemSku: body.itemSku || undefined,
    },
  });
  await recomputeInvoiceTotals(invoiceId);
  return line;
}

export async function updateInvoiceLine(lineId: string, body: any) {
  const row = await prisma.invoiceLine.findUnique({ where: { id: lineId } });
  if (!row) throw new Error("Invoice line not found");

  const qty = body.qty != null ? Number(body.qty) : toNum(row.qty);
  const price = body.price != null ? Number(body.price) : toNum(row.price);
  const amount = qty * price;

  const line = await prisma.invoiceLine.update({
    where: { id: lineId },
    data: {
      qty: new Prisma.Decimal(qty),
      price: new Prisma.Decimal(price),
      amount: new Prisma.Decimal(amount),
      itemId: body.itemId || row.itemId,
      itemName: body.itemName || row.itemName || undefined,
      itemSku: body.itemSku || row.itemSku || undefined,
    },
  });
  await recomputeInvoiceTotals(row.invoiceId);
  return line;
}

export async function deleteInvoiceLine(lineId: string) {
  const row = await prisma.invoiceLine.findUnique({ where: { id: lineId } });
  if (!row) throw new Error("Invoice line not found");

  await prisma.invoiceLine.delete({ where: { id: lineId } });
  await recomputeInvoiceTotals(row.invoiceId);
  return true;
}

export async function linkMovement(invoiceId: string, movementId: string) {
  return prisma.movement.update({
    where: { id: movementId },
    data: { invoiceId },
  });
}

/** ========================= Posting ========================= **/

// ✅ VERSION MỚI: bấm Lưu tồn lần nữa sẽ tự rollback movement cũ + post lại
export async function postInvoiceToStock(
  invoiceId: string,
  warehouseId?: string
) {
  const invoice = await prisma.invoice.findUnique({
    where: { id: invoiceId },
    include: { lines: true, movements: { include: { lines: true } } },
  });
  if (!invoice) throw new Error("Invoice not found");

  const warehouse = await ensureWarehouse(warehouseId);

  const hadMovementsBefore = invoice.movements.length > 0;
  let createdInCount = 0;
  let createdOutCount = 0;

  await prisma.$transaction(async (tx) => {
    // 1️⃣ Nếu đã có movement cũ => rollback tồn + xoá movement cũ
    if (hadMovementsBefore) {
      const effectMap = new Map<string, number>(); // itemId -> signed qty

      for (const mv of invoice.movements) {
        for (const ml of mv.lines) {
          let signed = 0;

          if (mv.type === "IN" && ml.toLocationId === warehouse.id) {
            signed = +toNum(ml.qty);
          } else if (
            mv.type === "OUT" &&
            ml.fromLocationId === warehouse.id
          ) {
            signed = -toNum(ml.qty);
          } else if (mv.type === "ADJUST") {
            if (ml.toLocationId === warehouse.id) {
              signed = +toNum(ml.qty);
            }
            if (ml.fromLocationId === warehouse.id) {
              signed = -toNum(ml.qty);
            }
          }

          if (signed !== 0) {
            effectMap.set(
              ml.itemId,
              (effectMap.get(ml.itemId) || 0) + signed
            );
          }
        }
      }

      // rollback stock: delta = -signed
      for (const [itemId, signed] of effectMap.entries()) {
        if (signed === 0) continue;
        const delta = -signed;

        await tx.stock.upsert({
          where: {
            itemId_locationId: { itemId, locationId: warehouse.id },
          },
          create: {
            itemId,
            locationId: warehouse.id,
            qty: new Prisma.Decimal(delta),
          },
          update: {
            qty: { increment: new Prisma.Decimal(delta) },
          },
        });
      }

      // xoá movementLine + movement cũ
      const mvIds = invoice.movements.map((m) => m.id);
      if (mvIds.length) {
        await tx.movementLine.deleteMany({
          where: { movementId: { in: mvIds } },
        });
        await tx.movement.deleteMany({ where: { id: { in: mvIds } } });
      }
    }

    // 2️⃣ Tính lại ảnh hưởng cần ghi theo các dòng hiện tại của hoá đơn
    const desiredRows = invoice.lines.map((l) => ({
      itemId: l.itemId,
      qty: desiredSignedQty(invoice.type, toNum(l.qty)),
    }));
    const desiredMap = sumByItem(desiredRows);

    const adjustIn: Array<{ itemId: string; qty: number; locationId: string }> =
      [];
    const adjustOut: Array<{ itemId: string; qty: number; locationId: string }> =
      [];

    for (const [itemId, d] of desiredMap.entries()) {
      if (d > 0) adjustIn.push({ itemId, qty: d, locationId: warehouse.id });
      if (d < 0)
        adjustOut.push({
          itemId,
          qty: Math.abs(d),
          locationId: warehouse.id,
        });
    }

    createdInCount = adjustIn.length;
    createdOutCount = adjustOut.length;

    // 3️⃣ CHẶN TỒN ÂM VỚI HÓA ĐƠN BÁN HÀNG (SALES)
    if (invoice.type === "SALES" && adjustOut.length) {
      const itemIds = adjustOut.map((l) => l.itemId);

      const [stocks, items] = await Promise.all([
        tx.stock.findMany({
          where: {
            locationId: warehouse.id,
            itemId: { in: itemIds },
          },
        }),
        tx.item.findMany({
          where: { id: { in: itemIds } },
          select: { id: true, name: true, sku: true },
        }),
      ]);

      const stockMap = new Map<string, number>();
      stocks.forEach((s) => {
        stockMap.set(s.itemId, toNum(s.qty));
      });

      const nameMap = new Map<string, string>();
      items.forEach((it) => {
        nameMap.set(it.id, it.name || it.sku || it.id);
      });

      const errors: string[] = [];

      for (const l of adjustOut) {
        const current = stockMap.get(l.itemId) ?? 0;
        const name = nameMap.get(l.itemId) ?? l.itemId;

        if (current <= 0) {
          errors.push(`Sản phẩm "${name}" đã hết hàng trong kho.`);
        } else if (current < l.qty) {
          errors.push(
            `Sản phẩm "${name}" không đủ tồn kho (còn ${current}, cần ${l.qty}).`
          );
        }
      }

      if (errors.length) {
        // lỗi business, sẽ bị rollback toàn bộ transaction
        throw httpError(400, errors.join(" "));
      }
    }

    // 4️⃣ Tạo movement mới + cập nhật tồn
    if (adjustIn.length) {
      await tx.movement.create({
        data: {
          type: "IN",
          posted: true,
          invoiceId: invoice.id,
          lines: {
            create: adjustIn.map((l) => ({
              itemId: l.itemId,
              qty: new Prisma.Decimal(l.qty),
              toLocationId: l.locationId,
            })),
          },
        },
      });

      for (const l of adjustIn) {
        await tx.stock.upsert({
          where: {
            itemId_locationId: {
              itemId: l.itemId,
              locationId: l.locationId,
            },
          },
          create: {
            itemId: l.itemId,
            locationId: l.locationId,
            qty: new Prisma.Decimal(l.qty),
          },
          update: { qty: { increment: new Prisma.Decimal(l.qty) } },
        });
      }
    }

    if (adjustOut.length) {
      await tx.movement.create({
        data: {
          type: "OUT",
          posted: true,
          invoiceId: invoice.id,
          lines: {
            create: adjustOut.map((l) => ({
              itemId: l.itemId,
              qty: new Prisma.Decimal(l.qty),
              fromLocationId: l.locationId,
            })),
          },
        },
      });

      for (const l of adjustOut) {
        await tx.stock.upsert({
          where: {
            itemId_locationId: {
              itemId: l.itemId,
              locationId: l.locationId,
            },
          },
          create: {
            itemId: l.itemId,
            locationId: l.locationId,
            qty: new Prisma.Decimal(-l.qty),
          },
          update: { qty: { increment: new Prisma.Decimal(-l.qty) } },
        });
      }
    }
  });

  return {
    firstPost: !hadMovementsBefore,
    adjustIn: createdInCount,
    adjustOut: createdOutCount,
  };
}

/**
 * HỦY POST TỒN
 */
export async function unpostInvoiceStock(
  invoiceId: string,
  warehouseId?: string
) {
  const invoice = await prisma.invoice.findUnique({
    where: { id: invoiceId },
    include: { movements: { include: { lines: true } } },
  });
  if (!invoice) throw new Error("Invoice not found");
  if (!invoice.movements.length) {
    throw new Error("Hóa đơn này chưa post tồn, không cần hủy.");
  }

  const warehouse = await ensureWarehouse(warehouseId);

  // 1) Tính tổng ảnh hưởng đã ghi vào tồn (IN:+, OUT:-, ADJUST ±)
  const effectMap = new Map<string, number>(); // itemId -> signed qty
  for (const mv of invoice.movements) {
    for (const ml of mv.lines) {
      let signed = 0;
      if (mv.type === "IN" && ml.toLocationId === warehouse.id)
        signed = +toNum(ml.qty);
      else if (mv.type === "OUT" && ml.fromLocationId === warehouse.id)
        signed = -toNum(ml.qty);
      else if (mv.type === "ADJUST") {
        if (ml.toLocationId === warehouse.id) signed = +toNum(ml.qty);
        if (ml.fromLocationId === warehouse.id) signed = -toNum(ml.qty);
      }
      if (signed !== 0) {
        effectMap.set(ml.itemId, (effectMap.get(ml.itemId) || 0) + signed);
      }
    }
  }

  await prisma.$transaction(async (tx) => {
    // 2) rollback stock: delta = -signed
    for (const [itemId, signed] of effectMap.entries()) {
      if (signed === 0) continue;
      const delta = -signed;
      await tx.stock.upsert({
        where: {
          itemId_locationId: { itemId, locationId: warehouse.id },
        },
        create: {
          itemId,
          locationId: warehouse.id,
          qty: new Prisma.Decimal(delta),
        },
        update: {
          qty: { increment: new Prisma.Decimal(delta) },
        },
      });
    }

    // 3) xóa movementLine + movement
    const mvIds = invoice.movements.map((m) => m.id);
    if (mvIds.length) {
      await tx.movementLine.deleteMany({
        where: { movementId: { in: mvIds } },
      });
      await tx.movement.deleteMany({ where: { id: { in: mvIds } } });
    }
  });

  return { ok: true, message: "Đã hủy post tồn." };
}

/** ========================= HARD DELETE ========================= **/
export async function hardDeleteInvoice(id: string) {
  const inv = await prisma.invoice.findUnique({
    where: { id },
    include: { movements: { include: { lines: true } } },
  });
  if (!inv) throw new Error("Invoice not found");

  const warehouse = await ensureWarehouse();

  const effectMap = new Map<string, number>();
  for (const mv of inv.movements) {
    for (const ml of mv.lines) {
      let signed = 0;
      if (mv.type === "IN" && ml.toLocationId === warehouse.id)
        signed = +toNum(ml.qty);
      else if (mv.type === "OUT" && ml.fromLocationId === warehouse.id)
        signed = -toNum(ml.qty);
      else if (mv.type === "ADJUST") {
        if (ml.toLocationId === warehouse.id) signed = +toNum(ml.qty);
        if (ml.fromLocationId === warehouse.id) signed = -toNum(ml.qty);
      }
      if (signed !== 0)
        effectMap.set(
          ml.itemId,
          (effectMap.get(ml.itemId) || 0) + signed
        );
    }
  }

  const adjustIn: Array<{ itemId: string; qty: number; locationId: string }> =
    [];
  const adjustOut: Array<{ itemId: string; qty: number; locationId: string }> =
    [];

  for (const [itemId, signed] of effectMap.entries()) {
    if (signed > 0) {
      adjustOut.push({
        itemId,
        qty: Math.abs(signed),
        locationId: warehouse.id,
      });
    } else if (signed < 0) {
      adjustIn.push({
        itemId,
        qty: Math.abs(signed),
        locationId: warehouse.id,
      });
    }
  }

  await prisma.$transaction(async (tx) => {
    if (adjustIn.length) {
      await tx.movement.create({
        data: {
          type: "ADJUST",
          posted: true,
          note: `Rollback invoice ${inv.code} (increase)`,
          lines: {
            create: adjustIn.map((l) => ({
              itemId: l.itemId,
              qty: new Prisma.Decimal(l.qty),
              toLocationId: l.locationId,
            })),
          },
        },
      });
      for (const l of adjustIn) {
        await tx.stock.upsert({
          where: {
            itemId_locationId: { itemId: l.itemId, locationId: l.locationId },
          },
          create: {
            itemId: l.itemId,
            locationId: l.locationId,
            qty: new Prisma.Decimal(l.qty),
          },
          update: { qty: { increment: new Prisma.Decimal(l.qty) } },
        });
      }
    }

    if (adjustOut.length) {
      await tx.movement.create({
        data: {
          type: "ADJUST",
          posted: true,
          note: `Rollback invoice ${inv.code} (decrease)`,
          lines: {
            create: adjustOut.map((l) => ({
              itemId: l.itemId,
              qty: new Prisma.Decimal(l.qty),
              fromLocationId: l.locationId,
            })),
          },
        },
      });
      for (const l of adjustOut) {
        await tx.stock.upsert({
          where: {
            itemId_locationId: { itemId: l.itemId, locationId: l.locationId },
          },
          create: {
            itemId: l.itemId,
            locationId: l.locationId,
            qty: new Prisma.Decimal(-l.qty),
          },
          update: { qty: { increment: new Prisma.Decimal(-l.qty) } },
        });
      }
    }

    await tx.movement.deleteMany({ where: { invoiceId: id } });
    await tx.invoiceLine.deleteMany({ where: { invoiceId: id } });
    await tx.invoice.delete({ where: { id } });
  });

  return { ok: true, rolledBack: effectMap.size, deleted: true };
}

/** ========================= Revenue Aggregation ========================= **/
export async function aggregateRevenue(params: {
  from?: Date;
  to?: Date;
  type?: InvoiceType;
  saleUserId?: string;
  techUserId?: string;
  q?: string;
  paymentStatus?: PaymentStatus;
}) {
  const where: Prisma.InvoiceWhereInput = {};
  if (params.type) where.type = params.type;
  if (params.saleUserId) where.saleUserId = params.saleUserId as any;
  if (params.techUserId) where.techUserId = params.techUserId as any;
  if (params.from || params.to) {
    where.issueDate = {};
    if (params.from) (where.issueDate as any).gte = params.from;
    if (params.to) (where.issueDate as any).lte = params.to;
  }
  if (params.q) {
    (where as any).OR = [
      { code: { contains: params.q, mode: "insensitive" } },
      { partnerName: { contains: params.q, mode: "insensitive" } },
    ];
  }
  if (params.paymentStatus) {
    where.paymentStatus = params.paymentStatus;
  }

  const rows = await prisma.invoice.findMany({
    where,
    select: {
      id: true,
      code: true,
      issueDate: true,
      total: true,
      paidAmount: true,
      paymentStatus: true,
      saleUserId: true,
      saleUserName: true,
      techUserId: true,
      techUserName: true,
    },
    orderBy: { issueDate: "asc" },
  });

  // Doanh thu thực tế = số tiền đã thu
  const grandTotal = rows.reduce(
    (s, r) => s + toNum(r.paidAmount ?? 0),
    0
  );

  const bySale = new Map<
    string,
    { userId: string | null; name: string; total: number; count: number }
  >();
  const byTech = new Map<
    string,
    { userId: string | null; name: string; total: number; count: number }
  >();

  for (const r of rows) {
    const amount = toNum(r.paidAmount ?? 0);

    const saleKey = r.saleUserId || r.saleUserName || "UNKNOWN";
    const saleName = r.saleUserName || r.saleUserId || "UNKNOWN";
    const s0 =
      bySale.get(saleKey) || {
        userId: r.saleUserId,
        name: saleName,
        total: 0,
        count: 0,
      };
    s0.total += amount;
    s0.count += 1;
    bySale.set(saleKey, s0);

    const techKey = r.techUserId || r.techUserName || "UNKNOWN";
    const techName = r.techUserName || r.techUserId || "UNKNOWN";
    const t0 =
      byTech.get(techKey) || {
        userId: r.techUserId,
        name: techName,
        total: 0,
        count: 0,
      };
    t0.total += amount;
    t0.count += 1;
    byTech.set(techKey, t0);
  }

  return {
    totalInvoices: rows.length,
    grandTotal,
    bySale: Array.from(bySale.values()).sort((a, b) => b.total - a.total),
    byTech: Array.from(byTech.values()).sort((a, b) => b.total - a.total),
  };
}
