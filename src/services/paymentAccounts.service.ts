import { PrismaClient, UserRole, AccountType } from "@prisma/client";

const prisma = new PrismaClient();

type Actor = { id: string; role: UserRole };

function httpError(status: number, message: string) {
  const err: any = new Error(message);
  err.statusCode = status;
  return err;
}

function assertAdmin(actor: Actor) {
  if (actor.role !== "admin") throw httpError(403, "Chỉ admin được thao tác.");
}

export async function listPaymentAccounts(actor: Actor, activeOnly = true) {
  // staff cũng được list để dropdown, chỉ lấy active
  return prisma.paymentAccount.findMany({
    where: activeOnly ? { isActive: true } : {},
    orderBy: [{ type: "asc" }, { code: "asc" }],
    select: {
      id: true,
      code: true,
      name: true,
      type: true,
      bankName: true,
      accountNo: true,
      holder: true,
      isActive: true,
      note: true,
      createdAt: true,
      updatedAt: true,
    },
  });
}

export async function createPaymentAccount(actor: Actor, body: any) {
  assertAdmin(actor);

  const code = String(body.code || "").trim();
  const name = String(body.name || "").trim();
  if (!code || !name) throw httpError(400, "Thiếu code hoặc name");

  const type: AccountType = body.type ?? "BANK";

  return prisma.paymentAccount.create({
    data: {
      code,
      name,
      type,
      bankName: body.bankName ?? null,
      accountNo: body.accountNo ?? null,
      holder: body.holder ?? null,
      isActive: body.isActive ?? true,
      note: body.note ?? null,
    },
  });
}

export async function updatePaymentAccount(actor: Actor, id: string, body: any) {
  assertAdmin(actor);

  return prisma.paymentAccount.update({
    where: { id },
    data: {
      code: body.code !== undefined ? String(body.code).trim() : undefined,
      name: body.name !== undefined ? String(body.name).trim() : undefined,
      type: body.type ?? undefined,
      bankName: body.bankName ?? undefined,
      accountNo: body.accountNo ?? undefined,
      holder: body.holder ?? undefined,
      isActive: body.isActive ?? undefined,
      note: body.note ?? undefined,
    },
  });
}

// Soft delete: set inactive để giữ lịch sử payment
export async function deactivatePaymentAccount(actor: Actor, id: string) {
  assertAdmin(actor);

  return prisma.paymentAccount.update({
    where: { id },
    data: { isActive: false },
  });
}
