import { PrismaClient } from '@prisma/client';
const prisma = new PrismaClient();

export async function listAssets(q?:string) {
  const where = q ? { OR: [{ assetCode: { contains:q, mode:'insensitive' }}, { name: { contains:q, mode:'insensitive' }}]} : {};
  return prisma.asset.findMany({ where, orderBy:{createdAt:'desc'}});
}
export async function createAsset(data:any){ return prisma.asset.create({ data }); }
export async function assignAsset(assetCode:string, locationId:string, note?:string){
  return prisma.assetAssignment.create({ data: { assetCode, locationId, note }});
}
