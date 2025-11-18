import { PrismaClient } from '@prisma/client';
const prisma = new PrismaClient();

export async function listLocations() { return prisma.location.findMany({ orderBy: { code: 'asc' }}); }
export async function createLocation(data:any){ return prisma.location.create({ data }); }
export async function updateLocation(id:string, data:any){ return prisma.location.update({ where: { id }, data }); }
export async function removeLocation(id:string){ return prisma.location.delete({ where: { id }}); }
