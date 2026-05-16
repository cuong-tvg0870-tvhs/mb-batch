import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

async function main() {
  const folders = await prisma.creativeFolder.findMany({
    where: {
      OR: [
        { parentId: null },
        { parentId: '4303729193176038' }
      ]
    },
    select: { id: true, name: true, parentId: true }
  });
  console.log(JSON.stringify(folders, null, 2));
}

main().catch(console.error).finally(() => prisma.$disconnect());
