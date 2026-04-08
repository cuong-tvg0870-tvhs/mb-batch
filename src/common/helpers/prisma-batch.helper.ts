import { PrismaClient } from '@prisma/client';

export class PrismaBatchHelper {
  constructor(private prisma: PrismaClient) {}

  /**
   * Chunk array để tránh overload RAM
   */
  chunkArray<T>(arr: T[], size = 100) {
    const chunks: T[][] = [];
    for (let i = 0; i < arr.length; i += size) {
      chunks.push(arr.slice(i, i + size));
    }
    return chunks;
  }

  /**
   * createMany theo batch
   */
  async createManySafe<T>(model: any, data: T[], chunkSize = 200) {
    const chunks = this.chunkArray(data, chunkSize);

    for (const chunk of chunks) {
      await model.createMany({
        data: chunk,
        skipDuplicates: true,
      });
    }
  }

  /**
   * upsert hàng loạt (transaction nhỏ)
   */
  async upsertMany<T>(
    items: T[],
    handler: (item: T) => Promise<any>,
    chunkSize = 20,
  ) {
    const chunks = this.chunkArray(items, chunkSize);

    for (const chunk of chunks) {
      await Promise.all(chunk.map((item) => handler(item)));
    }
  }
}
