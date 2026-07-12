import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger } from '@nestjs/common';
import { Queue } from 'bull';
import { randomUUID } from 'crypto';

// Bull queue chỉ dùng để MƯỢN kết nối Redis (ioredis) đã cấu hình sẵn — KHÔNG đăng ký
// processor nên không có job nào chạy trên nó. Tránh phải thêm client/dep Redis mới.
export const DISTRIBUTED_LOCK_QUEUE = 'distributed-lock';

/**
 * Khóa phân tán qua Redis cho các cron chạy INLINE trong process (không qua Bull).
 *
 * Vì sao cần: các cron như help-ai / user-lark-sync chạy thẳng trong tiến trình và chỉ
 * có cờ in-memory chống chồng lượt. Nếu mb-batch chạy >1 instance (multi-replica) thì
 * MỖI replica sẽ chạy lượt đó → double-run (refresh knowledge, sync user 2 lần...).
 * `SET key token NX EX ttl` là thao tác NGUYÊN TỬ cross-replica: chỉ 1 instance chiếm
 * được khóa mỗi chu kỳ. Khóa tự hết hạn theo TTL nên nếu instance giữ khóa chết giữa
 * chừng, chu kỳ sau vẫn chạy được (tự phục hồi).
 */
@Injectable()
export class DistributedLockService {
  private readonly logger = new Logger(DistributedLockService.name);

  constructor(
    @InjectQueue(DISTRIBUTED_LOCK_QUEUE) private readonly queue: Queue,
  ) {}

  /**
   * Chạy `fn` dưới khóa Redis độc quyền theo `key`.
   * - Chiếm được khóa → chạy `fn`, trả về `true`.
   * - Instance khác đang giữ khóa → BỎ QUA, trả về `false` (KHÔNG chạy `fn`).
   * - FAIL-OPEN: Redis không truy cập được → vẫn chạy `fn` (coi như single-instance)
   *   để một sự cố Redis không làm treo toàn bộ cron.
   *
   * Lỗi từ `fn` được ném ra ngoài như bình thường (khóa vẫn được nhả trong `finally`).
   *
   * @param ttlSeconds nên LỚN HƠN thời lượng chạy tối đa của job để khóa không hết hạn
   *        giữa chừng làm một instance khác chen vào.
   */
  async runExclusive(
    key: string,
    ttlSeconds: number,
    fn: () => Promise<unknown>,
  ): Promise<boolean> {
    const lockKey = `lock:cron:${key}`;
    const token = randomUUID();
    const client: any = (this.queue as any).client;

    let acquired = false;
    if (client) {
      try {
        const res = await client.set(lockKey, token, 'EX', ttlSeconds, 'NX');
        if (res !== 'OK') {
          this.logger.warn(
            `Khóa "${key}" đang được instance khác giữ → bỏ qua lượt này.`,
          );
          return false;
        }
        acquired = true;
      } catch (e: any) {
        this.logger.warn(
          `Redis lock "${key}" lỗi: ${e?.message || e}. Chạy KHÔNG khóa (fail-open).`,
        );
      }
    }

    try {
      await fn();
      return true;
    } finally {
      if (acquired && client) {
        try {
          // Chỉ xoá khi token còn KHỚP: tránh nhả nhầm khóa mà instance khác đã chiếm
          // lại sau khi khóa của mình vô tình hết hạn (chạy quá TTL).
          const releaseLua =
            'if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end';
          await client.eval(releaseLua, 1, lockKey, token);
        } catch {
          // best-effort: khóa sẽ tự hết hạn theo TTL.
        }
      }
    }
  }
}
