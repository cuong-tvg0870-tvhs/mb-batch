import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger } from '@nestjs/common';
import { Queue } from 'bull';
import { randomUUID } from 'crypto';

// Bull queue chỉ dùng để MƯỢN kết nối Redis (ioredis) đã cấu hình sẵn — KHÔNG đăng ký
// processor nên không có job nào chạy trên nó. Tránh phải thêm client/dep Redis mới.
export const DISTRIBUTED_LOCK_QUEUE = 'distributed-lock';

/**
 * Trần thời gian cho MỘT lệnh Redis (giành/nhả khóa).
 *
 * VÌ SAO CẦN (sự cố prod 05/08/2026 — engine campaign-rule đứng 5h45): fail-open bên dưới
 * chỉ chạy khi Redis NÉM LỖI. Nhưng ioredis mặc định bật `enableOfflineQueue`: khi kết nối
 * chết/wedged, lệnh KHÔNG lỗi mà bị XẾP HÀNG chờ reconnect — `await client.set(...)` treo
 * VÔ HẠN. Cron gọi runExclusive vì thế đứng im mà không hề có lỗi, và cờ chống chồng tick
 * của scheduler mắc luôn ⇒ toàn bộ rule ngừng chạy.
 *
 * Bằng chứng khoanh vùng: mốc treo nằm TRƯỚC khi dòng campaign_rule_run được tạo (0 dòng
 * status=RUNNING sau 08:22), tức không phải treo ở lời gọi Meta — chỉ còn đúng lệnh Redis
 * này là await không có trần thời gian trên đường đi đó.
 *
 * 3 giây là dư cho một lệnh SET/EVAL trên Redis nội bộ; quá đó coi như Redis không dùng
 * được và fail-open (chạy KHÔNG khóa) — cùng quyết định đã có sẵn cho trường hợp Redis lỗi.
 */
export const REDIS_OP_TIMEOUT_MS = 3000;

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
   * Chặn một lệnh Redis treo vô hạn: hết `REDIS_OP_TIMEOUT_MS` thì REJECT để nhánh
   * fail-open phía dưới xử lý. Lệnh gốc vẫn có thể settle muộn (không huỷ được lệnh
   * ioredis) nhưng không ai còn chờ nó nữa.
   */
  private withTimeout<T>(op: Promise<T>, what: string): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(
        () =>
          reject(
            new Error(`Redis ${what} không trả lời trong ${REDIS_OP_TIMEOUT_MS}ms`),
          ),
        REDIS_OP_TIMEOUT_MS,
      );
      // `unref` để timer không giữ event loop khi process muốn thoát (graceful shutdown).
      timer.unref?.();
      op.then(
        (v) => {
          clearTimeout(timer);
          resolve(v);
        },
        (e) => {
          clearTimeout(timer);
          reject(e);
        },
      );
    });
  }

  /**
   * Chạy `fn` dưới khóa Redis độc quyền theo `key`.
   * - Chiếm được khóa → chạy `fn`, trả về `true`.
   * - Instance khác đang giữ khóa → BỎ QUA, trả về `false` (KHÔNG chạy `fn`).
   * - FAIL-OPEN: Redis không truy cập được → vẫn chạy `fn` (coi như single-instance)
   *   để một sự cố Redis không làm treo toàn bộ cron. Fail-open áp dụng cho CẢ trường hợp
   *   Redis KHÔNG lỗi mà chỉ "không trả lời" — xem REDIS_OP_TIMEOUT_MS.
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
        const res = await this.withTimeout(
          client.set(lockKey, token, 'EX', ttlSeconds, 'NX'),
          `SET ${lockKey}`,
        );
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
          await this.withTimeout(
            client.eval(releaseLua, 1, lockKey, token),
            `EVAL release ${lockKey}`,
          );
        } catch {
          // best-effort: khóa sẽ tự hết hạn theo TTL.
        }
      }
    }
  }
}
