import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { DistributedLockService } from '../distributed-lock/distributed-lock.service';
import { UserLarkSyncService } from './user-lark-sync.service';

// TTL khóa cross-replica (giây). Tải toàn bộ danh bạ Lark có thể lâu → để rộng 1 giờ.
const LOCK_TTL = 60 * 60;

/**
 * Cron đồng bộ User ↔ danh bạ Lark. Mỗi ngày 03:00 (giờ VN). Guard in-process
 * `running` chống chồng lượt cùng process; khóa Redis chống double-run khi chạy
 * nhiều replica (mỗi replica cũng bắn cron này).
 */
@Injectable()
export class UserLarkSyncScheduler {
  private readonly logger = new Logger(UserLarkSyncScheduler.name);
  private running = false;

  constructor(
    private readonly service: UserLarkSyncService,
    private readonly lock: DistributedLockService,
  ) {}

  @Cron(CronExpression.EVERY_DAY_AT_3AM, { timeZone: 'Asia/Ho_Chi_Minh' })
  async handleCron() {
    if (this.running) {
      this.logger.warn('Lượt sync User↔Lark trước chưa xong → bỏ qua lượt này');
      return;
    }
    this.running = true;
    try {
      const ran = await this.lock.runExclusive('user-lark-sync', LOCK_TTL, async () => {
        this.logger.log('📅 Bắt đầu đồng bộ User ↔ danh bạ Lark...');
        await this.service.syncAll();
      });
      if (!ran) {
        this.logger.warn('Instance khác đang chạy sync User↔Lark → bỏ qua lượt này');
      }
    } catch (e: any) {
      this.logger.error(`Sync User↔Lark lỗi: ${e?.message || e}`);
    } finally {
      this.running = false;
    }
  }
}
