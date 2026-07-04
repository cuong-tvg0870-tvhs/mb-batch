import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { UserLarkSyncService } from './user-lark-sync.service';

/**
 * Cron đồng bộ User ↔ danh bạ Lark. Mỗi ngày 03:00 (giờ VN). Guard in-process
 * `running` để một lượt chưa xong thì lượt sau bỏ qua (tránh chồng chéo lâu).
 */
@Injectable()
export class UserLarkSyncScheduler {
  private readonly logger = new Logger(UserLarkSyncScheduler.name);
  private running = false;

  constructor(private readonly service: UserLarkSyncService) {}

  @Cron(CronExpression.EVERY_DAY_AT_3AM, { timeZone: 'Asia/Ho_Chi_Minh' })
  async handleCron() {
    if (this.running) {
      this.logger.warn('Lượt sync User↔Lark trước chưa xong → bỏ qua lượt này');
      return;
    }
    this.running = true;
    try {
      this.logger.log('📅 Bắt đầu đồng bộ User ↔ danh bạ Lark...');
      await this.service.syncAll();
    } catch (e: any) {
      this.logger.error(`Sync User↔Lark lỗi: ${e?.message || e}`);
    } finally {
      this.running = false;
    }
  }
}
