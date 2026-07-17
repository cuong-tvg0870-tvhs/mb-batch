import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import axios from 'axios';
import { DistributedLockService } from '../distributed-lock/distributed-lock.service';

// Publish ĐỒNG BỘ (awaitPublish) → 1 lượt run-due có thể lâu (nhiều trang × gọi Meta). Đặt
// timeout axios + TTL khóa RỘNG để axios chờ trọn lượt (giữ khóa suốt lượt) → tick sau thấy
// khóa còn giữ = bỏ qua, KHÔNG chồng lượt. TTL > timeout > worst-case-1-lượt. (mb-ads còn có
// in-process mutex runDueRules làm backstop nếu lượt vượt cả timeout.)
const RUN_DUE_TIMEOUT_MS = 9.5 * 60 * 1000; // 570s
const LOCK_TTL = 10 * 60; // 600s (> timeout, > cron 300s)

/**
 * Cron 5': đánh thức mb-ads chạy các "Quy tắc tự lên Camp" tới hạn theo khung giờ.
 *
 * Logic build+publish nằm ở mb-ads nên mb-batch chỉ POST `/internal/auto-launch/run-due`.
 * CHỐNG CHỒNG/ĐĂNG-ĐÔI 3 lớp: (1) `running` in-process chống chồng tick cùng process nếu lượt
 * trước chưa xong (run-due đồng bộ có thể >5' khi publish nhiều); (2) DistributedLock Redis NX
 * chống 2 replica cùng bắn cron; (3) claim-first `dedupeKey @unique` bên mb-ads (chốt cuối).
 *
 * Env cần set để bật: AUTO_LAUNCH_INTERNAL_SECRET (khớp mb-ads) + AUTO_LAUNCH_ADS_URL
 * (mặc định http://localhost:8000). Chưa set secret ⇒ scheduler tự bỏ qua.
 */
@Injectable()
export class AutoLaunchTriggerScheduler {
  private readonly logger = new Logger(AutoLaunchTriggerScheduler.name);
  private running = false;

  constructor(private readonly lock: DistributedLockService) {}

  @Cron('*/5 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async tick() {
    const secret = process.env.AUTO_LAUNCH_INTERNAL_SECRET;
    if (!secret) return; // chưa cấu hình → chưa bật tự động (an toàn)

    if (this.running) {
      this.logger.warn('Lượt auto-launch run-due trước chưa xong → bỏ qua lượt này.');
      return;
    }
    this.running = true;
    try {
      const ran = await this.lock.runExclusive('auto-launch-run-due', LOCK_TTL, async () => {
        const base = process.env.AUTO_LAUNCH_ADS_URL || 'http://localhost:8000';
        const res = await axios.post(
          `${base}/internal/auto-launch/run-due`,
          {},
          { headers: { 'x-internal-secret': secret }, timeout: RUN_DUE_TIMEOUT_MS },
        );
        const r = res.data?.ran ?? 0;
        const skipped = res.data?.skipped ?? 0;
        if (r > 0) this.logger.log(`auto-launch run-due: ran=${r} skipped=${skipped}`);
      });
      if (!ran) {
        this.logger.warn('Instance khác đang chạy auto-launch run-due → bỏ qua lượt này.');
      }
    } catch (e: any) {
      this.logger.error(
        `auto-launch run-due failed: ${e?.response?.status ?? ''} ${e?.message ?? ''}`,
      );
    } finally {
      this.running = false;
    }
  }
}
