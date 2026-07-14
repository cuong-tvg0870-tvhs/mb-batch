import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import axios from 'axios';

/**
 * Cron 5': đánh thức mb-ads chạy các "Quy tắc tự lên Camp" tới hạn theo khung giờ.
 *
 * Logic build+publish nằm ở mb-ads (dùng DraftCampaignService) nên mb-batch KHÔNG tự
 * dựng camp — chỉ POST kích hoạt `/internal/auto-launch/run-due`. Chống chạy trùng đã
 * lo bằng `dedupeKey @unique` bên mb-ads (2 replica cùng gọi cũng chỉ 1 run/slot).
 *
 * Env cần set để bật: AUTO_LAUNCH_INTERNAL_SECRET (khớp mb-ads) + AUTO_LAUNCH_ADS_URL
 * (mặc định http://localhost:8000). Chưa set secret ⇒ scheduler tự bỏ qua.
 */
@Injectable()
export class AutoLaunchTriggerScheduler {
  private readonly logger = new Logger(AutoLaunchTriggerScheduler.name);

  @Cron('*/5 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async tick() {
    const secret = process.env.AUTO_LAUNCH_INTERNAL_SECRET;
    if (!secret) return; // chưa cấu hình → chưa bật tự động (an toàn)

    const base = process.env.AUTO_LAUNCH_ADS_URL || 'http://localhost:8000';
    try {
      const res = await axios.post(
        `${base}/internal/auto-launch/run-due`,
        {},
        { headers: { 'x-internal-secret': secret }, timeout: 120_000 },
      );
      const ran = res.data?.ran ?? 0;
      const skipped = res.data?.skipped ?? 0;
      if (ran > 0) {
        this.logger.log(`auto-launch run-due: ran=${ran} skipped=${skipped}`);
      }
    } catch (e: any) {
      this.logger.error(
        `auto-launch run-due failed: ${e?.response?.status ?? ''} ${e?.message ?? ''}`,
      );
    }
  }
}
