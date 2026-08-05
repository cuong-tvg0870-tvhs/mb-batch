import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import {
  CAMPAIGN_RULE_TICK_CRON,
  CAMPAIGN_RULE_TICK_TIMEZONE,
  TICK_MAX_WALL_MS,
} from './campaign-rule-runner.constants';
import { CampaignRuleRunnerService } from './campaign-rule-runner.service';

/**
 * Tick cron mỗi 5 phút cho runner campaign rule. Có cờ in-memory chống chồng tick:
 * nếu lượt trước chưa xong (rule chậm / nhiều entity) thì bỏ qua lượt này. Chống
 * double-run cross-replica do DistributedLockService + dedupeKey trong service đảm nhận.
 *
 * WATCHDOG (sự cố prod 05/08/2026 — engine đứng 5h45, 188 rule không chạy lần nào):
 * cờ chống chồng tick trước đây là boolean nhả trong `finally`. Một lượt `runDueRules()`
 * treo vĩnh viễn (socket Meta không timeout) ⇒ `finally` không bao giờ chạy ⇒ cờ mắc
 * `true` mãi ⇒ TOÀN BỘ rule ngừng được kiểm tra, âm thầm, tới khi ai đó restart process.
 * Nay cờ là MỐC THỜI GIAN: quá `TICK_MAX_WALL_MS` thì tick mới được cướp lượt và chạy
 * tiếp. Bơm ngân sách hai lần vẫn không xảy ra vì mỗi rule còn khóa phân tán
 * `crr:<ruleId>` (TTL 300s) và mỗi slot còn unique `dedupeKey` — đúng cơ chế đang bảo vệ
 * trường hợp nhiều replica cùng chạy.
 */
@Injectable()
export class CampaignRuleRunnerScheduler implements OnModuleInit {
  private readonly logger = new Logger(CampaignRuleRunnerScheduler.name);
  /** Mốc bắt đầu của lượt đang chạy (epoch ms); null = rảnh. */
  private runningSince: number | null = null;

  constructor(private readonly service: CampaignRuleRunnerService) {}

  onModuleInit() {
    this.logger.log('🚀 CampaignRuleRunnerScheduler Initialized');
  }

  @Cron(CAMPAIGN_RULE_TICK_CRON, { timeZone: CAMPAIGN_RULE_TICK_TIMEZONE })
  async tick() {
    const previous = this.runningSince;
    if (previous !== null) {
      const ageMs = Date.now() - previous;
      if (ageMs < TICK_MAX_WALL_MS) {
        this.logger.warn(
          `Tick trước chưa xong (${Math.round(ageMs / 1000)}s) → bỏ qua lượt này.`,
        );
        return;
      }
      // Quá trần: coi lượt cũ là TREO. Cướp lượt để engine không đứng vô hạn — log ERROR
      // để lộ ra ngoài (đây là dấu hiệu một lời gọi Meta không tôn trọng timeout).
      this.logger.error(
        `Tick trước TREO ${Math.round(ageMs / 1000)}s > trần ${
          TICK_MAX_WALL_MS / 1000
        }s → cướp lượt và chạy tiếp (khóa phân tán + dedupeKey vẫn chặn chạy trùng).`,
      );
    }

    const startedAt = Date.now();
    this.runningSince = startedAt;
    try {
      await this.service.runDueRules();
    } catch (error) {
      this.logger.error(`runDueRules lỗi: ${error?.message || error}`);
    } finally {
      // CHỈ nhả cờ nếu cờ vẫn là của lượt này. Lượt bị cướp mà sau đó settle muộn thì
      // KHÔNG được xoá cờ của lượt đang chạy (nếu không, hai lượt sẽ chồng nhau thật).
      if (this.runningSince === startedAt) this.runningSince = null;
    }
  }
}
