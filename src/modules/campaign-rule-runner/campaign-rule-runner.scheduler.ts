import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import {
  CAMPAIGN_RULE_TICK_CRON,
  CAMPAIGN_RULE_TICK_TIMEZONE,
} from './campaign-rule-runner.constants';
import { CampaignRuleRunnerService } from './campaign-rule-runner.service';

/**
 * Tick cron mỗi 5 phút cho runner campaign rule. Có cờ in-memory chống chồng tick:
 * nếu lượt trước chưa xong (rule chậm / nhiều entity) thì bỏ qua lượt này. Chống
 * double-run cross-replica do DistributedLockService + dedupeKey trong service đảm nhận.
 */
@Injectable()
export class CampaignRuleRunnerScheduler implements OnModuleInit {
  private readonly logger = new Logger(CampaignRuleRunnerScheduler.name);
  private running = false;

  constructor(private readonly service: CampaignRuleRunnerService) {}

  onModuleInit() {
    this.logger.log('🚀 CampaignRuleRunnerScheduler Initialized');
  }

  @Cron(CAMPAIGN_RULE_TICK_CRON, { timeZone: CAMPAIGN_RULE_TICK_TIMEZONE })
  async tick() {
    if (this.running) {
      this.logger.warn('Tick trước chưa xong → bỏ qua lượt này.');
      return;
    }
    this.running = true;
    try {
      await this.service.runDueRules();
    } catch (error) {
      this.logger.error(`runDueRules lỗi: ${error?.message || error}`);
    } finally {
      this.running = false;
    }
  }
}
