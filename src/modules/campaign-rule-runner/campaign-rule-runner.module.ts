import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { CampaignRuleRunnerScheduler } from './campaign-rule-runner.scheduler';
import { CampaignRuleRunnerService } from './campaign-rule-runner.service';
import { CampaignRuleSyncService } from './campaign-rule-sync.service';

/**
 * Runner "campaign rule": cron chạy nhánh "Theo điều kiện" của engine rule scheduling.
 * PrismaModule & DistributedLockModule là @Global nên chỉ cần import PrismaModule cho rõ.
 * Các helper (schedule/evaluator/metric/executor) là hàm thuần → không cần provider.
 */
@Module({
  imports: [PrismaModule],
  providers: [
    CampaignRuleRunnerService,
    CampaignRuleRunnerScheduler,
    CampaignRuleSyncService,
  ],
  exports: [CampaignRuleRunnerService, CampaignRuleSyncService],
})
export class CampaignRuleRunnerModule {}
