import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { Job } from 'bull';
import { BatchRunLoggerService } from '../batch-run-log/batch-run-logger.service';
import {
  INSIGHT_SYNC_JOBS,
  INSIGHT_SYNC_QUEUE,
  SyncAccountJobData,
} from './insight-sync.constants';
import { InsightSyncService } from './insight-sync.service';

@Processor(INSIGHT_SYNC_QUEUE)
export class InsightSyncProcessor {
  private readonly logger = new Logger(InsightSyncProcessor.name);

  constructor(
    private readonly syncService: InsightSyncService,
    private readonly batchRunLogger: BatchRunLoggerService,
  ) {}

  @Process({
    name: INSIGHT_SYNC_JOBS.SYNC_ACCOUNT,
    concurrency: Number(process.env.INSIGHT_SYNC_ACCOUNT_CONCURRENCY || 3),
  })
  async handleSyncAccount(job: Job<SyncAccountJobData>) {
    const { accountId, levels, ranges } = job.data;
    return this.batchRunLogger.track(
      INSIGHT_SYNC_JOBS.SYNC_ACCOUNT,
      INSIGHT_SYNC_QUEUE,
      async (ctx) => {
        ctx.setTotal(1);
        ctx.setMeta({ accountId, levels, ranges });
        const start = Date.now();
        this.logger.log(
          `🚀 [JOB START] Account: ${accountId} | Levels: ${levels.join(',')} | Ranges: ${ranges.join(',')}`,
        );
        await this.syncService.syncAccountInsights(accountId, levels, ranges);
        ctx.addSuccess(1);
        const duration = ((Date.now() - start) / 1000).toFixed(2);
        this.logger.log(
          `✨ [JOB FINISHED] Account: ${accountId} | Total Duration: ${duration}s`,
        );
      },
    );
  }

  @Process({
    name: INSIGHT_SYNC_JOBS.SYNC_AUDIENCE,
    concurrency: 5,
  })
  async handleSyncAudience(job: Job<{ accountId: string }>) {
    const { accountId } = job.data;
    return this.batchRunLogger.track(
      INSIGHT_SYNC_JOBS.SYNC_AUDIENCE,
      INSIGHT_SYNC_QUEUE,
      async (ctx) => {
        ctx.setTotal(1);
        ctx.setMeta({ accountId });
        const start = Date.now();
        this.logger.log(`🚀 [JOB START] Audience sync for Account: ${accountId}`);
        await this.syncService.syncAccountAudienceInsights(accountId);
        ctx.addSuccess(1);
        const duration = ((Date.now() - start) / 1000).toFixed(2);
        this.logger.log(
          `✨ [JOB FINISHED] Audience sync for Account: ${accountId} | Duration: ${duration}s`,
        );
      },
    );
  }

  @Process({
    name: INSIGHT_SYNC_JOBS.SYNC_MISSING_DAILY,
    concurrency: 2, // Low concurrency because daily sync is heavy
  })
  async handleSyncMissingDaily(job: Job<{ accountId: string }>) {
    const { accountId } = job.data;
    return this.batchRunLogger.track(
      INSIGHT_SYNC_JOBS.SYNC_MISSING_DAILY,
      INSIGHT_SYNC_QUEUE,
      async (ctx) => {
        ctx.setTotal(1);
        ctx.setMeta({ accountId });
        const start = Date.now();
        this.logger.log(
          `🚀 [JOB START] Missing Daily sync for Account: ${accountId}`,
        );
        await this.syncService.syncAccountMissingDailyInsights(accountId);
        ctx.addSuccess(1);
        const duration = ((Date.now() - start) / 1000).toFixed(2);
        this.logger.log(
          `✨ [JOB FINISHED] Missing Daily sync for Account: ${accountId} | Duration: ${duration}s`,
        );
      },
    );
  }

  @Process({
    name: INSIGHT_SYNC_JOBS.SYNC_LIFETIME_BACKFILL,
    concurrency: Number(process.env.INSIGHT_LIFETIME_BACKFILL_CONCURRENCY || 1),
  })
  async handleLifetimeBackfill(job: Job<{ accountId: string }>) {
    const { accountId } = job.data;
    return this.batchRunLogger.track(
      INSIGHT_SYNC_JOBS.SYNC_LIFETIME_BACKFILL,
      INSIGHT_SYNC_QUEUE,
      async (ctx) => {
        ctx.setTotal(1);
        ctx.setMeta({ accountId });
        const start = Date.now();
        this.logger.log(
          `🚀 [JOB START] Lifetime backfill for Account: ${accountId}`,
        );
        await this.syncService.backfillLifetimeDailyInsights(accountId);
        ctx.addSuccess(1);
        const duration = ((Date.now() - start) / 1000).toFixed(2);
        this.logger.log(
          `✨ [JOB FINISHED] Lifetime backfill for Account: ${accountId} | Duration: ${duration}s`,
        );
      },
    );
  }
}
