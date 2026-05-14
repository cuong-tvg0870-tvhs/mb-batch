import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { Job } from 'bull';
import {
  INSIGHT_SYNC_JOBS,
  INSIGHT_SYNC_QUEUE,
  SyncAccountJobData,
} from './insight-sync.constants';
import { InsightSyncService } from './insight-sync.service';

@Processor(INSIGHT_SYNC_QUEUE)
export class InsightSyncProcessor {
  private readonly logger = new Logger(InsightSyncProcessor.name);

  constructor(private readonly syncService: InsightSyncService) {}

  @Process({
    name: INSIGHT_SYNC_JOBS.SYNC_ACCOUNT,
    concurrency: 10,
  })
  async handleSyncAccount(job: Job<SyncAccountJobData>) {
    const { accountId, levels, ranges } = job.data;
    const start = Date.now();

    this.logger.log(
      `🚀 [JOB START] Account: ${accountId} | Levels: ${levels.join(',')} | Ranges: ${ranges.join(',')}`,
    );

    try {
      await this.syncService.syncAccountInsights(accountId, levels, ranges);
      const duration = ((Date.now() - start) / 1000).toFixed(2);
      this.logger.log(
        `✨ [JOB FINISHED] Account: ${accountId} | Total Duration: ${duration}s`,
      );
    } catch (error) {
      this.logger.error(
        `❌ [JOB FAILED] Account: ${accountId} | Error: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error;
    }
  }

  @Process({
    name: INSIGHT_SYNC_JOBS.SYNC_AUDIENCE,
    concurrency: 5,
  })
  async handleSyncAudience(job: Job<{ accountId: string }>) {
    const { accountId } = job.data;
    const start = Date.now();

    this.logger.log(`🚀 [JOB START] Audience sync for Account: ${accountId}`);

    try {
      await this.syncService.syncAccountAudienceInsights(accountId);
      const duration = ((Date.now() - start) / 1000).toFixed(2);
      this.logger.log(
        `✨ [JOB FINISHED] Audience sync for Account: ${accountId} | Duration: ${duration}s`,
      );
    } catch (error) {
      this.logger.error(
        `❌ [JOB FAILED] Audience sync for Account: ${accountId} | Error: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error;
    }
  }
}
