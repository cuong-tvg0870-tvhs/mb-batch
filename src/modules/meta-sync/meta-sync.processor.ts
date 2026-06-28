import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { Job } from 'bull';
import { BatchRunLoggerService } from '../batch-run-log/batch-run-logger.service';
import { META_SYNC_JOBS, META_SYNC_QUEUE } from './meta-sync.constants';
import { MetaSyncService } from './meta-sync.service';

@Processor(META_SYNC_QUEUE)
export class MetaSyncProcessor {
  private readonly logger = new Logger(MetaSyncProcessor.name);

  constructor(
    private readonly metaSyncService: MetaSyncService,
    private readonly batchRunLogger: BatchRunLoggerService,
  ) {}

  @Process({ name: META_SYNC_JOBS.SYNC_CAMPAIGN_CORE, concurrency: 1 })
  async handleSyncCampaignCore(job: Job) {
    return this.batchRunLogger.track(
      META_SYNC_JOBS.SYNC_CAMPAIGN_CORE,
      META_SYNC_QUEUE,
      async (ctx) => {
        this.logger.log('🚀 [JOB START] Sync Campaign Core');
        const stats = await this.metaSyncService.syncCampaignData();
        ctx.setTotal(stats.accountsTotal);
        ctx.addSuccess(stats.accountsOk);
        ctx.addFailure(stats.accountsFailed);
        for (const e of stats.errors) ctx.error(e);
        ctx.setMeta({
          campaignsCreated: stats.campaignsCreated,
          campaignsUpdated: stats.campaignsUpdated,
          adsetsCreated: stats.adsetsCreated,
          adsetsUpdated: stats.adsetsUpdated,
          adsCreated: stats.adsCreated,
          adsUpdated: stats.adsUpdated,
          creativesCreated: stats.creativesCreated,
          creativesUpdated: stats.creativesUpdated,
          images: stats.images,
          videos: stats.videos,
        });
        this.logger.log('✨ [JOB FINISHED] Sync Campaign Core');
      },
    );
  }
}
