import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { Job } from 'bull';
import { META_SYNC_JOBS, META_SYNC_QUEUE } from './meta-sync.constants';
import { MetaSyncService } from './meta-sync.service';

@Processor(META_SYNC_QUEUE)
export class MetaSyncProcessor {
  private readonly logger = new Logger(MetaSyncProcessor.name);

  constructor(private readonly metaSyncService: MetaSyncService) {}

  @Process({ name: META_SYNC_JOBS.SYNC_CAMPAIGN_CORE, concurrency: 1 })
  async handleSyncCampaignCore(job: Job) {
    this.logger.log('🚀 [JOB START] Sync Campaign Core');
    await this.metaSyncService.syncCampaignData();
    this.logger.log('✨ [JOB FINISHED] Sync Campaign Core');
  }
}
