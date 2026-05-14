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

  @Process({ name: META_SYNC_JOBS.SYNC_IMAGE_DATA, concurrency: 1 })
  async handleSyncImageData(job: Job) {
    this.logger.log('🚀 [JOB START] Sync Image Data');
    await this.metaSyncService.syncImage();
    this.logger.log('✨ [JOB FINISHED] Sync Image Data');
  }

  @Process({ name: META_SYNC_JOBS.SYNC_VIDEO_DATA, concurrency: 1 })
  async handleSyncVideoData(job: Job) {
    this.logger.log('🚀 [JOB START] Sync Video Data');
    await this.metaSyncService.syncVideo();
    this.logger.log('✨ [JOB FINISHED] Sync Video Data');
  }

  @Process({ name: META_SYNC_JOBS.SYNC_FOLDER_VIDEO_DATA, concurrency: 1 })
  async handleSyncFolderVideoData(job: Job) {
    this.logger.log('🚀 [JOB START] Sync Folder Video Data');
    await this.metaSyncService.syncFolderVideo();
    this.logger.log('✨ [JOB FINISHED] Sync Folder Video Data');
  }

  @Process({ name: META_SYNC_JOBS.SYNC_FOLDER_IMAGE_DATA, concurrency: 1 })
  async handleSyncFolderImageData(job: Job) {
    this.logger.log('🚀 [JOB START] Sync Folder Image Data');
    await this.metaSyncService.syncFolderImage();
    this.logger.log('✨ [JOB FINISHED] Sync Folder Image Data');
  }
}
