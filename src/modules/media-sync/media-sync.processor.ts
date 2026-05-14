import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { Job } from 'bull';
import { MEDIA_SYNC_JOBS, MEDIA_SYNC_QUEUE } from './media-sync.constants';
import { MediaSyncService } from './media-sync.service';

@Processor(MEDIA_SYNC_QUEUE)
export class MediaSyncProcessor {
  private readonly logger = new Logger(MediaSyncProcessor.name);

  constructor(private readonly mediaSyncService: MediaSyncService) {}

  @Process({ name: MEDIA_SYNC_JOBS.SYNC_WORKFLOW, concurrency: 1 })
  async handleMediaSync(job: Job) {
    this.logger.log('🚀 [JOB START] Media Sync Workflow');
    await this.mediaSyncService.handleMediaSync();
    this.logger.log('✨ [JOB FINISHED] Media Sync Workflow');
  }
}
