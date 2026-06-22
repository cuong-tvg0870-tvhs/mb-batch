import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { Job } from 'bull';
import {
  META_MEDIA_SYNC_JOBS,
  META_MEDIA_SYNC_QUEUE,
} from './meta-media-sync.constants';
import { MetaMediaSyncService } from './meta-media-sync.service';

@Processor(META_MEDIA_SYNC_QUEUE)
export class MetaMediaSyncProcessor {
  private readonly logger = new Logger(MetaMediaSyncProcessor.name);

  constructor(private readonly service: MetaMediaSyncService) {}

  @Process({ name: META_MEDIA_SYNC_JOBS.SYNC_AD_IMAGE_DATA, concurrency: 1 })
  async handleSyncAdImageData(job: Job) {
    this.logger.log('🚀 [JOB START] Sync Ad Image Data');
    await this.service.syncAdImage();
    this.logger.log('✨ [JOB FINISHED] Sync Ad Image Data');
  }

  @Process({ name: META_MEDIA_SYNC_JOBS.SYNC_AD_VIDEO_DATA, concurrency: 1 })
  async handleSyncAdVideoData(job: Job) {
    this.logger.log('🚀 [JOB START] Sync Ad Video Data');
    await this.service.syncAdVideo();
    this.logger.log('✨ [JOB FINISHED] Sync Ad Video Data');
  }

  @Process({ name: META_MEDIA_SYNC_JOBS.SYNC_AD_VIDEO_ERROR_DATA, concurrency: 1 })
  async handleSyncAdVideoErrorData(job: Job) {
    this.logger.log('🚀 [JOB START] Sync Ad Video Error Data');
    await this.service.syncAdVideoError();
    this.logger.log('✨ [JOB FINISHED] Sync Ad Video Error Data');
  }

  @Process({
    name: META_MEDIA_SYNC_JOBS.RECALCULATE_LOCAL_URL_EXPIRED,
    concurrency: 1,
  })
  async handleRecalculateLocalUrlExpired(job: Job) {
    this.logger.log('🚀 [JOB START] Recalculate Local URL Expired');
    await this.service.recalculateLocalUrlExpired();
    this.logger.log('✨ [JOB FINISHED] Recalculate Local URL Expired');
  }
}
