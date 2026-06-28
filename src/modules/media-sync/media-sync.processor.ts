import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { Job } from 'bull';
import { BatchRunLoggerService } from '../batch-run-log/batch-run-logger.service';
import { MEDIA_SYNC_JOBS, MEDIA_SYNC_QUEUE } from './media-sync.constants';
import { MediaSyncService } from './media-sync.service';

@Processor(MEDIA_SYNC_QUEUE)
export class MediaSyncProcessor {
  private readonly logger = new Logger(MediaSyncProcessor.name);

  constructor(
    private readonly mediaSyncService: MediaSyncService,
    private readonly batchRunLogger: BatchRunLoggerService,
  ) {}

  @Process({ name: MEDIA_SYNC_JOBS.SYNC_FOLDERS, concurrency: 1 })
  async handleSyncFolders(job: Job) {
    return this.batchRunLogger.track(
      MEDIA_SYNC_JOBS.SYNC_FOLDERS,
      MEDIA_SYNC_QUEUE,
      async () => {
        this.logger.log('🚀 [JOB START] Sync Folders');
        await this.mediaSyncService.syncMetaFolders();
        this.logger.log('✨ [JOB FINISHED] Sync Folders');
      },
    );
  }

  @Process({ name: MEDIA_SYNC_JOBS.SYNC_CREATIVES, concurrency: 1 })
  async handleSyncCreatives(job: Job) {
    return this.batchRunLogger.track(
      MEDIA_SYNC_JOBS.SYNC_CREATIVES,
      MEDIA_SYNC_QUEUE,
      async () => {
        this.logger.log('🚀 [JOB START] Sync Creatives');
        await this.mediaSyncService.syncMetaAssets();
        this.logger.log('✨ [JOB FINISHED] Sync Creatives');
      },
    );
  }

  @Process({ name: MEDIA_SYNC_JOBS.SYNC_VIDEO_SOURCES, concurrency: 1 })
  async handleSyncVideoSources(job: Job) {
    return this.batchRunLogger.track(
      MEDIA_SYNC_JOBS.SYNC_VIDEO_SOURCES,
      MEDIA_SYNC_QUEUE,
      async () => {
        this.logger.log('🚀 [JOB START] Sync Video Sources');
        await this.mediaSyncService.syncVideoSources();
        this.logger.log('✨ [JOB FINISHED] Sync Video Sources');
      },
    );
  }

  @Process({ name: MEDIA_SYNC_JOBS.SYNC_EXPIRED_URLS, concurrency: 1 })
  async handleSyncExpiredUrls(job: Job) {
    return this.batchRunLogger.track(
      MEDIA_SYNC_JOBS.SYNC_EXPIRED_URLS,
      MEDIA_SYNC_QUEUE,
      async () => {
        this.logger.log('🚀 [JOB START] Sync Expired URLs');
        await this.mediaSyncService.syncExpiredUrls();
        this.logger.log('✨ [JOB FINISHED] Sync Expired URLs');
      },
    );
  }
}
