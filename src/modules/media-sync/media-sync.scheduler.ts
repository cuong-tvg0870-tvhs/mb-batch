import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { Queue } from 'bull';
import { MEDIA_SYNC_JOBS, MEDIA_SYNC_QUEUE } from './media-sync.constants';

@Injectable()
export class MediaSyncScheduler implements OnModuleInit {
  private readonly logger = new Logger(MediaSyncScheduler.name);

  constructor(
    @InjectQueue(MEDIA_SYNC_QUEUE) private readonly mediaSyncQueue: Queue,
  ) {}

  async onModuleInit() {
    this.logger.log('🚀 MediaSyncScheduler Initialized');
    // Initial sync on startup
  }

  /**
   * ⏰ SYNC FOLDERS (Every hour at minute 0)
   */
  @Cron('0 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleSyncFolders() {
    this.logger.log('📅 Scheduling Folders Sync...');
    await this.mediaSyncQueue.add(
      MEDIA_SYNC_JOBS.SYNC_FOLDERS,
      {},
      {
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential', delay: 60000 },
      },
    );
  }

  /**
   * ⏰ SYNC CREATIVES (Every hour at minute 3)
   */
  @Cron('3 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleSyncCreatives() {
    this.logger.log('📅 Scheduling Creatives Sync...');
    await this.mediaSyncQueue.add(
      MEDIA_SYNC_JOBS.SYNC_CREATIVES,
      {},
      {
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential', delay: 60000 },
      },
    );
  }

  /**
   * ⏰ SYNC VIDEO SOURCES (Every hour at minute 6)
   */
  @Cron('6 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleSyncVideoSources() {
    this.logger.log('📅 Scheduling Video Sources Sync...');
    await this.mediaSyncQueue.add(
      MEDIA_SYNC_JOBS.SYNC_VIDEO_SOURCES,
      {},
      {
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential', delay: 60000 },
      },
    );
  }

  /**
   * ⏰ SYNC EXPIRED URLS (Every 2 hours at minute 0)
   */
  @Cron('0 */1 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleSyncExpiredUrls() {
    this.logger.log('📅 Scheduling Expired URLs Sync...');
    await this.mediaSyncQueue.add(
      MEDIA_SYNC_JOBS.SYNC_EXPIRED_URLS,
      {},
      {
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential', delay: 60000 },
      },
    );
  }
}
