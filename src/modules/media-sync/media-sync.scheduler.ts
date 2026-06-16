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

  private async enqueueSingletonJob(jobName: string) {
    const jobId = `media-sync:${jobName}:singleton`;
    const existingJob = await this.mediaSyncQueue.getJob(jobId);
    if (existingJob) {
      const state = await existingJob.getState();
      if (['active', 'waiting', 'delayed', 'paused'].includes(state)) {
        this.logger.warn(
          `⏳ Skip ${jobName}; existing singleton job is ${state}.`,
        );
        return false;
      }
      await existingJob.remove().catch(() => undefined);
    }

    await this.mediaSyncQueue.add(
      jobName,
      {},
      {
        jobId,
        removeOnComplete: true,
        removeOnFail: true,
        attempts: 3,
        backoff: { type: 'exponential', delay: 60000 },
      },
    );
    return true;
  }

  async onModuleInit() {
    this.logger.log('🚀 MediaSyncScheduler Initialized');

    if (
      process.env.NODE_ENV === 'production' &&
      process.env.DISABLE_STARTUP_SYNC !== 'true'
    ) {
      this.logger.log(
        '🚀 [Deploy Startup] Production environment detected. Triggering Expired URLs Sync...',
      );
      try {
        const queued = await this.enqueueSingletonJob(
          MEDIA_SYNC_JOBS.SYNC_EXPIRED_URLS,
        );
        this.logger.log(
          queued
            ? '✅ Startup Expired URLs Sync successfully queued.'
            : '⏳ Startup Expired URLs Sync skipped because a job is already pending.',
        );
      } catch (error: any) {
        this.logger.error(
          `❌ Failed to queue startup Expired URLs Sync: ${error.message}`,
        );
      }
    }
  }

  /**
   * ⏰ SYNC FOLDERS (Every hour at minute 0)
   */
  @Cron('0 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleSyncFolders() {
    this.logger.log('📅 Scheduling Folders Sync...');
    await this.enqueueSingletonJob(MEDIA_SYNC_JOBS.SYNC_FOLDERS);
  }

  /**
   * ⏰ SYNC CREATIVES (Every hour at minute 3)
   */
  @Cron('3 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleSyncCreatives() {
    this.logger.log('📅 Scheduling Creatives Sync...');
    await this.enqueueSingletonJob(MEDIA_SYNC_JOBS.SYNC_CREATIVES);
  }

  /**
   * ⏰ SYNC VIDEO SOURCES (Every 3 hours at minute 6)
   */
  @Cron('6 */3 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleSyncVideoSources() {
    this.logger.log('📅 Scheduling Video Sources Sync...');
    await this.enqueueSingletonJob(MEDIA_SYNC_JOBS.SYNC_VIDEO_SOURCES);
  }

  /**
   * ⏰ SYNC EXPIRED URLS (Every 2 hours at minute 0)
   */
  @Cron('0 */2 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleSyncExpiredUrls() {
    this.logger.log('📅 Scheduling Expired URLs Sync...');
    await this.enqueueSingletonJob(MEDIA_SYNC_JOBS.SYNC_EXPIRED_URLS);
  }
}
