import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { Queue } from 'bull';
import {
  META_MEDIA_SYNC_JOBS,
  META_MEDIA_SYNC_QUEUE,
} from './meta-media-sync.constants';

@Injectable()
export class MetaMediaSyncScheduler implements OnModuleInit {
  private readonly logger = new Logger(MetaMediaSyncScheduler.name);

  constructor(
    @InjectQueue(META_MEDIA_SYNC_QUEUE)
    private readonly mediaSyncQueue: Queue,
  ) {}

  async onModuleInit() {
    this.logger.log('🚀 MetaMediaSyncScheduler Initialized');
    await this.scheduleAdVideoSync();
  }

  /**
   * 🖼️ AD IMAGE DATA (19:05, 20:05, 21:05)
   */
  @Cron('5 19,20,21 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleAdImageSync() {
    this.logger.log('📅 Scheduling Ad Image Data Sync...');
    const bucket = new Date().toISOString().slice(0, 13);
    await this.mediaSyncQueue.add(
      META_MEDIA_SYNC_JOBS.SYNC_AD_IMAGE_DATA,
      {},
      {
        jobId: `${META_MEDIA_SYNC_JOBS.SYNC_AD_IMAGE_DATA}:${bucket}`,
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential' },
      },
    );
  }

  /**
   * 🎥 AD VIDEO DATA (20:05, 21:05, 22:05)
   */
  @Cron('5 20,21,22 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleAdVideoSync() {
    this.logger.log('📅 Scheduling Ad Video Data Sync...');
    const bucket = new Date().toISOString().slice(0, 13);
    await this.mediaSyncQueue.add(
      META_MEDIA_SYNC_JOBS.SYNC_AD_VIDEO_DATA,
      {},
      {
        jobId: `${META_MEDIA_SYNC_JOBS.SYNC_AD_VIDEO_DATA}:${bucket}`,
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential' },
      },
    );
  }

  /**
   * ⏰ RECALCULATE LOCAL URL EXPIRED (Daily at 00:00 AM)
   */
  @Cron('0 0 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleRecalculateLocalUrlExpired() {
    this.logger.log('📅 Scheduling Recalculate Local URL Expired...');
    const bucket = new Date().toISOString().slice(0, 10);
    await this.mediaSyncQueue.add(
      META_MEDIA_SYNC_JOBS.RECALCULATE_LOCAL_URL_EXPIRED,
      {},
      {
        jobId: `${META_MEDIA_SYNC_JOBS.RECALCULATE_LOCAL_URL_EXPIRED}:${bucket}`,
        removeOnComplete: true,
        attempts: 2,
        backoff: { type: 'exponential' },
      },
    );
  }
}
