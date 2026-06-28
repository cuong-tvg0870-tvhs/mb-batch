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
   * 🎥 AD VIDEO DATA — rải đều cả ngày, mỗi 3h tại phút :05
   * (00:05, 03:05, 06:05, 09:05, 12:05, 15:05, 18:05, 21:05)
   */
  @Cron('5 */3 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
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
   * 🎥 AD VIDEO ERROR DATA (đường lấy thẳng /<video_id> bằng token DB) —
   * rải đều cả ngày, mỗi 3h tại phút :35 (chạy ~30' sau AdVideo để xử lý các
   * video vừa bị đánh dấu ERROR, và lệch khỏi các job token-B của media-sync ở :00–:06)
   * (00:35, 03:35, 06:35, 09:35, 12:35, 15:35, 18:35, 21:35)
   */
  @Cron('35 */3 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleAdVideoErrorSync() {
    this.logger.log('📅 Scheduling Ad Video Error Data Sync...');
    const bucket = new Date().toISOString().slice(0, 13);
    await this.mediaSyncQueue.add(
      META_MEDIA_SYNC_JOBS.SYNC_AD_VIDEO_ERROR_DATA,
      {},
      {
        jobId: `${META_MEDIA_SYNC_JOBS.SYNC_AD_VIDEO_ERROR_DATA}:${bucket}`,
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
