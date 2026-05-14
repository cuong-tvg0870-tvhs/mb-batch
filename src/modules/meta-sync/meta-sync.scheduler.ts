import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { Queue } from 'bull';
import { META_SYNC_JOBS, META_SYNC_QUEUE } from './meta-sync.constants';

@Injectable()
export class MetaSyncScheduler implements OnModuleInit {
  private readonly logger = new Logger(MetaSyncScheduler.name);

  constructor(
    @InjectQueue(META_SYNC_QUEUE) private readonly metaSyncQueue: Queue,
  ) {}

  async onModuleInit() {
    this.logger.log('🚀 MetaSyncScheduler Initialized');
    // Initial sync on startup
    await this.scheduleCampaignCoreSync();
  }

  /**
   * 🔹 CORE DATA (1 lần / ngày lúc 00:05)
   */
  @Cron('5 0 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleCampaignCoreSync() {
    this.logger.log('📅 Scheduling Campaign Core Sync...');
    await this.metaSyncQueue.add(
      META_SYNC_JOBS.SYNC_CAMPAIGN_CORE,
      {},
      {
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential', delay: 60000 },
      },
    );
  }

  /**
   * 🖼️ IMAGE DATA (19:05, 20:05, 21:05)
   */
  @Cron('5 19,20,21 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleImageDataSync() {
    this.logger.log('📅 Scheduling Image Data Sync...');
    await this.metaSyncQueue.add(
      META_SYNC_JOBS.SYNC_IMAGE_DATA,
      {},
      {
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential', delay: 60000 },
      },
    );
  }

  /**
   * 🎥 VIDEO DATA (20:05, 21:05, 22:05)
   */
  @Cron('5 20,21,22 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleVideoDataSync() {
    this.logger.log('📅 Scheduling Video Data Sync...');
    await this.metaSyncQueue.add(
      META_SYNC_JOBS.SYNC_VIDEO_DATA,
      {},
      {
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential', delay: 60000 },
      },
    );
  }

  /**
   * 📁 FOLDER VIDEO DATA (Mỗi giờ phút 20)
   */
  @Cron('20 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleFolderVideoSync() {
    this.logger.log('📅 Scheduling Folder Video Sync...');
    await this.metaSyncQueue.add(
      META_SYNC_JOBS.SYNC_FOLDER_VIDEO_DATA,
      {},
      {
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential', delay: 60000 },
      },
    );
  }

  /**
   * 📁 FOLDER IMAGE DATA (Mỗi giờ phút 40)
   */
  @Cron('40 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleFolderImageSync() {
    this.logger.log('📅 Scheduling Folder Image Sync...');
    await this.metaSyncQueue.add(
      META_SYNC_JOBS.SYNC_FOLDER_IMAGE_DATA,
      {},
      {
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential', delay: 60000 },
      },
    );
  }
}
