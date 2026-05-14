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
   * ⏰ MEDIA SYNC (Every 30 minutes)
   */
  @Cron('*/30 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleMediaSync() {
    this.logger.log('📅 Scheduling Media Sync Workflow...');
    await this.mediaSyncQueue.add(
      MEDIA_SYNC_JOBS.SYNC_WORKFLOW,
      {},
      {
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential', delay: 60000 },
      },
    );
  }
}
