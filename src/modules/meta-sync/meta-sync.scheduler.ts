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
    // await this.scheduleCampaignCoreSync();
  }

  /**
   * 🔹 CORE DATA (incremental, hourly)
   */
  @Cron('5 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleCampaignCoreSync() {
    this.logger.log('📅 Scheduling Campaign Core Sync...');
    const bucket = new Date().toISOString().slice(0, 13);
    await this.metaSyncQueue.add(
      META_SYNC_JOBS.SYNC_CAMPAIGN_CORE,
      {},
      {
        jobId: `${META_SYNC_JOBS.SYNC_CAMPAIGN_CORE}:${bucket}`,
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential', delay: 60000 },
      },
    );
  }
}
