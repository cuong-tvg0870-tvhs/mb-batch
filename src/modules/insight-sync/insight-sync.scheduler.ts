import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { Queue } from 'bull';
import { PrismaService } from '../prisma/prisma.service';
import {
  INSIGHT_SYNC_JOBS,
  INSIGHT_SYNC_QUEUE,
  InsightSyncLevel,
  InsightSyncRange,
  SyncAccountJobData,
} from './insight-sync.constants';

@Injectable()
export class InsightSyncScheduler implements OnModuleInit {
  private readonly logger = new Logger(InsightSyncScheduler.name);

  constructor(
    @InjectQueue(INSIGHT_SYNC_QUEUE) private readonly syncQueue: Queue,
    private readonly prisma: PrismaService,
  ) {}

  async onModuleInit() {
    // Trigger immediate sync in development mode for testing
    if (process.env.NODE_ENV !== 'production') {
      this.logger.log('🚀 Triggering immediate sync for development...');
      // Wait a bit for everything to be ready
      setTimeout(() => {
       
        this.scheduleAudienceSync().catch((err) =>
          this.logger.error(`Failed to trigger immediate audience sync: ${err.message}`),
        );
      }, 5000);
    }
  }

  /**
   * 🔵 TODAY SYNC
   * Runs every 1 hour
   */
  @Cron('0 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleTodaySync() {
    this.logger.log('📅 Scheduling Today Insights Sync (1h)...');
    await this.queueSyncForAllAccounts(
      [InsightSyncLevel.CAMPAIGN, InsightSyncLevel.ADSET, InsightSyncLevel.AD],
      [InsightSyncRange.TODAY],
    );
  }

  /**
   * 🟢 LAST 3D SYNC
   * Runs every 6 hours
   */
  @Cron('0 */6 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async schedule3DSync() {
    this.logger.log('📅 Scheduling Last 3D Insights Sync (6h)...');
    await this.queueSyncForAllAccounts(
      [InsightSyncLevel.CAMPAIGN, InsightSyncLevel.ADSET, InsightSyncLevel.AD],
      [InsightSyncRange.LAST_3D],
    );
  }

  /**
   * 🟡 LAST 7D SYNC
   * Runs every 8 hours
   */
  @Cron('0 */8 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async schedule7DSync() {
    this.logger.log('📅 Scheduling Last 7D Insights Sync (8h)...');
    await this.queueSyncForAllAccounts(
      [InsightSyncLevel.CAMPAIGN, InsightSyncLevel.ADSET, InsightSyncLevel.AD],
      [InsightSyncRange.LAST_7D],
    );
  }

  /**
   * 🔴 MAX SYNC
   * Runs every 3 hours
   */
  @Cron('0 */3 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleMaxSync() {
    this.logger.log('📅 Scheduling Max Insights Sync (3h)...');
    await this.queueSyncForAllAccounts(
      [InsightSyncLevel.CAMPAIGN, InsightSyncLevel.ADSET, InsightSyncLevel.AD],
      [InsightSyncRange.MAX],
    );
  }

  /**
   * 👥 AUDIENCE SYNC
   * Runs once a day at 04:35 AM
   */
  @Cron('35 4 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleAudienceSync() {
    this.logger.log('📅 Scheduling Audience Insights Sync (Daily 04:35 AM)...');
    await this.queueAudienceSyncForAllAccounts();
  }

  private async queueSyncForAllAccounts(
    levels: InsightSyncLevel[],
    ranges: InsightSyncRange[],
  ) {
    const accounts = await this.prisma.account.findMany({
      where: { needsReauth: false },
      select: { id: true },
    });

    this.logger.log(`Found ${accounts.length} accounts to sync insights.`);

    for (const account of accounts) {
      const jobData: SyncAccountJobData = {
        accountId: account.id,
        levels,
        ranges,
      };

      await this.syncQueue.add(INSIGHT_SYNC_JOBS.SYNC_ACCOUNT, jobData, {
        attempts: 3, // Retry up to 3 times
        backoff: {
          type: 'exponential',
          delay: 60000, // Wait 1 minute before first retry
        },
        removeOnComplete: true,
        removeOnFail: false, // Keep failed jobs for investigation
      });
    }

    this.logger.log(
      `✅ Successfully queued insight jobs for ${accounts.length} accounts.`,
    );
  }

  private async queueAudienceSyncForAllAccounts() {
    const accounts = await this.prisma.account.findMany({
      where: { needsReauth: false },
      select: { id: true },
    });

    this.logger.log(`Found ${accounts.length} accounts to sync audience.`);

    for (const account of accounts) {
      await this.syncQueue.add(
        INSIGHT_SYNC_JOBS.SYNC_AUDIENCE,
        { accountId: account.id },
        {
          attempts: 3,
          backoff: { type: 'exponential', delay: 60000 },
          removeOnComplete: true,
          removeOnFail: false,
        },
      );
    }

    this.logger.log(
      `✅ Successfully queued audience jobs for ${accounts.length} accounts.`,
    );
  }
}
