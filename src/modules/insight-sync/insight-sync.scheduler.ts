import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { Queue } from 'bull';
import { PrismaService } from '../prisma/prisma.service';
import { InsightSyncService } from './insight-sync.service';
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
    private readonly syncService: InsightSyncService,
  ) {}

  async onModuleInit() {
    if (process.env.NODE_ENV === 'production' && process.env.DISABLE_STARTUP_SYNC !== 'true') {
      this.logger.log('🚀 [Deploy Startup] Production environment detected. Preparing to trigger all Insight Syncs...');

      const client = (this.syncQueue as any).client;
      if (client) {
        try {
          const cooldownKey = 'lock:insight-sync:startup-cooldown';
          const isCooldownActive = await client.get(cooldownKey);

          if (isCooldownActive) {
            this.logger.warn(
              '⚠️ Startup Insight Sync skipped due to 5-minute cooldown lock. ' +
              'This prevents spamming the Meta API if the container restarts frequently.'
            );
            return;
          }

          // Set cooldown lock for 5 minutes (300 seconds)
          await client.set(cooldownKey, 'true', 'EX', 300);
          this.logger.log('🔒 Redis cooldown lock set for 5 minutes.');
        } catch (redisError: any) {
          this.logger.warn(`Failed to access/set Redis lock: ${redisError.message}. Proceeding without lock.`);
        }
      }

      await this.triggerAllSyncsSequentially();
    }
  }

  private async triggerAllSyncsSequentially() {
    this.logger.log('📢 Triggering all insight syncs sequentially (Today/3D/7D, Max, Missing Daily, Audience, Inactive Sliding)...');
    try {
      await this.scheduleTodaySync();
      await this.scheduleMaxSync();
      await this.scheduleMissingDailySync();
      await this.scheduleAudienceSync();
      await this.scheduleInactiveSlidingWindow();
      this.logger.log('✅ All insight syncs successfully queued on startup.');
    } catch (error: any) {
      this.logger.error(`❌ Failed to queue startup syncs: ${error.message}`);
    }
  }

  /**
   * 🔵 NEAR-REAL-TIME SYNC (Today + 3D + 7D)
   * Runs every 1 hour. Only TODAY triggers a Meta fetch (last-7-day DAILY); the
   * 3D/7D rollups are rebuilt locally from that same DAILY in the same pass — no
   * extra Meta calls — so every short range stays fresh hourly and they never
   * drift apart. (Replaces the former separate 6h/8h crons.)
   */
  @Cron('0 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleTodaySync() {
    this.logger.log('📅 Scheduling Near-Real-Time Insights Sync (Today/3D/7D, 1h)...');
    await this.queueSyncForAllAccounts(
      [InsightSyncLevel.CAMPAIGN, InsightSyncLevel.ADSET, InsightSyncLevel.AD],
      [
        InsightSyncRange.TODAY,
        InsightSyncRange.LAST_3D,
        InsightSyncRange.LAST_7D,
      ],
    );
  }

  /**
   * 🔴 MAX SYNC
   * Runs once a day. MAX is expensive and should not compete with near-real-time
   * ranges. 3D/7D are bundled in (local rollup only, no extra Meta call) so the
   * daily creative performanceStatus is computed with real roas7d/roas3d buckets
   * — otherwise a pure-MAX run leaves them empty and SCALE_P1/P2 is unreachable.
   */
  @Cron('15 2 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleMaxSync() {
    this.logger.log('📅 Scheduling Max Insights Sync (daily, +3D/7D for status)...');
    await this.queueSyncForAllAccounts(
      [InsightSyncLevel.CAMPAIGN, InsightSyncLevel.ADSET, InsightSyncLevel.AD],
      [
        InsightSyncRange.MAX,
        InsightSyncRange.LAST_3D,
        InsightSyncRange.LAST_7D,
      ],
    );
  }

  /**
   * 🟠 MISSING DAILY SYNC
   * Runs once a day at 03:00 AM
   */
  @Cron('0 3 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleMissingDailySync() {
    this.logger.log(
      '📅 Scheduling Missing Daily Insights Sync (Daily 03:00 AM)...',
    );
    await this.queueMissingDailySyncForAllAccounts();
  }

  /**
   * 🟣 LIFETIME DAILY BACKFILL (gradual)
   * Runs every 4 hours and backfills a BOUNDED slice of entities per account per
   * run (INSIGHT_LIFETIME_BACKFILL_ENTITIES_PER_RUN) so historical MAX fills in
   * over several runs without spiking the Meta API quota. Idempotent: once an
   * entity's lifetime is covered it is skipped on subsequent runs.
   */
  @Cron('45 */4 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleLifetimeBackfill() {
    this.logger.log(
      '📅 Scheduling Lifetime DAILY Backfill (every 4h, bounded slice)...',
    );
    await this.queueLifetimeBackfillForAllAccounts();
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
      where: { needsReauth: false, accountType: 'AD_ACCOUNT' as any },
      select: { id: true },
    });

    this.logger.log(`Found ${accounts.length} accounts to sync insights.`);
    const bucket = new Date().toISOString().slice(0, 13);

    for (const account of accounts) {
      const jobData: SyncAccountJobData = {
        accountId: account.id,
        levels,
        ranges,
      };

      await this.syncQueue.add(INSIGHT_SYNC_JOBS.SYNC_ACCOUNT, jobData, {
        jobId: [
          INSIGHT_SYNC_JOBS.SYNC_ACCOUNT,
          account.id,
          levels.join('-'),
          ranges.join('-'),
          bucket,
        ].join(':'),
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

  private async queueMissingDailySyncForAllAccounts() {
    const accounts = await this.prisma.account.findMany({
      where: { needsReauth: false, accountType: 'AD_ACCOUNT' as any },
      select: { id: true },
    });

    this.logger.log(`Found ${accounts.length} accounts to sync missing daily.`);
    const bucket = new Date().toISOString().slice(0, 10);

    for (const account of accounts) {
      await this.syncQueue.add(
        INSIGHT_SYNC_JOBS.SYNC_MISSING_DAILY,
        { accountId: account.id },
        {
          jobId: `${INSIGHT_SYNC_JOBS.SYNC_MISSING_DAILY}:${account.id}:${bucket}`,
          attempts: 3,
          // Real backoff (was delay:0, which retried instantly and usually hit
          // the same transient Meta error). 5 min, doubling.
          backoff: { type: 'exponential', delay: 300000 },
          removeOnComplete: true,
          removeOnFail: false,
        },
      );
    }

    this.logger.log(
      `✅ Successfully queued missing daily jobs for ${accounts.length} accounts.`,
    );
  }

  private async queueLifetimeBackfillForAllAccounts() {
    const accounts = await this.prisma.account.findMany({
      where: { needsReauth: false, accountType: 'AD_ACCOUNT' as any },
      select: { id: true },
    });

    this.logger.log(
      `Found ${accounts.length} accounts for lifetime backfill.`,
    );
    // Hour bucket so each 4-hourly run enqueues a fresh bounded slice instead of
    // being de-duplicated away for the rest of the day.
    const bucket = new Date().toISOString().slice(0, 13);

    for (const account of accounts) {
      await this.syncQueue.add(
        INSIGHT_SYNC_JOBS.SYNC_LIFETIME_BACKFILL,
        { accountId: account.id },
        {
          jobId: `${INSIGHT_SYNC_JOBS.SYNC_LIFETIME_BACKFILL}:${account.id}:${bucket}`,
          attempts: 2,
          backoff: { type: 'exponential', delay: 60000 },
          removeOnComplete: true,
          removeOnFail: false,
        },
      );
    }

    this.logger.log(
      `✅ Successfully queued lifetime backfill jobs for ${accounts.length} accounts.`,
    );
  }

  private async queueAudienceSyncForAllAccounts() {
    const accounts = await this.prisma.account.findMany({
      where: { needsReauth: false, accountType: 'AD_ACCOUNT' as any },
      select: { id: true },
    });

    this.logger.log(`Found ${accounts.length} accounts to sync audience.`);
    const bucket = new Date().toISOString().slice(0, 10);

    for (const account of accounts) {
      await this.syncQueue.add(
        INSIGHT_SYNC_JOBS.SYNC_AUDIENCE,
        { accountId: account.id },
        {
          jobId: `${INSIGHT_SYNC_JOBS.SYNC_AUDIENCE}:${account.id}:${bucket}`,
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

  /**
   * 🟣 INACTIVE SLIDING WINDOW JOB
   * Runs once a day at 12:10 AM
   */
  @Cron('10 0 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleInactiveSlidingWindow() {
    this.logger.log('📅 Scheduling Inactive Sliding Window Job (12:10 AM)...');
    try {
      await this.syncService.slideInactiveInsights();
    } catch (err: any) {
      this.logger.error(`❌ Inactive Sliding Window Job failed: ${err.message}`);
    }
  }
}
