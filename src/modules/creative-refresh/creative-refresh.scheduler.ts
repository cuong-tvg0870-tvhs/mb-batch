import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { Queue } from 'bull';
import {
  CREATIVE_REFRESH_JOBS,
  CREATIVE_REFRESH_QUEUE,
} from './creative-refresh.constants';

@Injectable()
export class CreativeRefreshScheduler implements OnModuleInit {
  private readonly logger = new Logger(CreativeRefreshScheduler.name);

  constructor(
    @InjectQueue(CREATIVE_REFRESH_QUEUE)
    private readonly creativeRefreshQueue: Queue,
  ) {}

  async onModuleInit() {
    this.logger.log('CreativeRefreshScheduler initialized');
    await this.scheduleRefreshExpiringCreatives();
  }

  @Cron('20 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleRefreshExpiringCreatives() {
    this.logger.log('Scheduling Creative refresh...');
    const bucket = new Date().toISOString().slice(0, 13);

    await this.creativeRefreshQueue.add(
      CREATIVE_REFRESH_JOBS.REFRESH_EXPIRING_CREATIVES,
      {},
      {
        jobId: `${CREATIVE_REFRESH_JOBS.REFRESH_EXPIRING_CREATIVES}:${bucket}`,
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential' },
      },
    );
  }

  @Cron('30 0 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleRecalculateCreativeUrlExpired() {
    this.logger.log('Scheduling Creative URL expiry recalculation...');
    const bucket = new Date().toISOString().slice(0, 10);

    await this.creativeRefreshQueue.add(
      CREATIVE_REFRESH_JOBS.RECALCULATE_CREATIVE_URL_EXPIRED,
      {},
      {
        jobId: `${CREATIVE_REFRESH_JOBS.RECALCULATE_CREATIVE_URL_EXPIRED}:${bucket}`,
        removeOnComplete: true,
        attempts: 2,
        backoff: { type: 'exponential' },
      },
    );
  }
}
