import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { Queue } from 'bull';
import { LARK_SYNC_JOBS, LARK_SYNC_QUEUE } from './lark-sync.constants';

@Injectable()
export class LarkSyncScheduler implements OnModuleInit {
  private readonly logger = new Logger(LarkSyncScheduler.name);

  constructor(
    @InjectQueue(LARK_SYNC_QUEUE) private readonly larkSyncQueue: Queue,
  ) {}

  async onModuleInit() {
    this.logger.log('🚀 LarkSyncScheduler Initialized');
  }

  /**
   * 🔄 SYNC WORKFLOW (Lark <-> Drive)
   * Runs every 5 minutes
   */
  @Cron(CronExpression.EVERY_30_MINUTES, { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleSyncWorkflow() {
    this.logger.log('📅 Scheduling Lark <-> Drive Sync Workflow...');
    await this.larkSyncQueue.add(
      LARK_SYNC_JOBS.SYNC_WORKFLOW,
      {},
      {
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential' },
      },
    );
  }
}
