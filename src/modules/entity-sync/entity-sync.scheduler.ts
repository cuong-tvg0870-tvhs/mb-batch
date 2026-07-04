import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { Queue } from 'bull';
import { ENTITY_SYNC_JOBS, ENTITY_SYNC_QUEUE } from './entity-sync.constants';

@Injectable()
export class EntitySyncScheduler implements OnModuleInit {
  private readonly logger = new Logger(EntitySyncScheduler.name);

  constructor(
    @InjectQueue(ENTITY_SYNC_QUEUE) private readonly entitySyncQueue: Queue,
  ) {}

  async onModuleInit() {
    this.logger.log('🚀 EntitySyncScheduler Initialized');
  }

  /**
   * 🔹 Đồng bộ metadata thực thể Meta (TKQC/Fanpage/Pixel/Audience/Catalog) —
   * 1 ngày/lần lúc 01:00 (giờ VN), trước các job insight (02:15+) để tránh dồn tải.
   * jobId theo ngày để idempotent: nhiều lần enqueue trong cùng ngày chỉ chạy 1.
   */
  @Cron('0 1 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleEntitySync() {
    this.logger.log('📅 Scheduling Meta Entity Sync...');
    const bucket = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
    await this.entitySyncQueue.add(
      ENTITY_SYNC_JOBS.SYNC_META_ENTITIES,
      {},
      {
        jobId: `${ENTITY_SYNC_JOBS.SYNC_META_ENTITIES}:${bucket}`,
        removeOnComplete: true,
        attempts: 2,
        backoff: { type: 'exponential', delay: 120000 },
      },
    );
  }
}
