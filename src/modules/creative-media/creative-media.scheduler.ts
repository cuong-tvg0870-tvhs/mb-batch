import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { Queue } from 'bull';
import {
  CREATIVE_MEDIA_JOBS,
  CREATIVE_MEDIA_QUEUE,
} from './creative-media.constants';

@Injectable()
export class CreativeMediaScheduler implements OnModuleInit {
  private readonly logger = new Logger(CreativeMediaScheduler.name);

  constructor(
    @InjectQueue(CREATIVE_MEDIA_QUEUE)
    private readonly queue: Queue,
  ) {}

  async onModuleInit() {
    this.logger.log('CreativeMediaScheduler initialized');
    await this.scheduleRehost();
    await this.scheduleOrphanRehost();
  }

  // Re-host thumbnail VIDEO — lệch phút để không đụng tải nặng cùng lúc.
  @Cron('45 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleRehost() {
    this.logger.log('Scheduling Creative media re-host...');
    const bucket = new Date().toISOString().slice(0, 13); // theo giờ

    await this.queue.add(
      CREATIVE_MEDIA_JOBS.REHOST_CREATIVE_IMAGES,
      {},
      {
        jobId: `${CREATIVE_MEDIA_JOBS.REHOST_CREATIVE_IMAGES}:${bucket}`,
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential' },
      },
    );
  }

  // Re-host creative ORPHAN (gọi Meta) — lệch :15 khỏi job video (:45).
  @Cron('15 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleOrphanRehost() {
    this.logger.log('Scheduling Creative orphan re-host...');
    const bucket = new Date().toISOString().slice(0, 13); // theo giờ

    await this.queue.add(
      CREATIVE_MEDIA_JOBS.REHOST_ORPHAN_IMAGES,
      {},
      {
        jobId: `${CREATIVE_MEDIA_JOBS.REHOST_ORPHAN_IMAGES}:${bucket}`,
        removeOnComplete: true,
        attempts: 3,
        backoff: { type: 'exponential' },
      },
    );
  }
}
