import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { Queue } from 'bull';
import {
  META_MEDIA_UPLOAD_JOBS,
  META_MEDIA_UPLOAD_QUEUE,
} from './meta-media-upload.constants';

@Injectable()
export class MetaMediaUploadScheduler implements OnModuleInit {
  private readonly logger = new Logger(MetaMediaUploadScheduler.name);

  constructor(
    @InjectQueue(META_MEDIA_UPLOAD_QUEUE)
    private readonly metaMediaUploadQueue: Queue,
  ) {}

  async onModuleInit() {
    this.logger.log('🚀 MetaMediaUploadScheduler initialized');
    this.logger.log('📅 Scheduling startup Meta media auto-upload...');
    await this.enqueueAutoUpload();
  }

  @Cron('*/20 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async scheduleAutoUpload() {
    this.logger.log('📅 Scheduling Meta media auto-upload...');
    await this.enqueueAutoUpload();
  }

  private async enqueueAutoUpload() {
    const counts = await this.metaMediaUploadQueue.getJobCounts();
    const pendingJobCount =
      (counts.active || 0) + (counts.waiting || 0) + (counts.delayed || 0);

    if (pendingJobCount > 0) {
      this.logger.warn(
        `Skipping Meta media auto-upload enqueue because ${pendingJobCount} job(s) are already active/waiting/delayed`,
      );
      return;
    }

    await this.metaMediaUploadQueue.add(
      META_MEDIA_UPLOAD_JOBS.AUTO_UPLOAD,
      {},
      {
        jobId: `${META_MEDIA_UPLOAD_JOBS.AUTO_UPLOAD}:singleton`,
        removeOnComplete: true,
        removeOnFail: true,
        attempts: 3,
        backoff: { type: 'exponential', delay: 60000 },
      },
    );
  }
}
