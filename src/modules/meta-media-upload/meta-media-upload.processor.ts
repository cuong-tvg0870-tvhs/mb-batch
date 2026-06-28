import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { Job } from 'bull';
import { BatchRunLoggerService } from '../batch-run-log/batch-run-logger.service';
import {
  META_MEDIA_UPLOAD_JOBS,
  META_MEDIA_UPLOAD_QUEUE,
} from './meta-media-upload.constants';
import { MetaMediaUploadService } from './meta-media-upload.service';

@Processor(META_MEDIA_UPLOAD_QUEUE)
export class MetaMediaUploadProcessor {
  private readonly logger = new Logger(MetaMediaUploadProcessor.name);

  constructor(
    private readonly metaMediaUploadService: MetaMediaUploadService,
    private readonly batchRunLogger: BatchRunLoggerService,
  ) {}

  @Process({ name: META_MEDIA_UPLOAD_JOBS.AUTO_UPLOAD, concurrency: 1 })
  async handleAutoUpload(job: Job) {
    return this.batchRunLogger.track(
      META_MEDIA_UPLOAD_JOBS.AUTO_UPLOAD,
      META_MEDIA_UPLOAD_QUEUE,
      async (ctx) => {
        this.logger.log(`🚀 [JOB START] Meta media auto-upload ${job.id}`);
        const result = await this.metaMediaUploadService.autoUpload();
        if (result?.skipped) {
          ctx.skip(result.reason);
          this.logger.warn(
            `⏭️ [JOB SKIPPED] Meta media auto-upload ${job.id}: ${result.reason}`,
          );
        }
        this.logger.log(`✨ [JOB FINISHED] Meta media auto-upload ${job.id}`);
      },
    );
  }
}
