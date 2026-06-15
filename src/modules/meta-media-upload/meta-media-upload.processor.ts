import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { Job } from 'bull';
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
  ) {}

  @Process({ name: META_MEDIA_UPLOAD_JOBS.AUTO_UPLOAD, concurrency: 1 })
  async handleAutoUpload(job: Job) {
    this.logger.log(`🚀 [JOB START] Meta media auto-upload ${job.id}`);
    await this.metaMediaUploadService.autoUpload();
    this.logger.log(`✨ [JOB FINISHED] Meta media auto-upload ${job.id}`);
  }
}
