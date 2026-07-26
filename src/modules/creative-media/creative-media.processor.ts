import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { Job } from 'bull';
import { BatchRunLoggerService } from '../batch-run-log/batch-run-logger.service';
import {
  CREATIVE_MEDIA_JOBS,
  CREATIVE_MEDIA_QUEUE,
} from './creative-media.constants';
import { CreativeMediaService } from './creative-media.service';

@Processor(CREATIVE_MEDIA_QUEUE)
export class CreativeMediaProcessor {
  private readonly logger = new Logger(CreativeMediaProcessor.name);

  constructor(
    private readonly service: CreativeMediaService,
    private readonly batchRunLogger: BatchRunLoggerService,
  ) {}

  @Process({
    name: CREATIVE_MEDIA_JOBS.REHOST_CREATIVE_IMAGES,
    concurrency: 1,
  })
  async handleRehostCreativeImages(_job: Job) {
    return this.batchRunLogger.track(
      CREATIVE_MEDIA_JOBS.REHOST_CREATIVE_IMAGES,
      CREATIVE_MEDIA_QUEUE,
      async () => {
        this.logger.log('[JOB START] Re-host Creative Images');
        await this.service.rehostPendingCreatives();
        this.logger.log('[JOB FINISHED] Re-host Creative Images');
      },
    );
  }
}
