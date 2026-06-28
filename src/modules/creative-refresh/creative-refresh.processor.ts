import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { Job } from 'bull';
import { BatchRunLoggerService } from '../batch-run-log/batch-run-logger.service';
import {
  CREATIVE_REFRESH_JOBS,
  CREATIVE_REFRESH_QUEUE,
} from './creative-refresh.constants';
import { CreativeRefreshService } from './creative-refresh.service';

@Processor(CREATIVE_REFRESH_QUEUE)
export class CreativeRefreshProcessor {
  private readonly logger = new Logger(CreativeRefreshProcessor.name);

  constructor(
    private readonly service: CreativeRefreshService,
    private readonly batchRunLogger: BatchRunLoggerService,
  ) {}

  @Process({
    name: CREATIVE_REFRESH_JOBS.RECALCULATE_CREATIVE_URL_EXPIRED,
    concurrency: 1,
  })
  async handleRecalculateCreativeUrlExpired(job: Job) {
    return this.batchRunLogger.track(
      CREATIVE_REFRESH_JOBS.RECALCULATE_CREATIVE_URL_EXPIRED,
      CREATIVE_REFRESH_QUEUE,
      async () => {
        this.logger.log('[JOB START] Recalculate Creative URL Expired');
        await this.service.recalculateCreativeUrlExpired();
        this.logger.log('[JOB FINISHED] Recalculate Creative URL Expired');
      },
    );
  }

  @Process({
    name: CREATIVE_REFRESH_JOBS.REFRESH_EXPIRING_CREATIVES,
    concurrency: 1,
  })
  async handleRefreshExpiringCreatives(job: Job) {
    return this.batchRunLogger.track(
      CREATIVE_REFRESH_JOBS.REFRESH_EXPIRING_CREATIVES,
      CREATIVE_REFRESH_QUEUE,
      async () => {
        this.logger.log('[JOB START] Refresh Expiring Creatives');
        await this.service.refreshExpiringCreatives();
        this.logger.log('[JOB FINISHED] Refresh Expiring Creatives');
      },
    );
  }
}
