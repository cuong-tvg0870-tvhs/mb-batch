import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { Job } from 'bull';
import { BatchRunLoggerService } from '../batch-run-log/batch-run-logger.service';
import { LARK_SYNC_JOBS, LARK_SYNC_QUEUE } from './lark-sync.constants';
import { LarkSyncService } from './lark-sync.service';

@Processor(LARK_SYNC_QUEUE)
export class LarkSyncProcessor {
  private readonly logger = new Logger(LarkSyncProcessor.name);

  constructor(
    private readonly larkSyncService: LarkSyncService,
    private readonly batchRunLogger: BatchRunLoggerService,
  ) {}

  @Process({ name: LARK_SYNC_JOBS.SYNC_WORKFLOW, concurrency: 1 })
  async handleSyncWorkflow(job: Job) {
    return this.batchRunLogger.track(
      LARK_SYNC_JOBS.SYNC_WORKFLOW,
      LARK_SYNC_QUEUE,
      async () => {
        this.logger.log('🚀 [JOB START] Lark <-> Drive Sync Workflow');
        await this.larkSyncService.syncLarkToDrive();
        this.logger.log('✨ [JOB FINISHED] Lark <-> Drive Sync Workflow');
      },
    );
  }
}
