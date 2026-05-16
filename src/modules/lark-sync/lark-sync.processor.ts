import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { Job } from 'bull';
import { LARK_SYNC_JOBS, LARK_SYNC_QUEUE } from './lark-sync.constants';
import { LarkSyncService } from './lark-sync.service';

@Processor(LARK_SYNC_QUEUE)
export class LarkSyncProcessor {
  private readonly logger = new Logger(LarkSyncProcessor.name);

  constructor(private readonly larkSyncService: LarkSyncService) {}

  @Process({ name: LARK_SYNC_JOBS.SYNC_WORKFLOW, concurrency: 1 })
  async handleSyncWorkflow(job: Job) {
    this.logger.log('🚀 [JOB START] Lark <-> Drive Sync Workflow');
    await this.larkSyncService.syncLarkToDrive();
    this.logger.log('✨ [JOB FINISHED] Lark <-> Drive Sync Workflow');
  }

  @Process({ name: LARK_SYNC_JOBS.META_UPLOAD_WORKFLOW, concurrency: 1 })
  async handleMetaUploadWorkflow(job: Job) {
    this.logger.log('🚀 [JOB START] Meta Upload Workflow');
    await this.larkSyncService.ensureFolderMeta();
    // await this.larkSyncService.uploadDriveToMeta(10);
    // this.logger.log('✨ [JOB FINISHED] Meta Upload Workflow');
  }

  // @Process({ name: LARK_SYNC_JOBS.CLEANUP_DUPLICATE_FOLDERS, concurrency: 1 })
  // async handleCleanupDuplicateFolders(job: Job) {
  //   this.logger.log('🚀 [JOB START] Cleanup Duplicate Folders');
  //   await this.larkSyncService.cleanupDuplicateFolders();
  //   this.logger.log('✨ [JOB FINISHED] Cleanup Duplicate Folders');
  // }
}
