import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { Job } from 'bull';
import { BatchRunLoggerService } from '../batch-run-log/batch-run-logger.service';
import { ENTITY_SYNC_JOBS, ENTITY_SYNC_QUEUE } from './entity-sync.constants';
import { EntitySyncService } from './entity-sync.service';

@Processor(ENTITY_SYNC_QUEUE)
export class EntitySyncProcessor {
  private readonly logger = new Logger(EntitySyncProcessor.name);

  constructor(
    private readonly entitySyncService: EntitySyncService,
    private readonly batchRunLogger: BatchRunLoggerService,
  ) {}

  @Process({ name: ENTITY_SYNC_JOBS.SYNC_META_ENTITIES, concurrency: 1 })
  async handleSyncMetaEntities(_job: Job) {
    return this.batchRunLogger.track(
      ENTITY_SYNC_JOBS.SYNC_META_ENTITIES,
      ENTITY_SYNC_QUEUE,
      async (ctx) => {
        this.logger.log('🚀 [JOB START] Sync Meta Entities');
        const stats = await this.entitySyncService.syncAll(ctx);

        // Đơn vị công việc = số TKQC; fanpage/catalog surface qua meta.
        ctx.setTotal(stats.accountsTotal);
        ctx.addSuccess(stats.accountsOk);
        ctx.addFailure(stats.accountsFailed);
        ctx.setMeta({
          fanpages: stats.fanpages,
          fanpagesWithWhatsapp: stats.fanpagesWithWhatsapp,
          fanpagesWithInstagram: stats.fanpagesWithInstagram,
          pixels: stats.pixels,
          audiences: stats.audiences,
          catalogs: stats.catalogs,
          productSets: stats.productSets,
          productFeeds: stats.productFeeds,
        });

        this.logger.log(
          `✨ [JOB FINISHED] Sync Meta Entities — accounts ${stats.accountsOk}/${stats.accountsTotal}, ` +
            `fanpages ${stats.fanpages} (WA ${stats.fanpagesWithWhatsapp}/IG ${stats.fanpagesWithInstagram}), ` +
            `pixels ${stats.pixels}, audiences ${stats.audiences}, catalogs ${stats.catalogs}`,
        );
      },
    );
  }
}
