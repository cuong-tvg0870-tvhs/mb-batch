import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { INSIGHT_SYNC_QUEUE } from './insight-sync.constants';
import { InsightSyncProcessor } from './insight-sync.processor';
import { InsightSyncScheduler } from './insight-sync.scheduler';
import { InsightSyncService } from './insight-sync.service';

@Module({
  imports: [
    PrismaModule,
    BullModule.registerQueue({
      name: INSIGHT_SYNC_QUEUE,
    }),
  ],
  providers: [
    InsightSyncService,
    InsightSyncProcessor,
    InsightSyncScheduler,
  ],
  exports: [InsightSyncService],
})
export class InsightSyncModule {}
