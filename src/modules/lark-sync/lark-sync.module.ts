import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { LARK_SYNC_QUEUE } from './lark-sync.constants';
import { LarkSyncProcessor } from './lark-sync.processor';
import { LarkSyncScheduler } from './lark-sync.scheduler';
import { LarkSyncService } from './lark-sync.service';

@Module({
  imports: [
    PrismaModule,
    BullModule.registerQueue({
      name: LARK_SYNC_QUEUE,
    }),
  ],
  providers: [LarkSyncService, LarkSyncProcessor, LarkSyncScheduler],
  exports: [LarkSyncService],
})
export class LarkSyncModule {}
