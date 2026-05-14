import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { META_SYNC_QUEUE } from './meta-sync.constants';
import { MetaSyncProcessor } from './meta-sync.processor';
import { MetaSyncScheduler } from './meta-sync.scheduler';
import { MetaSyncService } from './meta-sync.service';

@Module({
  imports: [
    PrismaModule,
    BullModule.registerQueue({
      name: META_SYNC_QUEUE,
    }),
  ],
  providers: [MetaSyncService, MetaSyncProcessor, MetaSyncScheduler],
  exports: [MetaSyncService],
})
export class MetaSyncModule {}
