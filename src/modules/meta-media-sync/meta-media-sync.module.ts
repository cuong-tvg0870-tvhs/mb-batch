import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { MetaApiModule } from '../meta-api/meta-api.module';
import { META_MEDIA_SYNC_QUEUE } from './meta-media-sync.constants';
import { MetaMediaSyncProcessor } from './meta-media-sync.processor';
import { MetaMediaSyncScheduler } from './meta-media-sync.scheduler';
import { MetaMediaSyncService } from './meta-media-sync.service';

@Module({
  imports: [
    PrismaModule,
    MetaApiModule,
    BullModule.registerQueue({
      name: META_MEDIA_SYNC_QUEUE,
    }),
  ],
  providers: [
    MetaMediaSyncService,
    MetaMediaSyncProcessor,
    MetaMediaSyncScheduler,
  ],
  exports: [MetaMediaSyncService],
})
export class MetaMediaSyncModule {}
