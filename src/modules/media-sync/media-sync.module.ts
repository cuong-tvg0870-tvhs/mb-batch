import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { MEDIA_SYNC_QUEUE } from './media-sync.constants';
import { MediaSyncProcessor } from './media-sync.processor';
import { MediaSyncScheduler } from './media-sync.scheduler';
import { MediaSyncService } from './media-sync.service';

@Module({
  imports: [
    PrismaModule,
    BullModule.registerQueue({
      name: MEDIA_SYNC_QUEUE,
    }),
  ],
  providers: [MediaSyncService, MediaSyncProcessor, MediaSyncScheduler],
  exports: [MediaSyncService],
})
export class MediaSyncModule {}
