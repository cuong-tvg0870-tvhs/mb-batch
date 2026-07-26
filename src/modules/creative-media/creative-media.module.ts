import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { ObjectStorageModule } from '../object-storage/object-storage.module';
import { PrismaModule } from '../prisma/prisma.module';
import { CREATIVE_MEDIA_QUEUE } from './creative-media.constants';
import { CreativeMediaProcessor } from './creative-media.processor';
import { CreativeMediaScheduler } from './creative-media.scheduler';
import { CreativeMediaService } from './creative-media.service';

@Module({
  imports: [
    PrismaModule,
    ObjectStorageModule,
    BullModule.registerQueue({
      name: CREATIVE_MEDIA_QUEUE,
    }),
  ],
  providers: [
    CreativeMediaService,
    CreativeMediaProcessor,
    CreativeMediaScheduler,
  ],
  exports: [CreativeMediaService],
})
export class CreativeMediaModule {}
