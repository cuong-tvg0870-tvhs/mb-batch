import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { META_MEDIA_UPLOAD_QUEUE } from './meta-media-upload.constants';
import { MetaMediaUploadProcessor } from './meta-media-upload.processor';
import { MetaMediaUploadScheduler } from './meta-media-upload.scheduler';
import { MetaMediaUploadService } from './meta-media-upload.service';

@Module({
  imports: [
    PrismaModule,
    BullModule.registerQueue({
      name: META_MEDIA_UPLOAD_QUEUE,
    }),
  ],
  providers: [
    MetaMediaUploadService,
    MetaMediaUploadProcessor,
    MetaMediaUploadScheduler,
  ],
  exports: [MetaMediaUploadService],
})
export class MetaMediaUploadModule {}
