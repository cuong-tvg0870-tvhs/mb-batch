import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { CREATIVE_REFRESH_QUEUE } from './creative-refresh.constants';
import { CreativeRefreshProcessor } from './creative-refresh.processor';
import { CreativeRefreshScheduler } from './creative-refresh.scheduler';
import { CreativeRefreshService } from './creative-refresh.service';

@Module({
  imports: [
    PrismaModule,
    BullModule.registerQueue({
      name: CREATIVE_REFRESH_QUEUE,
    }),
  ],
  providers: [
    CreativeRefreshService,
    CreativeRefreshProcessor,
    CreativeRefreshScheduler,
  ],
  exports: [CreativeRefreshService],
})
export class CreativeRefreshModule {}
