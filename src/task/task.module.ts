import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';
import { PrismaModule } from 'src/modules/prisma/prisma.module';
import { LarkCron } from './lark.cron';
import { MediaCron } from './media.cron';
import { MetaCron } from './meta.cron';

@Module({
  imports: [
    ScheduleModule.forRoot(),
    BullModule.registerQueue({ name: 'meta-sync' }),
    PrismaModule,
  ],
  providers: [MediaCron, LarkCron, MetaCron],
})
export class TaskModule {}
