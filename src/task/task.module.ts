import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';
import { PrismaModule } from 'src/modules/prisma/prisma.module';
import { LarkCron } from './lark.cron';
import { MetaCron } from './meta.cron';

@Module({
  imports: [
    ScheduleModule.forRoot(),
    BullModule.registerQueue({ name: 'meta-sync' }),
    PrismaModule,
  ],
  providers: [LarkCron, MetaCron],
})
export class TaskModule {}
