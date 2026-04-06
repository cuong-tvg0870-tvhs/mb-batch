import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';

import { MetaModule } from 'src/modules/meta/meta.module';
import { PrismaModule } from 'src/modules/prisma/prisma.module';
import { TaskCron } from './task.cron';
import { TaskProcessor } from './task.processor';

@Module({
  imports: [
    ScheduleModule.forRoot(),
    BullModule.registerQueue({ name: 'meta-sync' }),
    MetaModule,
    PrismaModule,
  ],
  providers: [TaskCron, TaskProcessor],
})
export class TaskModule {}
