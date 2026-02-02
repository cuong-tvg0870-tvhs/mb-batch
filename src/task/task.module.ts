import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';

import { UpsertDataModule } from 'src/modules/campaign-sync-service/upsert.module';
import { MetaModule } from 'src/modules/meta/meta.module';
import { PrismaModule } from 'src/modules/prisma/prisma.module';
import { TaskCron } from './task.cron';
import { TaskProcessor } from './task.processor';
import { TaskService } from './task.service';

@Module({
  imports: [
    ScheduleModule.forRoot(),
    BullModule.registerQueue({ name: 'meta-sync' }),
    MetaModule,
    PrismaModule,
    UpsertDataModule,
  ],
  providers: [TaskService, TaskCron, TaskProcessor],
})
export class TaskModule {}
