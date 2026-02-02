// import { TelegramModule } from '@app/modules/telegram/telegram.module';
import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { TaskProcessor } from './task.processor';
import { TaskService } from './task.service';

@Module({
  imports: [BullModule.registerQueue({ name: 'task-queue' })],
  providers: [TaskProcessor, TaskService],
})
export class TaskModule {}
