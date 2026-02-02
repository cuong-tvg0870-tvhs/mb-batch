// import { TelegramService } from '@app/modules/telegram/telegram.service';
import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { TaskJobName } from './task.dto';

@Processor('task-queue') // Tên queue
export class TaskProcessor {
  private readonly logger = new Logger(TaskProcessor.name);
  constructor() {}

  @Process(TaskJobName.UPDATE_BATCH_LOGS)
  async handleUpdateBatchLogs() {
    const threeHoursAgo = new Date(Date.now() - 3 * 60 * 60 * 1000);
    console.log(threeHoursAgo);
  }
}
