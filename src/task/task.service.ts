import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { Queue } from 'bull';
import { TaskJobName } from './task.dto';

@Injectable()
export class TaskService {
  private readonly logger = new Logger(TaskService.name);
  constructor(@InjectQueue('task-queue') private taskQueue: Queue) {}

  async onModuleInit() {
    this.logger.log('✅ Module initialized, starting crawler...');
    // await this.handleUpdateBatchLogs();
  }

  @Cron(CronExpression.EVERY_5_SECONDS)
  async check() {
    console.log('test');

    await this.taskQueue.add(TaskJobName.UPDATE_BATCH_LOGS, {
      time: new Date().toISOString(),
      log_id: 'test',
    });
  }
}
