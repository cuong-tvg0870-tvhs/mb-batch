import { Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';

@Processor('meta-sync')
export class TaskProcessor {
  private readonly logger = new Logger(TaskProcessor.name);

  constructor() {}
}
