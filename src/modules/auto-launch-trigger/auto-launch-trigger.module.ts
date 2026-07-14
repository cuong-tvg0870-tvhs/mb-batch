import { Module } from '@nestjs/common';
import { AutoLaunchTriggerScheduler } from './auto-launch-trigger.scheduler';

// Cron kích hoạt "Quy tắc tự lên Camp" tới hạn (POST sang mb-ads /run-due).
@Module({
  providers: [AutoLaunchTriggerScheduler],
})
export class AutoLaunchTriggerModule {}
