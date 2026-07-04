import { Module } from '@nestjs/common';
import { AppConfigReader } from './app-config.reader';

// PrismaModule là @Global. Module nào cần đọc "knob sản phẩm" (draft_cleanup_days,
// run_log_retention_days, ai_triage_enabled...) thì import AppConfigModule để dùng
// AppConfigReader. Nguồn cấu hình: mb-ads quản (registry + admin UI); mb-batch chỉ đọc.
@Module({
  providers: [AppConfigReader],
  exports: [AppConfigReader],
})
export class AppConfigModule {}
