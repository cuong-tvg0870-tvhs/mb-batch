import { Global, Module } from '@nestjs/common';
import { AppConfigModule } from '../app-config/app-config.module';
import { BatchLogCleanupScheduler } from './batch-log-cleanup.scheduler';
import { BatchRunLoggerService } from './batch-run-logger.service';

/**
 * Provides the BatchRunLoggerService application-wide so any processor can wrap
 * its job body with `track(...)` without importing this module explicitly.
 */
@Global()
@Module({
  imports: [AppConfigModule],
  providers: [BatchRunLoggerService, BatchLogCleanupScheduler],
  exports: [BatchRunLoggerService],
})
export class BatchRunLogModule {}
