import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import * as fs from 'fs';
import * as path from 'path';
import {
  batchLogDateFromFile,
  resolveBatchLogDir,
  resolveRunningDir,
} from './batch-run-log.util';

/** Keep batch run logs for this many days, then delete the day's file. */
const RETENTION_DAYS = 90;

/**
 * Remove orphaned "running" markers older than this. A marker normally lives
 * only for the job's duration; one this old means the process crashed mid-run
 * and never cleaned up. No real job here runs anywhere near 24h.
 */
const RUNNING_MARKER_MAX_AGE_MS = 24 * 60 * 60 * 1000;

@Injectable()
export class BatchLogCleanupScheduler {
  private readonly logger = new Logger(BatchLogCleanupScheduler.name);

  // Every day at 03:00 Asia/Ho_Chi_Minh.
  @Cron('0 3 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  cleanupOldLogs() {
    const dir = resolveBatchLogDir();
    if (!fs.existsSync(dir)) return;

    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - RETENTION_DAYS);

    let removed = 0;
    try {
      for (const fileName of fs.readdirSync(dir)) {
        const dateKey = batchLogDateFromFile(fileName);
        if (!dateKey) continue;
        // dateKey is YYYY-MM-DD; compare at day granularity.
        if (new Date(`${dateKey}T00:00:00+07:00`) < cutoff) {
          fs.unlinkSync(path.join(dir, fileName));
          removed += 1;
        }
      }
      if (removed > 0) {
        this.logger.log(
          `Removed ${removed} batch run log file(s) older than ${RETENTION_DAYS} days.`,
        );
      }
    } catch (e) {
      this.logger.error(
        `Failed to clean up batch run logs: ${(e as Error)?.message ?? e}`,
      );
    }

    this.cleanupOrphanedMarkers();
  }

  /** Drop "running" markers left behind by crashed runs. */
  private cleanupOrphanedMarkers() {
    const dir = resolveRunningDir();
    if (!fs.existsSync(dir)) return;

    const now = Date.now();
    let removed = 0;
    try {
      for (const fileName of fs.readdirSync(dir)) {
        if (!fileName.endsWith('.json')) continue;
        const file = path.join(dir, fileName);
        const age = now - fs.statSync(file).mtimeMs;
        if (age > RUNNING_MARKER_MAX_AGE_MS) {
          fs.unlinkSync(file);
          removed += 1;
        }
      }
      if (removed > 0) {
        this.logger.log(`Removed ${removed} orphaned running marker(s).`);
      }
    } catch (e) {
      this.logger.error(
        `Failed to clean up running markers: ${(e as Error)?.message ?? e}`,
      );
    }
  }
}
