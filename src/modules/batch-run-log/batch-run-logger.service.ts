import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
} from '@nestjs/common';
import { randomUUID } from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import {
  BatchRunContext,
  BatchRunLogEntry,
  BatchRunningMarker,
  BatchRunRecord,
  BatchRunStatus,
} from './batch-run-log.types';
import {
  batchLogDateKey,
  batchLogFileName,
  resolveBatchLogDir,
  resolveRunningDir,
} from './batch-run-log.util';

/** Truncate very long error/stack text so a single line can't bloat the file. */
const MAX_ERROR_LEN = 8000;
const MAX_LOG_ENTRIES = 200;
// Marker "đang chạy" tồn tại lâu hơn ngưỡng này = tàn dư của lần crash/deploy giữa
// chừng (không có job thật nào chạy 6 giờ). Dọn lúc boot để dashboard không hiển thị
// "đang chạy" ma. Ngưỡng đủ rộng để KHÔNG nhả nhầm job dài đang chạy ở replica khác.
const STALE_MARKER_MS = 6 * 60 * 60 * 1000;

@Injectable()
export class BatchRunLoggerService implements OnApplicationBootstrap {
  private readonly logger = new Logger(BatchRunLoggerService.name);

  /**
   * Lúc app khởi động (sau mỗi deploy), quét thư mục `running/` và xoá các marker
   * treo — job bị SIGKILL giữa chừng để lại file marker không bao giờ được dọn ở
   * `finally`. Dựa trên `startedAt` nên an toàn với multi-replica (không đụng job
   * mới bắt đầu ở instance khác).
   */
  onApplicationBootstrap(): void {
    this.sweepStaleMarkers();
  }

  private sweepStaleMarkers(): void {
    try {
      const dir = resolveRunningDir();
      if (!fs.existsSync(dir)) return;
      const now = Date.now();
      let removed = 0;
      for (const file of fs.readdirSync(dir)) {
        if (!file.endsWith('.json')) continue;
        const full = path.join(dir, file);
        try {
          const marker = JSON.parse(
            fs.readFileSync(full, 'utf8'),
          ) as BatchRunningMarker;
          const started = marker.startedAt
            ? new Date(marker.startedAt).getTime()
            : 0;
          if (!started || now - started > STALE_MARKER_MS) {
            fs.unlinkSync(full);
            removed += 1;
          }
        } catch {
          // marker hỏng/không đọc được → coi như rác, xoá.
          try {
            fs.unlinkSync(full);
            removed += 1;
          } catch {
            // best-effort
          }
        }
      }
      if (removed > 0) {
        this.logger.warn(
          `Đã dọn ${removed} marker "đang chạy" treo (crash/deploy giữa chừng).`,
        );
      }
    } catch (e) {
      this.logger.error(
        `Sweep stale markers lỗi: ${(e as Error)?.message ?? e}`,
      );
    }
  }

  /**
   * Wrap a batch job body. Records start/end time, duration, status and any
   * counts/warnings/errors the job reports via the context, then appends one
   * JSON line to the day's log file. Logging never breaks the job: a failure to
   * write is swallowed (and logged to stdout). The original error is re-thrown
   * so Bull still sees the job as failed.
   */
  async track<T>(
    jobName: string,
    queue: string | null,
    fn: (ctx: BatchRunContext) => Promise<T>,
  ): Promise<T> {
    const id = randomUUID();
    const startedAt = new Date();
    this.writeRunningMarker({
      id,
      queue,
      jobName,
      startedAt: startedAt.toISOString(),
    });

    let totalItems = 0;
    let totalExplicit = false;
    let successCount = 0;
    let failureCount = 0;
    let warningCount = 0;
    let skipped = false;
    let meta: Record<string, unknown> | null = null;
    const logs: BatchRunLogEntry[] = [];

    const pushLog = (level: BatchRunLogEntry['level'], message: string) => {
      if (logs.length < MAX_LOG_ENTRIES) {
        logs.push({ level, message, ts: new Date().toISOString() });
      }
    };

    const ctx: BatchRunContext = {
      setTotal: (n) => {
        totalItems = Math.max(0, Math.trunc(n));
        totalExplicit = true;
      },
      addTotal: (n = 1) => {
        totalItems += n;
        totalExplicit = true;
      },
      // success/failure are tracked independently; when the caller never sets a
      // total explicitly, total is derived as success + failure on finalize.
      addSuccess: (n = 1) => {
        successCount += n;
      },
      addFailure: (n = 1) => {
        failureCount += n;
      },
      warn: (message) => {
        warningCount += 1;
        pushLog('warn', message);
      },
      error: (message) => {
        pushLog('error', message);
      },
      skip: (reason) => {
        skipped = true;
        if (reason) pushLog('warn', reason);
      },
      setMeta: (m) => {
        meta = { ...(meta ?? {}), ...m };
      },
    };

    let status: BatchRunStatus = 'COMPLETED';
    let errorText: string | null = null;

    try {
      const result = await fn(ctx);
      // FAILED is reserved for a job that threw; per-item failures are surfaced
      // via failureCount (the dashboard flags partial-failure runs separately).
      status = skipped ? 'SKIPPED' : 'COMPLETED';
      return result;
    } catch (err) {
      status = 'FAILED';
      errorText = this.stringifyError(err);
      pushLog('error', errorText);
      throw err;
    } finally {
      this.removeRunningMarker(id);
      const finishedAt = new Date();
      const record: BatchRunRecord = {
        id,
        queue,
        jobName,
        status,
        startedAt: startedAt.toISOString(),
        finishedAt: finishedAt.toISOString(),
        durationMs: finishedAt.getTime() - startedAt.getTime(),
        totalItems: totalExplicit ? totalItems : successCount + failureCount,
        successCount,
        failureCount,
        warningCount,
        error: errorText ? errorText.slice(0, MAX_ERROR_LEN) : null,
        logs,
        meta,
      };
      this.append(record, startedAt);
    }
  }

  private writeRunningMarker(marker: BatchRunningMarker): void {
    try {
      const dir = resolveRunningDir();
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(
        path.join(dir, `${marker.id}.json`),
        JSON.stringify(marker),
      );
    } catch (e) {
      this.logger.error(
        `Failed to write running marker for ${marker.jobName}: ${
          (e as Error)?.message ?? e
        }`,
      );
    }
  }

  private removeRunningMarker(id: string): void {
    try {
      const file = path.join(resolveRunningDir(), `${id}.json`);
      if (fs.existsSync(file)) fs.unlinkSync(file);
    } catch {
      // best-effort; a leftover marker is just surfaced as a stale run.
    }
  }

  private append(record: BatchRunRecord, startedAt: Date): void {
    try {
      const dir = resolveBatchLogDir();
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      const file = path.join(dir, batchLogFileName(batchLogDateKey(startedAt)));
      fs.appendFileSync(file, JSON.stringify(record) + '\n');
    } catch (e) {
      // Never let log persistence break a job.
      this.logger.error(
        `Failed to write batch run log for ${record.jobName}: ${
          (e as Error)?.message ?? e
        }`,
      );
    }
  }

  private stringifyError(err: unknown): string {
    if (err instanceof Error) return err.stack || `${err.name}: ${err.message}`;
    if (typeof err === 'string') return err;
    try {
      return JSON.stringify(err);
    } catch {
      return String(err);
    }
  }
}
