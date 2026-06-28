export type BatchRunStatus = 'COMPLETED' | 'FAILED' | 'SKIPPED';

export type BatchRunLogLevel = 'warn' | 'error';

export interface BatchRunLogEntry {
  level: BatchRunLogLevel;
  message: string;
  ts: string; // ISO timestamp
}

/**
 * One JSON object per finished batch job run.
 * Persisted as a single JSON-line into <base>/batch-logs/runs-YYYY-MM-DD.jsonl
 * (Asia/Ho_Chi_Minh date), shared with mb-ads via the `shared_files` volume.
 */
export interface BatchRunRecord {
  id: string;
  queue: string | null;
  jobName: string;
  status: BatchRunStatus;
  startedAt: string; // ISO
  finishedAt: string; // ISO
  durationMs: number;
  totalItems: number;
  successCount: number;
  failureCount: number;
  warningCount: number;
  error: string | null;
  logs: BatchRunLogEntry[];
  meta: Record<string, unknown> | null;
}

/**
 * Marker written to <base>/batch-logs/running/<id>.json while a run is in
 * progress (deleted on completion). Lets the dashboard show currently-running
 * jobs and detect crashed/hung runs (started but never finished).
 */
export interface BatchRunningMarker {
  id: string;
  queue: string | null;
  jobName: string;
  startedAt: string; // ISO
}

/**
 * Mutable handle passed into the wrapped job body so a job can report what it
 * actually did. Everything is optional — a job that reports nothing still
 * produces a record with timing + status.
 */
export interface BatchRunContext {
  /** Set the total number of items the job will process. */
  setTotal(n: number): void;
  /** Increment the total item count. */
  addTotal(n?: number): void;
  /** Increment the success count (and, by default, the total). */
  addSuccess(n?: number): void;
  /** Increment the failure count (and, by default, the total). */
  addFailure(n?: number): void;
  /** Record a warning message (increments warningCount). */
  warn(message: string): void;
  /** Record a non-fatal error message (does NOT throw or mark the run FAILED). */
  error(message: string): void;
  /** Mark this run as SKIPPED (e.g. nothing to do / lock not acquired). */
  skip(reason?: string): void;
  /** Attach/merge arbitrary context (accountId, dateRange, ...). */
  setMeta(meta: Record<string, unknown>): void;
}
