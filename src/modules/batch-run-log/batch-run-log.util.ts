import * as fs from 'fs';
import * as path from 'path';

/**
 * Directory where batch run logs are written. In production this resolves to
 * `/app/files/batch-logs`, which lives on the `shared_files` Docker volume that
 * mb-ads also mounts (so the dashboard can read the same files). Locally it
 * falls back to `./files/batch-logs`. Override with BATCH_LOG_DIR if needed.
 */
export function resolveBatchLogDir(): string {
  if (process.env.BATCH_LOG_DIR) return process.env.BATCH_LOG_DIR;
  const base = fs.existsSync('/app')
    ? '/app/files'
    : path.join(process.cwd(), 'files');
  return path.join(base, 'batch-logs');
}

/**
 * Sub-directory holding one marker file per currently-running job
 * (`<id>.json`). The marker is created when a run starts and deleted when it
 * finishes; a leftover marker means the run is still in progress — or crashed
 * mid-run if it's been there too long.
 */
export function resolveRunningDir(): string {
  return path.join(resolveBatchLogDir(), 'running');
}

/** Local (Asia/Ho_Chi_Minh) calendar date as YYYY-MM-DD for the given instant. */
export function batchLogDateKey(date: Date): string {
  // en-CA formats as YYYY-MM-DD
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Ho_Chi_Minh',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

/** Filename for a given date key, e.g. `runs-2026-06-28.jsonl`. */
export function batchLogFileName(dateKey: string): string {
  return `runs-${dateKey}.jsonl`;
}

const FILE_RE = /^runs-(\d{4}-\d{2}-\d{2})\.jsonl$/;

/** Extract the YYYY-MM-DD date key from a log filename, or null if it doesn't match. */
export function batchLogDateFromFile(fileName: string): string | null {
  const m = FILE_RE.exec(fileName);
  return m ? m[1] : null;
}
