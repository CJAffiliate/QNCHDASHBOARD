/**
 * Generic sync orchestration.
 *
 * Re-running a sync must never duplicate data. Two mechanisms enforce that:
 *
 *  1. `job_key` is unique in `sync_runs`, so a second attempt at the same logical job is
 *     recognised rather than executed again.
 *  2. Every write is an upsert keyed on the provider's external ID.
 *
 * The store is an interface so the runner is tested against an in-memory fake, without a
 * database and without provider credentials.
 */

export type SyncStatus = "queued" | "running" | "succeeded" | "failed" | "cancelled";
export type IntegrationProvider = "shopify" | "meta" | "tiktok" | "xero" | "google_sheets";

export interface SyncPage<T> {
  records: T[];
  /** Cursor to resume from, or null when the final page has been read. */
  nextCursor: string | null;
  /** Highest source timestamp seen, stored as the incremental watermark. */
  watermarkAt?: string | null;
}

export interface SyncJob<T> {
  provider: IntegrationProvider;
  resourceName: string;
  connectionId: string;
  /** Stable identity for this logical run, e.g. `shopify:orders:2026-06-30`. */
  jobKey: string;
  /** Reads one page. Receives the stored cursor on the first call when resuming. */
  fetchPage: (cursor: string | null) => Promise<SyncPage<T>>;
  /** Must upsert by external ID. Returns the number of rows written. */
  upsert: (records: T[]) => Promise<number>;
  /** Safety valve against an API that never reports the last page. */
  maxPages?: number;
}

export interface SyncRunRecord {
  id: string;
  jobKey: string;
  status: SyncStatus;
  attemptCount: number;
  recordsReceived: number;
  recordsWritten: number;
  errorCode?: string | null;
  errorMessage?: string | null;
}

export interface SyncStore {
  /**
   * Reserves the job key. Returns null when a run for this key already succeeded, which is
   * what makes a repeated sync a no-op rather than a duplicate import.
   */
  claimRun: (job: SyncJob<unknown>) => Promise<SyncRunRecord | null>;
  markRunning: (runId: string) => Promise<void>;
  /**
   * Says the run is still alive, after each page is durably written.
   *
   * Staleness has to be measured from something a working job can move, otherwise the window
   * before another worker may reclaim a job key must be longer than the longest imaginable
   * backfill — which is what previously let a killed run hold its key for six hours, past the
   * point where retrying the same night was possible.
   */
  heartbeat: (runId: string) => Promise<void>;
  completeRun: (runId: string, totals: { received: number; written: number }) => Promise<void>;
  failRun: (runId: string, error: { code: string; message: string }) => Promise<void>;
  getCursor: (connectionId: string, resourceName: string) => Promise<string | null>;
  setCursor: (
    connectionId: string,
    resourceName: string,
    cursor: string | null,
    watermarkAt: string | null,
  ) => Promise<void>;
  recordConnectionAttempt: (connectionId: string, succeeded: boolean) => Promise<void>;
}

export type SyncOutcome =
  | { status: "skipped"; reason: "already_succeeded"; jobKey: string }
  | { status: "succeeded"; jobKey: string; pages: number; received: number; written: number }
  | { status: "failed"; jobKey: string; pages: number; received: number; written: number; error: Error };

const DEFAULT_MAX_PAGES = 1000;

export async function runSync<T>(job: SyncJob<T>, store: SyncStore): Promise<SyncOutcome> {
  const run = await store.claimRun(job as SyncJob<unknown>);
  if (!run) return { status: "skipped", reason: "already_succeeded", jobKey: job.jobKey };

  await store.markRunning(run.id);

  let cursor = await store.getCursor(job.connectionId, job.resourceName);
  let pages = 0;
  let received = 0;
  let written = 0;
  let watermarkAt: string | null = null;

  const maxPages = job.maxPages ?? DEFAULT_MAX_PAGES;

  try {
    do {
      const page = await job.fetchPage(cursor);
      pages += 1;
      received += page.records.length;

      if (page.records.length > 0) written += await job.upsert(page.records);
      if (page.watermarkAt) watermarkAt = maxIso(watermarkAt, page.watermarkAt);

      cursor = page.nextCursor;

      // The cursor is advanced after each page is durably written, so an interrupted run
      // resumes from the last completed page instead of restarting or skipping records.
      await store.setCursor(job.connectionId, job.resourceName, cursor, watermarkAt);

      // Paired with the cursor write: this run has demonstrably made progress, so it is not
      // abandoned and its job key must not be taken from it.
      await store.heartbeat(run.id);

      if (pages >= maxPages && cursor !== null) {
        throw new Error(`${job.resourceName} exceeded ${maxPages} pages without completing`);
      }
    } while (cursor !== null);

    await store.completeRun(run.id, { received, written });
    await store.recordConnectionAttempt(job.connectionId, true);
    return { status: "succeeded", jobKey: job.jobKey, pages, received, written };
  } catch (caught) {
    const error = caught instanceof Error ? caught : new Error(String(caught));
    await store.failRun(run.id, { code: error.name, message: error.message });
    await store.recordConnectionAttempt(job.connectionId, false);
    return { status: "failed", jobKey: job.jobKey, pages, received, written, error };
  }
}

function maxIso(current: string | null, candidate: string): string {
  return current === null || candidate > current ? candidate : current;
}

/** Runs jobs in order, continuing past a failure so one broken provider cannot block the rest. */
export async function runSyncSequence(jobs: readonly SyncJob<never>[], store: SyncStore): Promise<SyncOutcome[]> {
  const outcomes: SyncOutcome[] = [];
  for (const job of jobs) {
    outcomes.push(await runSync(job, store));
  }
  return outcomes;
}

export const buildJobKey = (provider: IntegrationProvider, resource: string, discriminator: string): string =>
  `${provider}:${resource}:${discriminator}`;
