/**
 * The Supabase implementation of `SyncStore`.
 *
 * `runSync` is written against an interface so it can be tested without a database. This is
 * the real backing: it turns the runner's claim/cursor/outcome calls into rows in
 * `sync_runs`, `sync_cursors` and `integration_connections`.
 *
 * The client is injected rather than constructed here, so tests can pass a fake and callers
 * decide whether they hold a service-role or user-scoped client. Connector workers must pass
 * a service-role client: every table below is behind row-level security.
 */

import type { SupabaseClient } from "@supabase/supabase-js";
import type { SyncJob, SyncRunRecord, SyncStatus, SyncStore } from "./sync-runner";

const UNIQUE_VIOLATION = "23505";

/**
 * How long a run may go without progress before another worker may take it over.
 *
 * A worker killed mid-run leaves its row in `running` for ever; without a timeout that job key
 * could never be synced again. This used to be six hours, on the reasoning that the window had
 * to exceed the longest imaginable backfill. That reasoning had a cost nobody had priced: six
 * hours after the 03:00 cron is 09:00, and the cron does not fire again until 03:00, so a
 * single timeout put the job beyond reach until the following night.
 *
 * Measuring from the heartbeat instead of from the start removes the trade-off. A backfill
 * that is genuinely working moves its own deadline after every page, so it cannot be stolen
 * however long it runs, while one that has stopped is reclaimable within the same night.
 */
const STALE_RUN_MS = 15 * 60 * 1000;

interface SyncRunRow {
  id: string;
  job_key: string;
  status: SyncStatus;
  attempt_count: number;
  records_received: number;
  records_written: number;
  error_code: string | null;
  error_message: string | null;
  started_at: string | null;
  heartbeat_at: string | null;
}

const RUN_COLUMNS =
  "id, job_key, status, attempt_count, records_received, records_written, error_code, error_message, started_at, heartbeat_at";

function toRecord(row: SyncRunRow): SyncRunRecord {
  return {
    id: row.id,
    jobKey: row.job_key,
    status: row.status,
    attemptCount: row.attempt_count,
    recordsReceived: row.records_received,
    recordsWritten: row.records_written,
    errorCode: row.error_code,
    errorMessage: row.error_message,
  };
}

/**
 * Whether a run has gone quiet long enough to be taken over.
 *
 * The heartbeat is the measure; `started_at` is the fallback for a run that was killed before
 * writing its first page, and for rows created before the heartbeat column existed.
 */
function isStale(row: Pick<SyncRunRow, "started_at" | "heartbeat_at">, now: number): boolean {
  const lastSeen = row.heartbeat_at ?? row.started_at;
  if (lastSeen === null) return false;
  return now - Date.parse(lastSeen) > STALE_RUN_MS;
}

export function createSupabaseSyncStore(client: SupabaseClient): SyncStore {
  return {
    /**
     * Returns null when this job key must not run: either it already succeeded, or another
     * worker is running it right now.
     *
     * Note the runner reports both cases as `already_succeeded`, because `SyncStore` has no
     * way to distinguish them in its return type. The data is safe either way — every write
     * is an upsert on the provider's external ID — but the outcome label is imprecise for
     * the in-flight case. Worth revisiting when the cron routes land and concurrent workers
     * become possible.
     */
    async claimRun(job: SyncJob<unknown>): Promise<SyncRunRecord | null> {
      const insert = await client
        .from("sync_runs")
        .insert({
          connection_id: job.connectionId,
          job_key: job.jobKey,
          resource_name: job.resourceName,
          status: "queued" satisfies SyncStatus,
          attempt_count: 1,
        })
        .select(RUN_COLUMNS)
        .single<SyncRunRow>();

      if (!insert.error) return toRecord(insert.data);
      if (insert.error.code !== UNIQUE_VIOLATION) throw insert.error;

      // The job key is taken. Decide whether this is a completed run, a live one, or the
      // remains of a worker that died.
      const existing = await client
        .from("sync_runs")
        .select(RUN_COLUMNS)
        .eq("job_key", job.jobKey)
        .single<SyncRunRow>();

      if (existing.error) throw existing.error;
      const row = existing.data;

      if (row.status === "succeeded") return null;

      const reclaimable =
        row.status === "failed" ||
        row.status === "cancelled" ||
        isStale(row, Date.now());
      if (!reclaimable) return null;

      // Compare-and-set on attempt_count. If another worker claimed this row between the
      // select and here, its attempt_count no longer matches and this update touches no
      // rows — so exactly one worker wins, without a database function or an advisory lock.
      const claimed = await client
        .from("sync_runs")
        .update({
          status: "queued" satisfies SyncStatus,
          attempt_count: row.attempt_count + 1,
          started_at: null,
          heartbeat_at: null,
          completed_at: null,
          error_code: null,
          error_message: null,
        })
        .eq("job_key", job.jobKey)
        .eq("attempt_count", row.attempt_count)
        .select(RUN_COLUMNS)
        .maybeSingle<SyncRunRow>();

      if (claimed.error) throw claimed.error;
      return claimed.data ? toRecord(claimed.data) : null;
    },

    async markRunning(runId: string): Promise<void> {
      const now = new Date().toISOString();
      const { error } = await client
        .from("sync_runs")
        .update({ status: "running" satisfies SyncStatus, started_at: now, heartbeat_at: now })
        .eq("id", runId);
      if (error) throw error;
    },

    async heartbeat(runId: string): Promise<void> {
      const { error } = await client
        .from("sync_runs")
        .update({ heartbeat_at: new Date().toISOString() })
        .eq("id", runId);
      if (error) throw error;
    },

    async completeRun(runId: string, totals: { received: number; written: number }): Promise<void> {
      const { error } = await client
        .from("sync_runs")
        .update({
          status: "succeeded" satisfies SyncStatus,
          completed_at: new Date().toISOString(),
          records_received: totals.received,
          records_written: totals.written,
        })
        .eq("id", runId);
      if (error) throw error;
    },

    async failRun(runId: string, failure: { code: string; message: string }): Promise<void> {
      const { error } = await client
        .from("sync_runs")
        .update({
          status: "failed" satisfies SyncStatus,
          completed_at: new Date().toISOString(),
          error_code: failure.code,
          error_message: failure.message,
        })
        .eq("id", runId);
      if (error) throw error;
    },

    async getCursor(connectionId: string, resourceName: string): Promise<string | null> {
      const { data, error } = await client
        .from("sync_cursors")
        .select("cursor_value")
        .eq("connection_id", connectionId)
        .eq("resource_name", resourceName)
        .maybeSingle<{ cursor_value: string | null }>();
      if (error) throw error;
      return data?.cursor_value ?? null;
    },

    async setCursor(
      connectionId: string,
      resourceName: string,
      cursor: string | null,
      watermarkAt: string | null,
    ): Promise<void> {
      const { error } = await client.from("sync_cursors").upsert(
        {
          connection_id: connectionId,
          resource_name: resourceName,
          cursor_value: cursor,
          watermark_at: watermarkAt,
          updated_at: new Date().toISOString(),
        },
        { onConflict: "connection_id,resource_name" },
      );
      if (error) throw error;
    },

    async recordConnectionAttempt(connectionId: string, succeeded: boolean): Promise<void> {
      const now = new Date().toISOString();
      const { error } = await client
        .from("integration_connections")
        .update(succeeded ? { last_attempt_at: now, last_success_at: now } : { last_attempt_at: now })
        .eq("id", connectionId);
      if (error) throw error;
    },
  };
}
