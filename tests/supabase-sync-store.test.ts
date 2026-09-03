/**
 * Tests for the Supabase-backed SyncStore.
 *
 * The interesting behaviour is `claimRun`: it decides whether a job key may run at all, and
 * a wrong answer means either a duplicate import or a job that can never run again. It is
 * exercised here against an in-memory fake of the PostgREST query builder — the same
 * approach the runner already uses, so the claim rules are pinned without a live database.
 */

import { describe, expect, it } from "vitest";
import type { SupabaseClient } from "@supabase/supabase-js";
import { createSupabaseSyncStore } from "@/lib/connectors/supabase-sync-store";
import type { SyncJob } from "@/lib/connectors/sync-runner";

import { createFakeSupabase, TABLE_DEFAULTS, type Row } from "./helpers/fake-supabase";

const DEFAULTS = TABLE_DEFAULTS;

function createFakeClient(seed: Record<string, Row[]> = {}) {
  const { client, tables } = createFakeSupabase(seed);
  return { client: client as unknown as SupabaseClient, tables };
}

const job: SyncJob<unknown> = {
  provider: "shopify",
  resourceName: "orders",
  connectionId: "connection-1",
  jobKey: "shopify:orders:2026-06-30",
  fetchPage: async () => ({ records: [], nextCursor: null }),
  upsert: async () => 0,
};

const HOUR_MS = 60 * 60 * 1000;

describe("createSupabaseSyncStore.claimRun", () => {
  it("claims a job key that has never run", async () => {
    const { client, tables } = createFakeClient();
    const run = await createSupabaseSyncStore(client).claimRun(job);

    expect(run).not.toBeNull();
    expect(run?.jobKey).toBe(job.jobKey);
    expect(run?.attemptCount).toBe(1);
    expect(tables.sync_runs).toHaveLength(1);
  });

  it("refuses a job key that already succeeded, so a repeat sync is a no-op", async () => {
    const { client } = createFakeClient({
      sync_runs: [{ ...DEFAULTS.sync_runs, id: "run-1", job_key: job.jobKey, status: "succeeded" }],
    });

    expect(await createSupabaseSyncStore(client).claimRun(job)).toBeNull();
  });

  it("retries a failed run and increments the attempt count", async () => {
    const { client } = createFakeClient({
      sync_runs: [
        {
          ...DEFAULTS.sync_runs,
          id: "run-1",
          job_key: job.jobKey,
          status: "failed",
          attempt_count: 2,
          error_code: "HttpError",
          error_message: "429 from Shopify",
        },
      ],
    });

    const run = await createSupabaseSyncStore(client).claimRun(job);

    expect(run?.attemptCount).toBe(3);
    // The previous failure must not be carried into the new attempt's record.
    expect(run?.errorCode).toBeNull();
    expect(run?.errorMessage).toBeNull();
  });

  it("refuses a run that another worker is currently executing", async () => {
    const { client } = createFakeClient({
      sync_runs: [
        {
          ...DEFAULTS.sync_runs,
          id: "run-1",
          job_key: job.jobKey,
          status: "running",
          started_at: new Date(Date.now() - HOUR_MS).toISOString(),
          heartbeat_at: new Date().toISOString(),
        },
      ],
    });

    expect(await createSupabaseSyncStore(client).claimRun(job)).toBeNull();
  });

  it("does not steal a long backfill that is still reporting progress", async () => {
    // The point of heartbeating. Staleness is measured from the last page written, not from
    // when the job started, so a genuinely slow import keeps its job key however long it runs
    // — which is what allows the staleness window to be short enough to retry the same night.
    const { client } = createFakeClient({
      sync_runs: [
        {
          ...DEFAULTS.sync_runs,
          id: "run-1",
          job_key: job.jobKey,
          status: "running",
          started_at: new Date(Date.now() - 12 * HOUR_MS).toISOString(),
          heartbeat_at: new Date(Date.now() - 60_000).toISOString(),
        },
      ],
    });

    expect(await createSupabaseSyncStore(client).claimRun(job)).toBeNull();
  });

  it("reclaims a run whose worker stopped reporting progress", async () => {
    // Without this, a worker that dies mid-run leaves its row in `running` forever and that
    // job key could never be synced again. Twenty minutes is past the staleness window, so
    // this run is reclaimed the same night rather than the next.
    const { client } = createFakeClient({
      sync_runs: [
        {
          ...DEFAULTS.sync_runs,
          id: "run-1",
          job_key: job.jobKey,
          status: "running",
          attempt_count: 1,
          started_at: new Date(Date.now() - HOUR_MS).toISOString(),
          heartbeat_at: new Date(Date.now() - 20 * 60_000).toISOString(),
        },
      ],
    });

    const run = await createSupabaseSyncStore(client).claimRun(job);

    expect(run).not.toBeNull();
    expect(run?.attemptCount).toBe(2);
  });

  it("reclaims a run killed before it wrote its first page", async () => {
    // A run killed between `markRunning` and its first page has no heartbeat to judge it by.
    // Falling back to `started_at` is what stops that job key being held for ever, and it is
    // also how rows created before the heartbeat column existed are still reclaimable.
    const { client } = createFakeClient({
      sync_runs: [
        {
          ...DEFAULTS.sync_runs,
          id: "run-1",
          job_key: job.jobKey,
          status: "running",
          attempt_count: 1,
          started_at: new Date(Date.now() - HOUR_MS).toISOString(),
          heartbeat_at: null,
        },
      ],
    });

    expect(await createSupabaseSyncStore(client).claimRun(job)).not.toBeNull();
  });

  it("lets only one worker win when two race to reclaim the same failed run", async () => {
    const { client, tables } = createFakeClient({
      sync_runs: [
        { ...DEFAULTS.sync_runs, id: "run-1", job_key: job.jobKey, status: "failed", attempt_count: 1 },
      ],
    });
    const store = createSupabaseSyncStore(client);

    const [first, second] = await Promise.all([store.claimRun(job), store.claimRun(job)]);

    // The compare-and-set on attempt_count means the loser's update matches no rows.
    expect([first, second].filter((run) => run !== null)).toHaveLength(1);
    expect(tables.sync_runs[0].attempt_count).toBe(2);
  });
});

describe("createSupabaseSyncStore cursors and outcomes", () => {
  it("returns null for a resource that has never been synced", async () => {
    const { client } = createFakeClient();
    expect(await createSupabaseSyncStore(client).getCursor("connection-1", "orders")).toBeNull();
  });

  it("overwrites the cursor for a resource rather than appending a second row", async () => {
    const { client, tables } = createFakeClient();
    const store = createSupabaseSyncStore(client);

    await store.setCursor("connection-1", "orders", "cursor-a", "2026-06-29T00:00:00.000Z");
    await store.setCursor("connection-1", "orders", "cursor-b", "2026-06-30T00:00:00.000Z");

    expect(tables.sync_cursors).toHaveLength(1);
    expect(await store.getCursor("connection-1", "orders")).toBe("cursor-b");
  });

  it("keeps cursors for different resources on the same connection apart", async () => {
    const { client } = createFakeClient();
    const store = createSupabaseSyncStore(client);

    await store.setCursor("connection-1", "orders", "cursor-orders", null);
    await store.setCursor("connection-1", "refunds", "cursor-refunds", null);

    expect(await store.getCursor("connection-1", "orders")).toBe("cursor-orders");
    expect(await store.getCursor("connection-1", "refunds")).toBe("cursor-refunds");
  });

  it("records totals when a run completes", async () => {
    const { client, tables } = createFakeClient();
    const store = createSupabaseSyncStore(client);
    const run = await store.claimRun(job);

    await store.completeRun(run!.id, { received: 120, written: 118 });

    expect(tables.sync_runs[0]).toMatchObject({
      status: "succeeded",
      records_received: 120,
      records_written: 118,
    });
  });

  it("records the error when a run fails", async () => {
    const { client, tables } = createFakeClient();
    const store = createSupabaseSyncStore(client);
    const run = await store.claimRun(job);

    await store.failRun(run!.id, { code: "HttpError", message: "503 from Shopify" });

    expect(tables.sync_runs[0]).toMatchObject({
      status: "failed",
      error_code: "HttpError",
      error_message: "503 from Shopify",
    });
  });

  it("advances last_success_at only when the attempt succeeded", async () => {
    const { client, tables } = createFakeClient({
      integration_connections: [{ id: "connection-1", last_attempt_at: null, last_success_at: null }],
    });
    const store = createSupabaseSyncStore(client);

    await store.recordConnectionAttempt("connection-1", false);
    expect(tables.integration_connections[0].last_attempt_at).not.toBeNull();
    expect(tables.integration_connections[0].last_success_at).toBeNull();

    await store.recordConnectionAttempt("connection-1", true);
    expect(tables.integration_connections[0].last_success_at).not.toBeNull();
  });
});
