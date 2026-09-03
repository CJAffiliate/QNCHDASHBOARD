import { describe, expect, it } from "vitest";
import {
  CURRENT_KEY_VERSION,
  decryptToken,
  encryptToken,
  payloadHash,
  readKeyVersion,
  resolveEncryptionKey,
  secretsMatch,
} from "../lib/connectors/crypto";
import { backoffDelayMs, DEFAULT_RETRY, fetchWithRetry, HttpError, retryAfterMs } from "../lib/connectors/http";
import { buildJobKey, runSync, type SyncJob, type SyncRunRecord, type SyncStore } from "../lib/connectors/sync-runner";

const KEY = Buffer.alloc(32, 7).toString("base64");

describe("token encryption", () => {
  it("round-trips a token", () => {
    const token = "shpat_example_refresh_token";
    expect(decryptToken(encryptToken(token, KEY), KEY)).toBe(token);
  });

  it("produces different ciphertext each time for the same token", () => {
    const a = encryptToken("same-token", KEY);
    const b = encryptToken("same-token", KEY);
    expect(a.equals(b)).toBe(false);
    expect(decryptToken(a, KEY)).toBe(decryptToken(b, KEY));
  });

  it("records the key version so rotation can decrypt old rows", () => {
    expect(readKeyVersion(encryptToken("token", KEY))).toBe(CURRENT_KEY_VERSION);
    expect(readKeyVersion(encryptToken("token", KEY, 2))).toBe(2);
  });

  it("fails loudly on the wrong key rather than returning rubbish", () => {
    const encrypted = encryptToken("token", KEY);
    expect(() => decryptToken(encrypted, Buffer.alloc(32, 9).toString("base64"))).toThrow();
  });

  it("detects tampering with the ciphertext", () => {
    const encrypted = encryptToken("token", KEY);
    encrypted[encrypted.length - 1] ^= 0xff;
    expect(() => decryptToken(encrypted, KEY)).toThrow();
  });

  it("rejects a truncated payload", () => {
    expect(() => decryptToken(Buffer.alloc(8), KEY)).toThrow(/truncated/);
  });

  it("refuses to encrypt an empty token", () => {
    expect(() => encryptToken("", KEY)).toThrow(/empty token/);
  });

  it("accepts base64, hex and raw keys but rejects the wrong length", () => {
    expect(resolveEncryptionKey(Buffer.alloc(32, 1).toString("base64"))).toHaveLength(32);
    expect(resolveEncryptionKey(Buffer.alloc(32, 1).toString("hex"))).toHaveLength(32);
    expect(resolveEncryptionKey("a".repeat(32))).toHaveLength(32);
    expect(() => resolveEncryptionKey("too-short")).toThrow(/32 bytes/);
  });
});

describe("payload hashing", () => {
  it("is stable regardless of key order", () => {
    expect(payloadHash({ a: 1, b: 2 })).toBe(payloadHash({ b: 2, a: 1 }));
  });

  it("changes when a value changes", () => {
    expect(payloadHash({ a: 1 })).not.toBe(payloadHash({ a: 2 }));
  });

  it("handles nested structures and arrays", () => {
    expect(payloadHash({ a: [{ x: 1, y: 2 }] })).toBe(payloadHash({ a: [{ y: 2, x: 1 }] }));
    expect(payloadHash({ a: [1, 2] })).not.toBe(payloadHash({ a: [2, 1] }));
  });

  it("distinguishes null from absent", () => {
    expect(payloadHash({ a: null })).not.toBe(payloadHash({}));
  });
});

describe("cron secret comparison", () => {
  it("matches only identical secrets", () => {
    expect(secretsMatch("abc123", "abc123")).toBe(true);
    expect(secretsMatch("abc123", "abc124")).toBe(false);
    expect(secretsMatch("abc", "abc123")).toBe(false);
  });
});

describe("http retry", () => {
  const options = { sleep: async () => {}, random: () => 1, maxAttempts: 3, baseDelayMs: 100, maxDelayMs: 5000 };

  it("returns a successful response without retrying", async () => {
    let calls = 0;
    const response = await fetchWithRetry(async () => {
      calls += 1;
      return new Response("ok", { status: 200 });
    }, options);
    expect(response.status).toBe(200);
    expect(calls).toBe(1);
  });

  it("retries a rate-limited response and succeeds", async () => {
    let calls = 0;
    const response = await fetchWithRetry(async () => {
      calls += 1;
      return calls < 3 ? new Response("slow down", { status: 429 }) : new Response("ok", { status: 200 });
    }, options);
    expect(response.status).toBe(200);
    expect(calls).toBe(3);
  });

  it("does not retry a client error", async () => {
    let calls = 0;
    const response = await fetchWithRetry(async () => {
      calls += 1;
      return new Response("bad request", { status: 400 });
    }, options);
    expect(response.status).toBe(400);
    expect(calls).toBe(1);
  });

  it("throws once retries are exhausted", async () => {
    await expect(fetchWithRetry(async () => new Response("boom", { status: 503 }), options)).rejects.toBeInstanceOf(
      HttpError,
    );
  });

  it("retries a network failure", async () => {
    let calls = 0;
    const response = await fetchWithRetry(async () => {
      calls += 1;
      if (calls < 2) throw new Error("ECONNRESET");
      return new Response("ok", { status: 200 });
    }, options);
    expect(response.status).toBe(200);
    expect(calls).toBe(2);
  });

  it("waits for the period the server asked for", () => {
    const now = Date.now();
    expect(retryAfterMs(new Response("", { headers: { "retry-after": "30" } }), now)).toBe(30_000);
    const httpDate = new Date(now + 60_000).toUTCString();
    expect(retryAfterMs(new Response("", { headers: { "retry-after": httpDate } }), now)).toBeGreaterThan(58_000);
    expect(retryAfterMs(new Response(""), now)).toBeNull();
  });

  it("backs off exponentially but stays under the cap", () => {
    const settings = { ...DEFAULT_RETRY, baseDelayMs: 500, maxDelayMs: 4000 };
    expect(backoffDelayMs(1, settings, () => 1)).toBe(500);
    expect(backoffDelayMs(2, settings, () => 1)).toBe(1000);
    expect(backoffDelayMs(9, settings, () => 1)).toBe(4000);
    expect(backoffDelayMs(3, settings, () => 0)).toBe(0);
  });
});

interface TestRecord {
  externalId: string;
}

function createStore(overrides: Partial<SyncStore> = {}) {
  const state = {
    runs: [] as SyncRunRecord[],
    cursors: new Map<string, { cursor: string | null; watermarkAt: string | null }>(),
    succeededKeys: new Set<string>(),
    connectionAttempts: [] as boolean[],
    heartbeats: [] as string[],
  };

  const store: SyncStore = {
    claimRun: async (job) => {
      if (state.succeededKeys.has(job.jobKey)) return null;
      const run: SyncRunRecord = {
        id: `run-${state.runs.length + 1}`,
        jobKey: job.jobKey,
        status: "queued",
        attemptCount: 1,
        recordsReceived: 0,
        recordsWritten: 0,
      };
      state.runs.push(run);
      return run;
    },
    markRunning: async (runId) => {
      const run = state.runs.find((candidate) => candidate.id === runId);
      if (run) run.status = "running";
    },
    heartbeat: async (runId) => {
      state.heartbeats.push(runId);
    },
    completeRun: async (runId, totals) => {
      const run = state.runs.find((candidate) => candidate.id === runId);
      if (!run) return;
      run.status = "succeeded";
      run.recordsReceived = totals.received;
      run.recordsWritten = totals.written;
      state.succeededKeys.add(run.jobKey);
    },
    failRun: async (runId, error) => {
      const run = state.runs.find((candidate) => candidate.id === runId);
      if (!run) return;
      run.status = "failed";
      run.errorCode = error.code;
      run.errorMessage = error.message;
    },
    getCursor: async (connectionId, resourceName) => state.cursors.get(`${connectionId}:${resourceName}`)?.cursor ?? null,
    setCursor: async (connectionId, resourceName, cursor, watermarkAt) => {
      state.cursors.set(`${connectionId}:${resourceName}`, { cursor, watermarkAt });
    },
    recordConnectionAttempt: async (_connectionId, succeeded) => {
      state.connectionAttempts.push(succeeded);
    },
    ...overrides,
  };

  return { store, state };
}

function pagedJob(pages: TestRecord[][], written: TestRecord[], overrides: Partial<SyncJob<TestRecord>> = {}): SyncJob<TestRecord> {
  return {
    provider: "shopify",
    resourceName: "orders",
    connectionId: "connection-1",
    jobKey: buildJobKey("shopify", "orders", "2026-06-30"),
    fetchPage: async (cursor) => {
      const index = cursor === null ? 0 : Number(cursor);
      return {
        records: pages[index] ?? [],
        nextCursor: index + 1 < pages.length ? String(index + 1) : null,
        watermarkAt: `2026-06-${String(index + 1).padStart(2, "0")}T00:00:00Z`,
      };
    },
    upsert: async (records) => {
      written.push(...records);
      return records.length;
    },
    ...overrides,
  };
}

describe("sync runner", () => {
  it("reads every page and reports the totals", async () => {
    const { store, state } = createStore();
    const written: TestRecord[] = [];
    const outcome = await runSync(pagedJob([[{ externalId: "1" }, { externalId: "2" }], [{ externalId: "3" }]], written), store);

    expect(outcome).toMatchObject({ status: "succeeded", pages: 2, received: 3, written: 3 });
    expect(written.map((record) => record.externalId)).toEqual(["1", "2", "3"]);
    expect(state.runs[0].status).toBe("succeeded");
  });

  it("skips a job that already succeeded, so a repeat run imports nothing twice", async () => {
    const { store } = createStore();
    const written: TestRecord[] = [];
    await runSync(pagedJob([[{ externalId: "1" }]], written), store);
    const second = await runSync(pagedJob([[{ externalId: "1" }]], written), store);

    expect(second).toEqual({ status: "skipped", reason: "already_succeeded", jobKey: "shopify:orders:2026-06-30" });
    expect(written).toHaveLength(1);
  });

  it("stores the cursor and watermark after each page", async () => {
    const { store, state } = createStore();
    await runSync(pagedJob([[{ externalId: "1" }], [{ externalId: "2" }]], []), store);
    expect(state.cursors.get("connection-1:orders")).toEqual({ cursor: null, watermarkAt: "2026-06-02T00:00:00Z" });
  });

  it("resumes from a stored cursor", async () => {
    const { store, state } = createStore();
    state.cursors.set("connection-1:orders", { cursor: "1", watermarkAt: null });
    const written: TestRecord[] = [];
    const outcome = await runSync(pagedJob([[{ externalId: "1" }], [{ externalId: "2" }]], written), store);

    expect(outcome).toMatchObject({ status: "succeeded", pages: 1 });
    expect(written.map((record) => record.externalId)).toEqual(["2"]);
  });

  it("records a failure without throwing, and keeps what was already written", async () => {
    const { store, state } = createStore();
    const written: TestRecord[] = [];
    const job = pagedJob([[{ externalId: "1" }], [{ externalId: "2" }]], written, {
      fetchPage: async (cursor) => {
        if (cursor === null) return { records: [{ externalId: "1" }], nextCursor: "1" };
        throw new Error("provider exploded");
      },
    });

    const outcome = await runSync(job, store);
    expect(outcome.status).toBe("failed");
    expect(outcome).toMatchObject({ received: 1, written: 1 });
    expect(state.runs[0].status).toBe("failed");
    expect(state.runs[0].errorMessage).toBe("provider exploded");
    expect(state.connectionAttempts).toEqual([false]);
  });

  it("allows a failed job to be retried, unlike a succeeded one", async () => {
    const { store } = createStore();
    const failing = pagedJob([], [], {
      fetchPage: async () => {
        throw new Error("transient");
      },
    });
    await runSync(failing, store);

    const written: TestRecord[] = [];
    const retry = await runSync(pagedJob([[{ externalId: "1" }]], written), store);
    expect(retry.status).toBe("succeeded");
    expect(written).toHaveLength(1);
  });

  it("stops a provider that never reports a final page", async () => {
    const { store } = createStore();
    const endless = pagedJob([], [], {
      maxPages: 3,
      fetchPage: async () => ({ records: [{ externalId: "x" }], nextCursor: "next" }),
    });

    const outcome = await runSync(endless, store);
    expect(outcome.status).toBe("failed");
    expect(outcome).toMatchObject({ pages: 3 });
  });

  it("handles an empty result set", async () => {
    const { store } = createStore();
    const outcome = await runSync(pagedJob([[]], []), store);
    expect(outcome).toMatchObject({ status: "succeeded", received: 0, written: 0 });
  });

  it("marks the connection healthy after a successful run", async () => {
    const { store, state } = createStore();
    await runSync(pagedJob([[{ externalId: "1" }]], []), store);
    expect(state.connectionAttempts).toEqual([true]);
  });
});
