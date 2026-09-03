/**
 * Tests for the behaviour the chunked pipeline exists to provide: surviving being killed.
 *
 * A serverless function can be terminated at any instant with no application code running
 * afterwards — no catch block, no `finally`. That is not something a test can ask the platform
 * to do on cue, so a killed step is simulated the only way it can be: by reserving a step and
 * then never recording its outcome, which is exactly the state a killed invocation leaves
 * behind. What matters is that the next invocation picks that step up rather than skipping it,
 * and that a step which can never complete is eventually given up on so the publish still runs.
 */

import { describe, expect, it } from "vitest";
import type { SupabaseClient } from "@supabase/supabase-js";
import { createFakeSupabase } from "./helpers/fake-supabase";
import { advanceRun } from "@/lib/connectors/pipeline-driver";
import {
  createPipelineStore,
  MAX_STEP_ATTEMPTS,
  nextRunnableIndex,
  summariseStatus,
} from "@/lib/connectors/pipeline-store";
import {
  planPipeline,
  PUBLISH_CHUNK_DAYS,
  publishChunks,
  RECALCULATION_WINDOW_DAYS,
} from "@/lib/connectors/pipeline";
import { encryptToken } from "@/lib/connectors/crypto";
import type { PipelineStep, StepResult } from "@/lib/connectors/pipeline";
import { addDays, enumerateDates } from "@/lib/financial/dates";

const ORGANISATION = "00000000-0000-0000-0000-000000000001";

const OPTIONS = {
  organisationId: ORGANISATION,
  businessTimezone: "Europe/London",
  encryptionKey: Buffer.alloc(32, 3).toString("base64"),
  jobDiscriminator: "2026-09-03",
};

function step(key: string, overrides: Partial<PipelineStep> = {}): PipelineStep {
  return {
    key,
    provider: "shopify",
    resource: key,
    connectionId: "connection-1",
    range: null,
    status: "pending",
    attempts: 0,
    written: 0,
    error: null,
    finishedAt: null,
    ...overrides,
  };
}

function succeed(target: PipelineStep): StepResult {
  return {
    step: { ...target, status: "succeeded", written: 1, finishedAt: "2026-09-03T03:00:00.000Z" },
    outcome: { status: "succeeded", jobKey: target.key, pages: 1, received: 1, written: 1 },
    calculation: null,
  };
}

function fail(target: PipelineStep, message: string): StepResult {
  return {
    step: { ...target, status: "failed", error: message, finishedAt: "2026-09-03T03:00:00.000Z" },
    outcome: {
      status: "failed",
      jobKey: target.key,
      pages: 0,
      received: 0,
      written: 0,
      error: new Error(message),
    },
    calculation: null,
  };
}

async function seedRun(steps: PipelineStep[]) {
  const { client, tables } = createFakeSupabase({});
  const supabase = client as unknown as SupabaseClient;
  const store = createPipelineStore(supabase, ORGANISATION);
  const run = await store.resumeOrCreate("cron:2026-09-03", "cron", async () => steps);
  return { supabase, store, run, tables };
}

/** A budget that never expires, for tests about what runs rather than about when it stops. */
const UNLIMITED = { deadline: Number.MAX_SAFE_INTEGER };

describe("publishChunks", () => {
  it("covers the recalculation window exactly, with no gap and no overlap", () => {
    const chunks = publishChunks("2026-09-03");
    const covered = chunks.flatMap((chunk) => enumerateDates(chunk.from, chunk.to));

    expect(covered).toHaveLength(RECALCULATION_WINDOW_DAYS);
    expect(new Set(covered).size).toBe(RECALCULATION_WINDOW_DAYS);
    expect(covered[0]).toBe(addDays("2026-09-03", -(RECALCULATION_WINDOW_DAYS - 1)));
    expect(covered.at(-1)).toBe("2026-09-03");
  });

  it("publishes the oldest days first", () => {
    // If the run is cut short, the days left unpublished must be the most recent ones. Those
    // read as a coverage gap on the data-quality page; a hole in the middle of the period
    // would not.
    const chunks = publishChunks("2026-09-03");
    const froms = chunks.map((chunk) => chunk.from);
    expect(froms).toStrictEqual([...froms].sort());
  });

  it("bounds every chunk to the configured size", () => {
    for (const chunk of publishChunks("2026-09-03")) {
      expect(enumerateDates(chunk.from, chunk.to).length).toBeLessThanOrEqual(PUBLISH_CHUNK_DAYS);
    }
  });

  it("handles a window that is not a whole number of chunks", () => {
    const chunks = publishChunks("2026-09-03", PUBLISH_CHUNK_DAYS + 1);
    expect(chunks).toHaveLength(2);
    expect(enumerateDates(chunks[1].from, chunks[1].to)).toStrictEqual([addDays("2026-09-03", 0)]);
  });
});

describe("planPipeline", () => {
  function connection(id: string, provider: string, externalAccountId: string) {
    return {
      id,
      organisation_id: ORGANISATION,
      provider,
      external_account_id: externalAccountId,
      status: "active",
      // Seeded the way PostgREST renders bytea -- a `\\x`-prefixed hex string, not a Buffer --
      // so the decode path under test is the one production actually takes.
      integration_tokens: [
        { encrypted_refresh_token: `\\x${encryptToken(`token-${id}`, OPTIONS.encryptionKey).toString("hex")}` },
      ],
    };
  }

  async function plan(rows: Record<string, unknown>[]) {
    const { client } = createFakeSupabase({ integration_connections: rows });
    return planPipeline(client as unknown as SupabaseClient, OPTIONS);
  }

  it("puts the Shopify catalogue before Shopify orders", async () => {
    // Order lines resolve their variant against `product_variants`. Orders read into a stale
    // catalogue write every line unattributed, which carries revenue at no cost.
    const steps = await plan([connection("c1", "shopify", "qnch.myshopify.com")]);
    const keys = steps.map((s) => s.resource);

    expect(keys.indexOf("variants")).toBeLessThan(keys.indexOf("orders"));
  });

  it("puts the Meta hierarchy before its insights, and account level before the finer levels", async () => {
    // An insight row whose entity is not yet in `ad_entities` is skipped. Account level is
    // what the P&L reads, so it must land even if a finer level fails.
    const steps = await plan([connection("c2", "meta", "act_123")]);
    const keys = steps.map((s) => s.resource);

    expect(keys.indexOf("entities")).toBeLessThan(keys.indexOf("insights:account"));
    for (const level of ["campaign", "adset", "ad"]) {
      expect(keys.indexOf("insights:account")).toBeLessThan(keys.indexOf(`insights:${level}`));
    }
  });

  it("publishes only after every provider has been read", async () => {
    const steps = await plan([
      connection("c1", "shopify", "qnch.myshopify.com"),
      connection("c2", "meta", "act_123"),
    ]);

    const firstPublish = steps.findIndex((s) => s.provider === "internal");
    expect(firstPublish).toBeGreaterThan(-1);
    expect(steps.slice(firstPublish).every((s) => s.provider === "internal")).toBe(true);
  });

  it("skips a connection with no stored token rather than failing the whole plan", async () => {
    // One half-configured provider must not stop the rest of the refresh.
    const steps = await plan([
      { ...connection("c1", "shopify", "qnch.myshopify.com"), integration_tokens: [] },
      connection("c2", "meta", "act_123"),
    ]);

    expect(steps.some((s) => s.provider === "shopify")).toBe(false);
    expect(steps.some((s) => s.provider === "meta")).toBe(true);
  });

  it("plans a publish even when nothing is connected", async () => {
    // A restated cost changes past days regardless of whether any provider is connected, so
    // the recalculation must still run.
    const steps = await plan([]);
    expect(steps.every((s) => s.provider === "internal")).toBe(true);
    expect(steps.length).toBeGreaterThan(0);
  });
});

describe("advanceRun", () => {
  it("runs every step and finishes the run when the budget allows", async () => {
    const { supabase, run } = await seedRun([step("a"), step("b"), step("c")]);

    const result = await advanceRun(supabase, run, { ...OPTIONS, ...UNLIMITED, execute: async (s) => succeed(s) });

    expect(result.completed.map((entry) => entry.resource)).toStrictEqual(["a", "b", "c"]);
    expect(result.hasPending).toBe(false);
    expect(result.run.status).toBe("succeeded");
    expect(result.run.finishedAt).not.toBeNull();
  });

  it("stops at the deadline and leaves the rest for the next invocation", async () => {
    const { supabase, run } = await seedRun([step("a"), step("b"), step("c")]);

    // Time advances one tick per check; the budget affords a single step.
    let clock = 0;
    const result = await advanceRun(supabase, run, {
      ...OPTIONS,
      deadline: 1,
      now: () => clock++,
      execute: async (s) => succeed(s),
    });

    expect(result.completed).toHaveLength(1);
    expect(result.hasPending).toBe(true);
    // Still running: a run left as finished here would report a night's work as complete when
    // two thirds of it had not happened.
    expect(result.run.status).toBe("running");
    expect(result.run.finishedAt).toBeNull();
  });

  it("resumes a step whose invocation was killed before it recorded an outcome", async () => {
    const { supabase, store, run } = await seedRun([step("a"), step("b"), step("c")]);

    // A budget that affords exactly one step, so the run is left part-finished.
    let clock = 0;
    const first = await advanceRun(supabase, run, {
      ...OPTIONS,
      deadline: 1,
      now: () => clock++,
      execute: async (s) => succeed(s),
    });
    expect(first.completed.map((entry) => entry.resource)).toStrictEqual(["a"]);

    // The kill: step "b" is reserved and then nothing more happens, exactly as when the
    // platform terminates the process mid-step.
    const abandoned = await store.reserveNextStep(first.run);
    expect(abandoned?.step.key).toBe("b");

    const resumed = await store.read(first.run.id);
    const second = await advanceRun(supabase, resumed!, {
      ...OPTIONS,
      ...UNLIMITED,
      execute: async (s) => succeed(s),
    });

    // "b" is retried rather than skipped, and the run completes.
    expect(second.completed.map((entry) => entry.resource)).toStrictEqual(["b", "c"]);
    expect(second.run.steps.map((s) => s.status)).toStrictEqual(["succeeded", "succeeded", "succeeded"]);
    // The abandoned attempt is still counted, which is what stops an unrunnable step looping.
    expect(second.run.steps[1].attempts).toBe(2);
    expect(second.run.status).toBe("succeeded");
  });

  it("gives up on a step that never completes, and still reaches the publish", async () => {
    // The failure this guards against is a step that always exceeds the budget. Retried for
    // ever it would block every step after it, including the recalculation — so the day's
    // figures would never be published because one provider was slow.
    const { supabase, store, run } = await seedRun([
      step("slow"),
      step("publish", { provider: "internal", resource: "publish", connectionId: null }),
    ]);

    let current = run;
    for (let attempt = 0; attempt < MAX_STEP_ATTEMPTS; attempt += 1) {
      const reserved = await store.reserveNextStep(current);
      expect(reserved?.step.key).toBe("slow");
      current = (await store.read(current.id))!;
    }

    const result = await advanceRun(supabase, current, {
      ...OPTIONS,
      ...UNLIMITED,
      execute: async (s) => succeed(s),
    });

    expect(result.completed.map((entry) => entry.resource)).toStrictEqual(["publish"]);
    expect(result.hasPending).toBe(false);
    expect(result.run.steps[0].status).toBe("pending");
    expect(result.run.steps[0].attempts).toBe(MAX_STEP_ATTEMPTS);
  });

  it("carries on past a failed step and reports the run as partial", async () => {
    // Meta being down must not stop Shopify orders importing, and the recalculation still runs
    // on whatever did arrive.
    const { supabase, run } = await seedRun([step("meta"), step("shopify"), step("publish")]);

    const result = await advanceRun(supabase, run, {
      ...OPTIONS,
      ...UNLIMITED,
      execute: async (s) => (s.key === "meta" ? fail(s, "429 from Meta") : succeed(s)),
    });

    expect(result.run.steps.map((s) => s.status)).toStrictEqual(["failed", "succeeded", "succeeded"]);
    expect(result.run.status).toBe("partial");
    expect(result.run.steps[0].error).toBe("429 from Meta");
  });

  it("does not retry a step that failed on its own terms", async () => {
    // A step that ran and reported a failure is finished. Only one killed without recording an
    // outcome is retried — otherwise a provider returning 401 would be hammered three times a
    // night for no reason.
    const { supabase, run } = await seedRun([step("meta")]);

    const result = await advanceRun(supabase, run, {
      ...OPTIONS,
      ...UNLIMITED,
      execute: async (s) => fail(s, "401 Unauthorized"),
    });

    expect(result.run.steps[0].attempts).toBe(1);
    expect(result.hasPending).toBe(false);
    expect(result.run.status).toBe("failed");
  });
});

describe("pipeline run store", () => {
  it("resumes the existing run rather than planning a second one for the same key", async () => {
    const { supabase, run } = await seedRun([step("a")]);
    const store = createPipelineStore(supabase, ORGANISATION);

    const again = await store.resumeOrCreate("cron:2026-09-03", "cron", async () => {
      throw new Error("must not re-plan a run that already exists");
    });

    expect(again.id).toBe(run.id);
  });

  it("lets only one invocation reserve a step when two arrive together", async () => {
    const { supabase, run } = await seedRun([step("a"), step("b")]);
    const store = createPipelineStore(supabase, ORGANISATION);

    const [first, second] = await Promise.all([store.reserveNextStep(run), store.reserveNextStep(run)]);

    // Both read the same revision, so exactly one compare-and-set can match.
    expect([first, second].filter(Boolean)).toHaveLength(1);
  });

  it("closes off a run abandoned on an earlier night, leaving the current one alone", async () => {
    const { supabase, run } = await seedRun([step("a")]);
    const store = createPipelineStore(supabase, ORGANISATION);

    const stale = await store.resumeOrCreate("cron:2026-09-02", "cron", async () => [step("a")]);

    const reaped = await store.reapAbandoned(run.id, new Date(Date.parse(stale.heartbeatAt) + 60 * 60 * 1000));

    expect(reaped).toBe(1);
    expect((await store.read(stale.id))?.status).toBe("failed");
    expect((await store.read(run.id))?.status).toBe("running");
  });

  it("reports a run with no failed steps as succeeded and any failure as partial", () => {
    expect(summariseStatus([step("a", { status: "succeeded" }), step("b", { status: "skipped" })])).toBe("succeeded");
    expect(summariseStatus([step("a", { status: "succeeded" }), step("b", { status: "failed" })])).toBe("partial");
    expect(summariseStatus([step("a", { status: "failed" })])).toBe("failed");
  });

  it("treats a step given up on as no longer runnable", () => {
    const exhausted = [step("a", { attempts: MAX_STEP_ATTEMPTS })];
    expect(nextRunnableIndex(exhausted)).toBe(-1);
    expect(nextRunnableIndex([step("a", { attempts: MAX_STEP_ATTEMPTS - 1 })])).toBe(0);
  });
});
