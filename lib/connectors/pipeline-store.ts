/**
 * Persistence for a refresh that outlives the invocation that started it.
 *
 * A serverless function can be killed at any moment without warning, so a run's progress has
 * to live in the database rather than in the process. This store is what makes a refresh
 * resumable: it records the planned steps, hands out one step at a time, and writes each
 * outcome back as it happens.
 *
 * Two hops can arrive at the same run together — a lost response retried, or a manual refresh
 * landing on top of the nightly one. Every write is therefore a compare-and-set on `revision`,
 * the same optimistic concurrency the sync store uses on `attempt_count`: the loser's update
 * matches no row and it re-reads instead of clobbering. That gives exactly one winner per step
 * without a database function or an advisory lock.
 */

import type { SupabaseClient } from "@supabase/supabase-js";
import type { PipelineStep } from "./pipeline";

export type PipelineRunStatus = "running" | "succeeded" | "partial" | "failed";
export type PipelineTrigger = "cron" | "manual";

export interface PipelineRun {
  id: string;
  runKey: string;
  trigger: PipelineTrigger;
  status: PipelineRunStatus;
  steps: PipelineStep[];
  revision: number;
  startedAt: string;
  heartbeatAt: string;
  finishedAt: string | null;
}

/**
 * How many times one step may be attempted before the run gives up on it.
 *
 * A step killed by the platform leaves no record of having started, so a resumed run cannot
 * distinguish "never ran" from "ran and died". Counting attempts is what stops a step that
 * always exceeds the budget from being retried for ever, which would leave the steps after it
 * — including the publish — permanently unreached.
 */
export const MAX_STEP_ATTEMPTS = 3;

/**
 * How long a run may go without progress before another invocation may take it over.
 *
 * Deliberately short. The heartbeat is refreshed after every completed step, so a run that is
 * genuinely working keeps moving its own deadline and cannot be stolen; only one that has
 * stopped becomes reclaimable. That is what allows a killed run to be retried the same night
 * rather than waiting for the next scheduled cron.
 */
export const STALE_RUN_MS = 15 * 60 * 1000;

const RUN_COLUMNS =
  "id, run_key, trigger, status, steps, revision, started_at, heartbeat_at, finished_at";

const UNIQUE_VIOLATION = "23505";

interface PipelineRunRow {
  id: string;
  run_key: string;
  trigger: PipelineTrigger;
  status: PipelineRunStatus;
  steps: PipelineStep[];
  revision: number;
  started_at: string;
  heartbeat_at: string;
  finished_at: string | null;
}

function toRun(row: PipelineRunRow): PipelineRun {
  return {
    id: row.id,
    runKey: row.run_key,
    trigger: row.trigger,
    status: row.status,
    steps: row.steps ?? [],
    revision: row.revision,
    startedAt: row.started_at,
    heartbeatAt: row.heartbeat_at,
    finishedAt: row.finished_at,
  };
}

/** A step still worth attempting: not finished, and not already given up on. */
export function isRunnable(step: PipelineStep): boolean {
  return step.status === "pending" && step.attempts < MAX_STEP_ATTEMPTS;
}

export function nextRunnableIndex(steps: readonly PipelineStep[]): number {
  return steps.findIndex(isRunnable);
}

/**
 * The status a finished run should carry.
 *
 * `partial` is not a softer failure: it means the run completed every step it could and at
 * least one provider did not deliver. The recalculation still ran on whatever arrived, so the
 * figures are real but incomplete — which is exactly what the data-quality page must say
 * rather than reporting a clean night.
 */
export function summariseStatus(steps: readonly PipelineStep[]): PipelineRunStatus {
  if (steps.length === 0) return "succeeded";
  if (steps.every((step) => step.status === "failed")) return "failed";
  return steps.some((step) => step.status === "failed") ? "partial" : "succeeded";
}

export function createPipelineStore(client: SupabaseClient, organisationId: string) {
  async function readByKey(runKey: string): Promise<PipelineRun | null> {
    const { data, error } = await client
      .from("pipeline_runs")
      .select(RUN_COLUMNS)
      .eq("organisation_id", organisationId)
      .eq("run_key", runKey)
      .maybeSingle<PipelineRunRow>();
    if (error) throw error;
    return data ? toRun(data) : null;
  }

  async function readById(runId: string): Promise<PipelineRun | null> {
    const { data, error } = await client
      .from("pipeline_runs")
      .select(RUN_COLUMNS)
      .eq("organisation_id", organisationId)
      .eq("id", runId)
      .maybeSingle<PipelineRunRow>();
    if (error) throw error;
    return data ? toRun(data) : null;
  }

  return {
    readByKey,
    read: readById,

    /**
     * Returns the run for this key, creating it from the plan only if it does not exist.
     *
     * Resuming rather than replacing is the point: a cron retried on the same day, or a hop
     * whose response was lost, must continue the run already in progress. The stored plan is
     * kept as it was — re-planning mid-run would let a connection added between hops change
     * the shape of a run already under way.
     */
    async resumeOrCreate(
      runKey: string,
      trigger: PipelineTrigger,
      plan: () => Promise<PipelineStep[]>,
    ): Promise<PipelineRun> {
      const existing = await readByKey(runKey);
      if (existing) return existing;

      // `started_at` and `heartbeat_at` are written explicitly rather than left to the column
      // defaults, so the timestamps a run is judged by come from one clock -- the application's
      // -- rather than from whichever of the app and the database happened to set each field.
      const now = new Date().toISOString();
      const insert = await client
        .from("pipeline_runs")
        .insert({
          organisation_id: organisationId,
          run_key: runKey,
          trigger,
          status: "running" satisfies PipelineRunStatus,
          steps: await plan(),
          started_at: now,
          heartbeat_at: now,
        })
        .select(RUN_COLUMNS)
        .single<PipelineRunRow>();

      if (!insert.error) return toRun(insert.data);

      // Another invocation created it between the read and the insert. Its plan is as good as
      // this one's, so adopt it rather than failing the hop.
      if (insert.error.code !== UNIQUE_VIOLATION) throw insert.error;

      const raced = await readByKey(runKey);
      if (!raced) throw insert.error;
      return raced;
    },

    /**
     * Reserves the next runnable step for this caller.
     *
     * The reservation is the attempt count, incremented before the work starts rather than
     * after it. A step killed mid-flight therefore comes back with its attempt already spent,
     * which is what stops an unrunnable step from being retried for ever.
     *
     * Returns null when nothing is left to do, or when another invocation won the race — the
     * caller re-reads and decides again either way.
     */
    async reserveNextStep(
      run: PipelineRun,
    ): Promise<{ run: PipelineRun; step: PipelineStep; index: number } | null> {
      const index = nextRunnableIndex(run.steps);
      if (index === -1) return null;

      const steps = run.steps.map((step, position) =>
        position === index ? { ...step, attempts: step.attempts + 1 } : step,
      );

      const { data, error } = await client
        .from("pipeline_runs")
        .update({ steps, revision: run.revision + 1, heartbeat_at: new Date().toISOString() })
        .eq("id", run.id)
        .eq("revision", run.revision)
        .select(RUN_COLUMNS)
        .maybeSingle<PipelineRunRow>();
      if (error) throw error;
      if (!data) return null;

      const claimed = toRun(data);
      return { run: claimed, step: claimed.steps[index], index };
    },

    /**
     * Writes one step's outcome and refreshes the heartbeat.
     *
     * Re-reads and retries on a lost compare-and-set. Losing here means another invocation
     * advanced the run while this step was working; the outcome is still real and must not be
     * dropped, so it is reapplied to the newer state rather than abandoned.
     */
    async recordStepResult(run: PipelineRun, index: number, step: PipelineStep): Promise<PipelineRun> {
      let current = run;

      for (let retry = 0; retry < 5; retry += 1) {
        const steps = current.steps.map((existing, position) => (position === index ? step : existing));

        const { data, error } = await client
          .from("pipeline_runs")
          .update({ steps, revision: current.revision + 1, heartbeat_at: new Date().toISOString() })
          .eq("id", current.id)
          .eq("revision", current.revision)
          .select(RUN_COLUMNS)
          .maybeSingle<PipelineRunRow>();
        if (error) throw error;
        if (data) return toRun(data);

        const reloaded = await readById(current.id);
        if (!reloaded) throw new Error(`Pipeline run ${current.id} vanished mid-run`);
        current = reloaded;
      }

      throw new Error(`Could not record step ${index} of run ${current.id} after 5 attempts`);
    },

    /** Marks the run finished, with the status its steps imply. */
    async finish(run: PipelineRun): Promise<PipelineRun> {
      const { data, error } = await client
        .from("pipeline_runs")
        .update({
          status: summariseStatus(run.steps),
          finished_at: new Date().toISOString(),
          heartbeat_at: new Date().toISOString(),
          revision: run.revision + 1,
        })
        .eq("id", run.id)
        .select(RUN_COLUMNS)
        .single<PipelineRunRow>();
      if (error) throw error;
      return toRun(data);
    },

    /**
     * Closes off runs that stopped without finishing.
     *
     * Only runs other than the one in hand, and only once they have gone quiet for longer than
     * the staleness window. A run abandoned on a previous night is recorded as what it was —
     * unfinished — rather than left reading as still in progress, which would make the
     * data-quality page report a stuck pipeline for ever.
     */
    async reapAbandoned(exceptRunId: string | null, now = new Date()): Promise<number> {
      const cutoff = new Date(now.getTime() - STALE_RUN_MS).toISOString();

      let query = client
        .from("pipeline_runs")
        .update({ status: "failed" satisfies PipelineRunStatus, finished_at: now.toISOString() })
        .eq("organisation_id", organisationId)
        .eq("status", "running")
        .lt("heartbeat_at", cutoff);

      if (exceptRunId) query = query.neq("id", exceptRunId);

      const { data, error } = await query.select("id");
      if (error) throw error;
      return (data ?? []).length;
    },
  };
}
