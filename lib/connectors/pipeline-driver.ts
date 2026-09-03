/**
 * Drives a stored pipeline run forward for as long as this invocation can afford to.
 *
 * Separated from the route so the resumption behaviour can be tested without HTTP, a
 * serverless runtime or provider credentials — which matters, because the whole point of this
 * code is what happens when the process is killed, and that is not something an end-to-end
 * test can provoke on demand.
 *
 * The loop is bounded by a deadline rather than by a step count. Steps differ enormously in
 * cost — a Meta insights window is seconds, a Shopify order backfill is minutes — so taking a
 * fixed number of them would either waste most of an invocation or overrun it. Taking as many
 * as fit means a quiet night finishes in one invocation and a heavy one spreads across
 * several, without either being configured for.
 *
 * A step that starts inside the budget still has to finish, so the budget must sit
 * comfortably under the platform's kill threshold. If a single step is slower than the whole
 * budget it will be killed, retried, and after `MAX_STEP_ATTEMPTS` given up on — which is the
 * correct outcome: the run continues to its remaining steps, including the publish, rather
 * than being blocked for ever by one bad provider.
 */

import type { SupabaseClient } from "@supabase/supabase-js";
import { executeStep, type PipelineStep, type RefreshOptions, type StepResult } from "./pipeline";
import { createPipelineStore, nextRunnableIndex, type PipelineRun } from "./pipeline-store";
import type { SyncOutcome } from "./sync-runner";
import type { CalculationResult } from "@/lib/reporting/calculate";

export interface AdvanceResult {
  run: PipelineRun;
  /** Steps completed by this invocation, not by the run as a whole. */
  completed: { provider: string; resource: string; outcome: SyncOutcome | null }[];
  /** The last publish outcome seen, for reporting a refusal to publish. */
  calculation: CalculationResult | null;
  /** True when work remains and another invocation is needed. */
  hasPending: boolean;
}

export interface AdvanceOptions extends RefreshOptions {
  /** Wall-clock instant after which no further step is started. */
  deadline: number;
  now?: () => number;
  /**
   * How a step is run. Defaults to the real one.
   *
   * The seam exists because this module's whole purpose is what happens when a step does not
   * finish, and a test cannot ask the platform to kill a process on cue. Substituting the
   * executor is the only way to exercise the resumption path deliberately rather than hoping
   * to observe it in production.
   */
  execute?: (step: PipelineStep) => Promise<StepResult>;
}

export async function advanceRun(
  client: SupabaseClient,
  run: PipelineRun,
  options: AdvanceOptions,
): Promise<AdvanceResult> {
  const store = createPipelineStore(client, options.organisationId);
  const now = options.now ?? Date.now;
  const execute = options.execute ?? ((step: PipelineStep) => executeStep(client, step, options));

  let current = run;
  const completed: AdvanceResult["completed"] = [];
  let calculation: CalculationResult | null = null;

  while (now() < options.deadline) {
    const reserved = await store.reserveNextStep(current);

    if (!reserved) {
      // Either nothing is left, or another invocation took the step between the read and the
      // reservation. Re-reading distinguishes the two without guessing.
      const reloaded = await store.read(current.id);
      if (!reloaded) throw new Error(`Pipeline run ${current.id} vanished mid-run`);
      current = reloaded;
      if (nextRunnableIndex(current.steps) === -1) break;
      continue;
    }

    current = reserved.run;

    const result = await execute(reserved.step);
    current = await store.recordStepResult(current, reserved.index, result.step);

    completed.push({
      provider: reserved.step.provider,
      resource: reserved.step.resource,
      outcome: result.outcome,
    });
    if (result.calculation) calculation = result.calculation;
  }

  const hasPending = nextRunnableIndex(current.steps) !== -1;

  // Finishing is the driver's job, not the caller's: a run left in `running` after its last
  // step reads as a stuck pipeline for ever, which is the failure this whole design exists to
  // stop being invisible.
  if (!hasPending) current = await store.finish(current);

  return { run: current, completed, calculation, hasPending };
}
