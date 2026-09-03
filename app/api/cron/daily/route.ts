/**
 * The nightly pipeline: sync, recalculate, publish — driven across as many invocations as it
 * takes.
 *
 * Authenticated by a shared secret rather than a user session, because no user is present.
 * The comparison is constant-time so the endpoint cannot be used to recover the secret one
 * character at a time.
 *
 * **Why this hands over to itself.** A serverless function has a wall-clock budget, and
 * exceeding it kills the process outright: no catch block runs, no `finally`, nothing gets
 * recorded. A refresh that did everything in one invocation therefore had a hard ceiling, and
 * hitting it left `sync_runs` holding a row in `running` that nothing would ever complete —
 * costing a full day of data, because the next scheduled cron was further away than the
 * staleness window.
 *
 * So this route does not run the refresh. It advances it: it takes as many steps as fit in a
 * budget, then asks itself to continue, and each hop is a new invocation with a new budget.
 * That removes the ceiling instead of raising it, and it means a killed hop costs one step
 * rather than the night.
 *
 * The response is sent *before* the work starts, and the work runs in `after`. That ordering
 * is what makes the chain work: `after` runs within this invocation's budget, so if the work
 * happened before the response, the invocation that called us would sit waiting for all of it
 * and the hand-over would buy nothing.
 *
 * A consequence worth knowing: this endpoint returns 202 almost immediately and Vercel's cron
 * log will therefore always show success. The run's real outcome lives in `pipeline_runs` and
 * is what the data-quality page reports.
 *
 * Recalculation deliberately covers a trailing window rather than only yesterday. A refund
 * processed today lands on today's P&L, but an order edited in Shopify changes a past day, and
 * a cost restated in settings changes every day it applies to. Recomputing only the last day
 * would leave those corrections unpublished.
 */

import { timingSafeEqual } from "node:crypto";
import { after, NextResponse, type NextRequest } from "next/server";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";
import { getServerEnvironment } from "@/lib/env";
import { planPipeline, type PipelineStep, type RefreshOptions } from "@/lib/connectors/pipeline";
import { advanceRun } from "@/lib/connectors/pipeline-driver";
import { createPipelineStore, isRunnable } from "@/lib/connectors/pipeline-store";
import { toBusinessDate } from "@/lib/financial/dates";

export const dynamic = "force-dynamic";
export const maxDuration = 300;

/**
 * A ceiling on the hand-over chain.
 *
 * The run's own attempt counting already stops a bad step being retried for ever, so reaching
 * this means something is wrong in a way the step accounting did not catch. Stopping loudly
 * beats a deployment quietly calling itself until the month's invocation budget is gone.
 */
const MAX_HOPS = 40;

function isAuthorised(request: NextRequest, secret: string): boolean {
  const header = request.headers.get("authorization") ?? "";
  const presented = header.startsWith("Bearer ") ? header.slice(7) : header;

  const a = Buffer.from(presented);
  const b = Buffer.from(secret);
  // timingSafeEqual throws on a length mismatch, which would itself leak the length.
  return a.length === b.length && timingSafeEqual(a, b);
}

/**
 * Exposed as both verbs deliberately: Vercel Cron invokes scheduled paths with GET, while a
 * manual or external trigger is more naturally a POST. Both require the same secret.
 */
export const GET = handle;
export const POST = handle;

interface Continuation {
  runId?: string;
  hop?: number;
}

async function readContinuation(request: NextRequest): Promise<Continuation> {
  if (request.method !== "POST") return {};
  try {
    const body = (await request.json()) as Continuation;
    return { runId: body?.runId, hop: body?.hop };
  } catch {
    // A POST with no body is a legitimate manual trigger, not a malformed continuation.
    return {};
  }
}

async function handle(request: NextRequest) {
  const environment = getServerEnvironment();
  if (!isAuthorised(request, environment.CRON_SECRET)) {
    return NextResponse.json({ error: "unauthorised" }, { status: 401 });
  }

  const { runId, hop = 0 } = await readContinuation(request);

  if (hop >= MAX_HOPS) {
    return NextResponse.json(
      { status: "aborted", reason: "hop_limit_reached", hops: hop },
      { status: 500 },
    );
  }

  const client = createSupabaseAdminClient();
  const organisationId = environment.ORGANISATION_ID;

  const { data: organisation, error } = await client
    .from("organisations")
    .select("business_timezone")
    .eq("id", organisationId)
    .maybeSingle();
  if (error) throw error;
  if (!organisation) {
    return NextResponse.json({ error: "organisation not found" }, { status: 500 });
  }

  const businessTimezone = organisation.business_timezone as string;
  const today = toBusinessDate(new Date(), businessTimezone);
  const store = createPipelineStore(client, organisationId);

  const options: RefreshOptions = {
    organisationId,
    businessTimezone,
    encryptionKey: environment.TOKEN_ENCRYPTION_KEY,
    // Dated, so a retried cron on the same day resumes rather than re-importing.
    jobDiscriminator: today,
  };

  // Resolve the run before responding, so the caller is told what it started rather than
  // having to discover it. A continuation names its run; a fresh trigger resumes or creates
  // the day's.
  const run = runId
    ? await store.read(runId)
    : await store.resumeOrCreate(`cron:${today}`, "cron", () => planPipeline(client, options));

  if (!run) {
    return NextResponse.json({ error: `run ${runId} not found` }, { status: 404 });
  }

  // Only on a fresh trigger: a continuation is itself the proof that this run is alive, and
  // reaping on every hop would spend a database round trip per step for nothing.
  if (!runId) await store.reapAbandoned(run.id);

  const budget = environment.PIPELINE_STEP_BUDGET_MS;

  after(async () => {
    // A continuation can arrive for a run another hop already finished. Advancing it would
    // only rewrite a settled result, so it stops here.
    if (countRemaining(run.steps) === 0) return;

    const result = await advanceRun(client, run, { ...options, deadline: Date.now() + budget });
    if (!result.hasPending) return;

    await handOver(request, environment.CRON_SECRET, environment.VERCEL_AUTOMATION_BYPASS_SECRET, {
      runId: run.id,
      hop: hop + 1,
    });
  });

  return NextResponse.json(
    {
      status: "accepted",
      runId: run.id,
      runKey: run.runKey,
      hop,
      steps: run.steps.length,
      remaining: countRemaining(run.steps),
    },
    { status: 202 },
  );
}

/** Steps this run may still attempt. Ones given up on after `MAX_STEP_ATTEMPTS` do not count. */
function countRemaining(steps: readonly PipelineStep[]): number {
  return steps.filter(isRunnable).length;
}

/**
 * Asks this same deployment to carry on with the next steps.
 *
 * The origin comes from the incoming request rather than an environment variable, so this
 * works unchanged on a preview deployment, in production and locally.
 *
 * Deployment Protection is the trap here: Vercel exempts its own cron invocations from it, but
 * not a request the deployment makes to itself. Without the bypass header the second hop is
 * answered with the authentication page and the run stops silently after one step. The header
 * is omitted when the secret is unset, which is correct locally and where protection is off.
 */
async function handOver(
  request: NextRequest,
  cronSecret: string,
  bypassSecret: string | undefined,
  continuation: Continuation,
): Promise<void> {
  const headers: Record<string, string> = {
    "content-type": "application/json",
    authorization: `Bearer ${cronSecret}`,
  };
  if (bypassSecret) headers["x-vercel-protection-bypass"] = bypassSecret;

  const response = await fetch(new URL(request.nextUrl.pathname, request.nextUrl.origin), {
    method: "POST",
    headers,
    body: JSON.stringify(continuation),
  });

  if (!response.ok) {
    // Nothing here can recover it, but the run is resumable: it stays in `pipeline_runs` with
    // its heartbeat frozen, so the next trigger picks it up. Failing loudly puts the reason in
    // the platform log rather than leaving a run that simply stopped.
    throw new Error(`Hand-over to step ${continuation.hop} failed: ${response.status} ${response.statusText}`);
  }
}
