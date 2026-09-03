/**
 * The full refresh, expressed as an ordered list of independently runnable steps.
 *
 * This is what "resync" means: fetch from every connected provider, then recalculate and
 * publish. One definition shared by the nightly cron route and the dashboard button, so a
 * manual refresh and an automatic one cannot drift apart.
 *
 * **Why steps rather than one function.** The refresh used to be a single call that ran
 * everything. On a serverless platform that call has a wall-clock budget, and exceeding it
 * kills the process outright — no catch block, no `finally`, no chance to record what
 * happened. The run was then stranded: `sync_runs` held a row in `running` that nothing would
 * complete, and because the staleness window was longer than the gap to the next nightly
 * cron, one timeout cost a full day of data.
 *
 * Splitting the work into steps fixes that at the root rather than by buying a longer budget.
 * Each step is small, separately recorded, and resumable, so a run that dies part-way through
 * continues from the step it reached instead of starting again or being abandoned. It also
 * means the pipeline can be driven across several invocations, each with its own fresh budget,
 * which is what `app/api/cron/daily` does.
 *
 * Ordering inside the list is load-bearing and is the only thing enforcing these constraints:
 *
 *  - Shopify variants before Shopify orders. Order lines resolve their variant against
 *    `product_variants`, so orders read into a stale catalogue write unattributed lines.
 *  - Meta entities before Meta insights. An insight row whose entity is not yet in
 *    `ad_entities` is skipped, and the breakdown loses it until the hierarchy catches up.
 *  - Meta account level before the finer levels. Account level is what the P&L reads and must
 *    land even if a finer level fails; the finer levels only feed the marketing breakdown.
 *  - Every provider before the publish steps, so the recalculation sees what arrived.
 *
 * A failing step does not stop the ones after it: Meta being down must not prevent Shopify
 * orders importing, and the recalculation still runs on whatever did arrive. Every outcome is
 * recorded, so a partial refresh is reported as partial rather than as success.
 */

import type { SupabaseClient } from "@supabase/supabase-js";
import { decryptToken } from "./crypto";
import { runSync, type SyncOutcome } from "./sync-runner";
import { createSupabaseSyncStore } from "./supabase-sync-store";
import { ShopifyClient } from "./shopify/client";
import { buildShopifyOrdersSyncJob, buildShopifyVariantsSyncJob } from "./shopify/sync";
import { MetaClient } from "./meta/client";
import { buildMetaHierarchySyncJob, buildMetaInsightsSyncJob, incrementalWindow } from "./meta/sync";
import { createShopifyRepository } from "@/lib/repositories/shopify-repository";
import { createMetaRepository } from "@/lib/repositories/meta-repository";
import { calculateAndPublish, type CalculationResult } from "@/lib/reporting/calculate";
import { addDays, toBusinessDate, type DateRange } from "@/lib/financial/dates";

/** Days republished on every run, to absorb late refunds and restated costs. */
export const RECALCULATION_WINDOW_DAYS = 45;

/**
 * Days republished per step.
 *
 * The publish is the step most likely to outgrow a time budget as history accumulates: it
 * loads every order in the window, allocates costs across them and writes the result. Slicing
 * it keeps each invocation bounded. Slicing is safe because `replace_daily_financials` only
 * stands down the dates it is given, so a chunk cannot blank out the days either side of it.
 */
export const PUBLISH_CHUNK_DAYS = 15;

export type PipelineStepStatus = "pending" | "succeeded" | "skipped" | "failed";

/**
 * One unit of work.
 *
 * Serialisable on purpose: the list is stored in `pipeline_runs.steps` so a later invocation
 * can pick up where this one stopped. Everything needed to run a step is either in the step
 * itself or re-derivable from the connection id — nothing depends on state held in the process
 * that planned it.
 */
export interface PipelineStep {
  /** Stable identity within a run. */
  key: string;
  provider: "shopify" | "meta" | "internal";
  resource: string;
  /** Null for steps that talk to no provider, i.e. the publish steps. */
  connectionId: string | null;
  /** Dates this step republishes. Null for sync steps. */
  range: DateRange | null;
  status: PipelineStepStatus;
  attempts: number;
  written: number;
  error: string | null;
  finishedAt: string | null;
}

export interface RefreshOptions {
  organisationId: string;
  businessTimezone: string;
  encryptionKey: string;
  /**
   * Distinguishes this run from another. Re-running the same value is a deliberate no-op,
   * which is what makes a retried cron safe. Defaults to the current minute so a manual
   * refresh is never silently skipped, while a repeated cron on the same day is.
   */
  jobDiscriminator?: string;
}

export interface RefreshResult {
  startedAt: string;
  finishedAt: string;
  today: string;
  syncs: { provider: string; resource: string; outcome: SyncOutcome }[];
  calculation: CalculationResult | null;
  /** True when every sync succeeded or was skipped as already done. */
  allSucceeded: boolean;
}

interface ProviderConnection {
  id: string;
  provider: string;
  externalAccountId: string;
  token: string;
}

/** PostgREST renders bytea as `\x<hex>`; pg would have given a Buffer directly. */
function toBuffer(value: string | Buffer): Buffer {
  if (Buffer.isBuffer(value)) return value;
  return Buffer.from(value.startsWith("\\x") ? value.slice(2) : value, "hex");
}

const CONNECTION_COLUMNS =
  "id, provider, external_account_id, status, integration_tokens(encrypted_refresh_token)";

function toConnection(row: Record<string, unknown>, encryptionKey: string): ProviderConnection[] {
  const tokens = row.integration_tokens as unknown as { encrypted_refresh_token: string }[] | null;
  const encrypted = Array.isArray(tokens) ? tokens[0]?.encrypted_refresh_token : undefined;
  // A connection with no stored token cannot be used. Skipped rather than throwing, so one
  // half-configured provider does not block the rest of the refresh.
  if (!encrypted) return [];

  return [
    {
      id: row.id as string,
      provider: row.provider as string,
      externalAccountId: row.external_account_id as string,
      token: decryptToken(toBuffer(encrypted), encryptionKey),
    },
  ];
}

async function loadConnections(
  client: SupabaseClient,
  organisationId: string,
  encryptionKey: string,
): Promise<ProviderConnection[]> {
  const { data, error } = await client
    .from("integration_connections")
    .select(CONNECTION_COLUMNS)
    .eq("organisation_id", organisationId)
    .eq("status", "active");
  if (error) throw error;

  return (data ?? []).flatMap((row) => toConnection(row, encryptionKey));
}

async function loadConnection(
  client: SupabaseClient,
  connectionId: string,
  encryptionKey: string,
): Promise<ProviderConnection> {
  const { data, error } = await client
    .from("integration_connections")
    .select(CONNECTION_COLUMNS)
    .eq("id", connectionId)
    .maybeSingle();
  if (error) throw error;
  if (!data) throw new Error(`Connection ${connectionId} no longer exists`);

  const [connection] = toConnection(data, encryptionKey);
  if (!connection) throw new Error(`Connection ${connectionId} has no stored token`);
  return connection;
}

function pendingStep(
  key: string,
  provider: PipelineStep["provider"],
  resource: string,
  connectionId: string | null,
  range: DateRange | null = null,
): PipelineStep {
  return {
    key,
    provider,
    resource,
    connectionId,
    range,
    status: "pending",
    attempts: 0,
    written: 0,
    error: null,
    finishedAt: null,
  };
}

/**
 * The dates each publish step covers, oldest first.
 *
 * Oldest first so that if the run is cut short, the days already published stay published and
 * the missing ones are the most recent — which the data-quality page reports as a coverage gap
 * rather than leaving a hole in the middle of the period.
 */
export function publishChunks(today: string, windowDays = RECALCULATION_WINDOW_DAYS): DateRange[] {
  const chunks: DateRange[] = [];
  const first = addDays(today, -(windowDays - 1));

  for (let offset = 0; offset < windowDays; offset += PUBLISH_CHUNK_DAYS) {
    const from = addDays(first, offset);
    const to = addDays(first, Math.min(offset + PUBLISH_CHUNK_DAYS, windowDays) - 1);
    chunks.push({ from, to });
  }

  return chunks;
}

/**
 * Meta insight levels, in the order they must run.
 *
 * Account level is what the P&L reads, so it lands first and must land even if a finer level
 * fails. The finer levels only feed the marketing breakdown. Summing across levels would
 * multiply spend, which is why the reporting layer reads account rows alone.
 */
const META_INSIGHT_LEVELS = ["account", "campaign", "adset", "ad"] as const;

/**
 * Works out what this refresh will do, before it does any of it.
 *
 * The result is stored with the run rather than recomputed on resume. Recomputing would let
 * the plan change halfway through — a connection added or deactivated between two hops would
 * silently alter the shape of a run already in progress, and a run that changes shape as it
 * goes cannot be reasoned about afterwards.
 */
export async function planPipeline(
  client: SupabaseClient,
  options: RefreshOptions,
): Promise<PipelineStep[]> {
  const today = toBusinessDate(new Date(), options.businessTimezone);
  const connections = await loadConnections(client, options.organisationId, options.encryptionKey);
  const steps: PipelineStep[] = [];

  for (const connection of connections) {
    if (connection.provider === "shopify") {
      // Catalogue before orders: an order line resolves its variant against `product_variants`.
      steps.push(pendingStep(`shopify:variants:${connection.id}`, "shopify", "variants", connection.id));
      steps.push(pendingStep(`shopify:orders:${connection.id}`, "shopify", "orders", connection.id));
    } else if (connection.provider === "meta") {
      // Hierarchy before insights, and account level before the finer levels.
      steps.push(pendingStep(`meta:entities:${connection.id}`, "meta", "entities", connection.id));
      for (const level of META_INSIGHT_LEVELS) {
        steps.push(
          pendingStep(`meta:insights:${level}:${connection.id}`, "meta", `insights:${level}`, connection.id),
        );
      }
    }
  }

  // Publishing last, so the recalculation sees everything that arrived.
  for (const range of publishChunks(today)) {
    steps.push(pendingStep(`publish:${range.from}`, "internal", "publish", null, range));
  }

  return steps;
}

export interface StepResult {
  step: PipelineStep;
  outcome: SyncOutcome | null;
  calculation: CalculationResult | null;
}

/**
 * Runs exactly one step and reports what happened.
 *
 * Never throws for a failure inside the step: a broken provider is an outcome to record and
 * carry on from, not an exception that abandons the rest of the refresh. The step comes back
 * marked `failed` with its error message, and the caller advances to the next one.
 */
export async function executeStep(
  client: SupabaseClient,
  step: PipelineStep,
  options: RefreshOptions,
): Promise<StepResult> {
  const attempted: PipelineStep = { ...step, attempts: step.attempts + 1 };

  try {
    if (step.provider === "internal") {
      const calculation = await runPublishStep(client, step, options);
      return {
        step: { ...attempted, status: "succeeded", finishedAt: new Date().toISOString() },
        outcome: null,
        calculation,
      };
    }

    const outcome = await runSyncStep(client, step, options);

    return {
      step: {
        ...attempted,
        status: outcome.status === "failed" ? "failed" : outcome.status === "skipped" ? "skipped" : "succeeded",
        written: outcome.status === "skipped" ? 0 : outcome.written,
        error: outcome.status === "failed" ? outcome.error.message : null,
        finishedAt: new Date().toISOString(),
      },
      outcome,
      calculation: null,
    };
  } catch (caught) {
    // A step that throws outside `runSync` — a bad token, a missing account row, a connection
    // deleted since the plan was made — is recorded as a failure so the refresh reports it
    // rather than losing it.
    const error = caught instanceof Error ? caught : new Error(String(caught));
    return {
      step: { ...attempted, status: "failed", error: error.message, finishedAt: new Date().toISOString() },
      outcome: {
        status: "failed",
        jobKey: step.key,
        pages: 0,
        received: 0,
        written: 0,
        error,
      },
      calculation: null,
    };
  }
}

async function runPublishStep(
  client: SupabaseClient,
  step: PipelineStep,
  options: RefreshOptions,
): Promise<CalculationResult> {
  if (!step.range) throw new Error(`Publish step ${step.key} has no range`);

  return calculateAndPublish(client, {
    organisationId: options.organisationId,
    businessTimezone: options.businessTimezone,
    range: step.range,
  });
}

async function runSyncStep(
  client: SupabaseClient,
  step: PipelineStep,
  options: RefreshOptions,
): Promise<SyncOutcome> {
  if (!step.connectionId) throw new Error(`Step ${step.key} has no connection`);

  const store = createSupabaseSyncStore(client);
  const connection = await loadConnection(client, step.connectionId, options.encryptionKey);
  const today = toBusinessDate(new Date(), options.businessTimezone);
  const discriminator = options.jobDiscriminator ?? new Date().toISOString().slice(0, 16);

  if (step.provider === "shopify") {
    const shopify = new ShopifyClient({
      shopDomain: connection.externalAccountId,
      accessToken: connection.token,
    });
    const repository = createShopifyRepository(client, { organisationId: options.organisationId });

    if (step.resource === "variants") {
      return runSync(
        buildShopifyVariantsSyncJob({
          client: shopify,
          repository,
          connectionId: connection.id,
          jobDiscriminator: discriminator,
        }),
        store,
      );
    }

    // Bounded by updatedAt, not createdAt, so an old order edited today — refunded, say — is
    // picked up. The stored cursor is what makes this incremental rather than a full re-read.
    const previousWatermark = await store.getCursor(connection.id, "orders");
    return runSync(
      buildShopifyOrdersSyncJob({
        client: shopify,
        repository,
        connectionId: connection.id,
        businessTimezone: options.businessTimezone,
        updatedSince: previousWatermark ? null : addDays(today, -30),
        jobDiscriminator: discriminator,
      }),
      store,
    );
  }

  const { data: account, error } = await client
    .from("ad_accounts")
    .select("id")
    .eq("organisation_id", options.organisationId)
    .eq("platform", "meta")
    .eq("external_id", connection.externalAccountId)
    .maybeSingle();
  if (error) throw error;
  if (!account) throw new Error(`No ad_accounts row for ${connection.externalAccountId}`);

  const meta = new MetaClient({ accessToken: connection.token });
  const repository = createMetaRepository(client, { organisationId: options.organisationId });

  if (step.resource === "entities") {
    return runSync(
      buildMetaHierarchySyncJob({
        client: meta,
        repository,
        connectionId: connection.id,
        accountExternalId: connection.externalAccountId,
        adAccountId: account.id as string,
        jobDiscriminator: discriminator,
      }),
      store,
    );
  }

  const level = step.resource.slice("insights:".length) as (typeof META_INSIGHT_LEVELS)[number];
  const window = incrementalWindow(today);

  return runSync(
    buildMetaInsightsSyncJob({
      client: meta,
      repository,
      connectionId: connection.id,
      accountExternalId: connection.externalAccountId,
      adAccountId: account.id as string,
      since: window.since,
      until: window.until,
      level,
      jobDiscriminator: `${level}_${discriminator}`,
    }),
    store,
  );
}

/**
 * Plans and runs the whole refresh in this process.
 *
 * Used where the caller can afford to wait for the result and wants it in one piece: the
 * operational scripts, and the dashboard's refresh button. The nightly cron drives the same
 * steps across several invocations instead — see `app/api/cron/daily`.
 */
export async function refreshEverything(
  client: SupabaseClient,
  options: RefreshOptions,
): Promise<RefreshResult> {
  const startedAt = new Date().toISOString();
  const today = toBusinessDate(new Date(), options.businessTimezone);

  const steps = await planPipeline(client, options);
  const syncs: RefreshResult["syncs"] = [];
  let calculation: CalculationResult | null = null;

  for (const step of steps) {
    const result = await executeStep(client, step, options);
    if (result.outcome) {
      syncs.push({ provider: step.provider, resource: step.resource, outcome: result.outcome });
    }
    // The last chunk published wins as the reported calculation. They share one policy load,
    // so a refusal to publish is the same refusal for every chunk.
    if (result.calculation) calculation = result.calculation;
  }

  return {
    startedAt,
    finishedAt: new Date().toISOString(),
    today,
    syncs,
    calculation,
    allSucceeded: syncs.every((sync) => sync.outcome.status !== "failed"),
  };
}
