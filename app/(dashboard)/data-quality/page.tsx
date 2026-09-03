import { requireSession } from "@/lib/auth/current-user";
import { createReportingRepository } from "@/lib/reporting/reporting-repository";
import { loadFullHistory } from "@/lib/reporting/dashboard-data";
import {
  checkAdAccountTimezone,
  checkFinancialPolicyApproved,
  checkSyncFreshness,
  checkVariantCostCoverage,
  summariseDataQuality,
  type DataQualityResult,
  type SyncState,
  type IntegrationProvider,
} from "@/lib/monitoring/data-quality";
import { toBusinessDate } from "@/lib/financial/dates";
import { relativeTime } from "@/lib/reporting/format";
import { isRunnable, type PipelineRunStatus } from "@/lib/connectors/pipeline-store";
import type { PipelineStep } from "@/lib/connectors/pipeline";
import { StatusPill } from "../../components/metric";
import { RefreshButton } from "../../components/refresh-button";

export const dynamic = "force-dynamic";

const MAXIMUM_SYNC_AGE_HOURS = 36;

/**
 * A run still in progress is amber, not green: it has not yet proved it will finish. `partial`
 * is amber for the opposite reason — it did finish, but a provider did not deliver.
 */
const RUN_SEVERITY: Record<PipelineRunStatus, "green" | "amber" | "red"> = {
  running: "amber",
  succeeded: "green",
  partial: "amber",
  failed: "red",
};

/** A step left pending on a finished run was given up on, which is a failure, not a wait. */
const STEP_SEVERITY: Record<PipelineStep["status"], "green" | "amber" | "red"> = {
  pending: "amber",
  succeeded: "green",
  skipped: "green",
  failed: "red",
};
const ALL_PROVIDERS: IntegrationProvider[] = ["shopify", "meta", "tiktok", "xero"];

/**
 * Whether the numbers on the other pages can be trusted today.
 *
 * A provider that has never been connected is reported as failing, not omitted. Omitting it
 * would make a dashboard missing three of its four data sources look healthy, which is the
 * precise failure this page exists to prevent.
 */
export default async function DataQualityPage() {
  const session = await requireSession();
  const today = toBusinessDate(new Date(), session.businessTimezone);

  const repository = createReportingRepository(session.client, {
    organisationId: session.organisationId,
    businessTimezone: session.businessTimezone,
  });

  const [
    { data: connections },
    { data: settings },
    { data: published },
    { data: adAccounts },
    { data: runs },
    policy,
    context,
  ] = await Promise.all([
      session.client
        .from("integration_connections")
        .select("provider, status, last_success_at, last_attempt_at")
        .eq("organisation_id", session.organisationId),
      session.client
        .from("business_settings")
        .select("financial_policy_status")
        .eq("organisation_id", session.organisationId)
        .maybeSingle(),
      session.client
        .from("daily_financials")
        .select("business_date, calculation_version, calculated_at")
        .eq("organisation_id", session.organisationId)
        .eq("is_current", true)
        .order("business_date", { ascending: false })
        .limit(1),
      session.client
        .from("ad_accounts")
        .select("platform, external_id, timezone")
        .eq("organisation_id", session.organisationId),
      session.client
        .from("pipeline_runs")
        .select("run_key, trigger, status, steps, started_at, heartbeat_at, finished_at")
        .eq("organisation_id", session.organisationId)
        .order("started_at", { ascending: false })
        .limit(1),
      repository.loadPolicy(),
      repository.loadAllocationContext(),
    ]);

  const byProvider = new Map((connections ?? []).map((row) => [row.provider as string, row]));
  const syncStates: SyncState[] = ALL_PROVIDERS.map((provider) => {
    const connection = byProvider.get(provider);
    return {
      provider,
      lastSuccessAt: (connection?.last_success_at as string | null) ?? null,
      lastAttemptAt: (connection?.last_attempt_at as string | null) ?? null,
      lastStatus: connection?.status === "failed" ? "failed" : connection ? "succeeded" : null,
    };
  });

  const results: DataQualityResult[] = [
    checkFinancialPolicyApproved(
      (settings?.financial_policy_status as "draft" | "approved" | undefined) ?? "draft",
    ),
    ...checkSyncFreshness(syncStates, new Date(), MAXIMUM_SYNC_AGE_HOURS),
    checkAdAccountTimezone(
      (adAccounts ?? []).map((row) => ({
        platform: row.platform as string,
        externalId: row.external_id as string,
        timezone: (row.timezone as string | null) ?? null,
      })),
      session.businessTimezone,
    ),
  ];

  // Cost coverage can only be checked once the policy allows the engine to run at all.
  if (policy.status === "approved") {
    const history = await loadFullHistory({ session, policy: policy.policy, today });
    const soldVariantIds = history.allocated.flatMap((order) =>
      order.lines.map((line) => line.variantId).filter((id): id is string => id !== null),
    );
    results.push(checkVariantCostCoverage(soldVariantIds, context.variantCostProfiles, today));
  }

  const summary = summariseDataQuality(results);
  const latestPublished = published?.[0];
  const latestRun = runs?.[0];
  const runSteps = (latestRun?.steps as PipelineStep[] | undefined) ?? [];

  return (
    <>
      <p className="eyebrow">QNCH · AS AT {today}</p>
      <h1 className="title-sm">Data quality</h1>

      <RefreshButton />

      <div className={`banner${summary.severity === "red" ? " red" : ""}`}>
        <h3>
          {summary.isTrustworthy
            ? "Figures are current"
            : `${summary.failing.length} failing, ${summary.warning.length} warning`}
        </h3>
        <p className="muted">
          {summary.isTrustworthy
            ? "Every check passed. The figures on the other pages reflect current data."
            : "Figures elsewhere in the dashboard are built on the data below. Treat them accordingly."}
        </p>
      </div>

      <section className="panel">
        <h2>Checks</h2>
        <ul className="alerts">
          {summary.results.map((result) => (
            <li key={result.checkKey}>
              <StatusPill status={result.severity} />
              <span>
                <strong>{result.checkKey}</strong>
                <br />
                <span className="muted small">{result.message}</span>
              </span>
            </li>
          ))}
        </ul>
      </section>

      <section className="panel">
        <h2>Connections</h2>
        <table>
          <thead>
            <tr>
              <th>Provider</th>
              <th>Status</th>
              <th>Last success</th>
              <th>Last attempt</th>
            </tr>
          </thead>
          <tbody>
            {ALL_PROVIDERS.map((provider) => {
              const connection = byProvider.get(provider);
              return (
                <tr key={provider}>
                  <td style={{ textTransform: "capitalize" }}>{provider}</td>
                  <td>
                    {connection ? (
                      (connection.status as string)
                    ) : (
                      <span className="status-red">not connected</span>
                    )}
                  </td>
                  <td>{relativeTime((connection?.last_success_at as string | null) ?? null)}</td>
                  <td>{relativeTime((connection?.last_attempt_at as string | null) ?? null)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </section>

      <section className="panel">
        <h2>Last refresh</h2>
        <p className="muted small" style={{ marginTop: "-0.5rem", marginBottom: "1.25rem" }}>
          The refresh runs as a sequence of steps across several invocations, so a step killed
          part-way through is retried rather than costing the night. A step still marked pending
          on a finished run is one that was given up on after repeated attempts — its provider did
          not import, and everything after it still did.
        </p>
        {latestRun ? (
          <>
            <table>
              <tbody>
                <tr>
                  <td>Run</td>
                  <td>
                    {latestRun.run_key as string} ({latestRun.trigger as string})
                  </td>
                </tr>
                <tr>
                  <td>Status</td>
                  <td>
                    <StatusPill status={RUN_SEVERITY[latestRun.status as PipelineRunStatus]} />{" "}
                    {latestRun.status as string}
                  </td>
                </tr>
                <tr>
                  <td>Started</td>
                  <td>{relativeTime(latestRun.started_at as string)}</td>
                </tr>
                <tr>
                  <td>{latestRun.finished_at ? "Finished" : "Last progress"}</td>
                  <td>
                    {relativeTime(
                      ((latestRun.finished_at ?? latestRun.heartbeat_at) as string | null) ?? null,
                    )}
                  </td>
                </tr>
                <tr>
                  <td>Steps</td>
                  <td>
                    {runSteps.filter((step) => step.status === "succeeded").length} of {runSteps.length}{" "}
                    succeeded
                    {runSteps.some(isRunnable)
                      ? `, ${runSteps.filter(isRunnable).length} still to run`
                      : ""}
                  </td>
                </tr>
              </tbody>
            </table>
            <table style={{ marginTop: "1.25rem" }}>
              <thead>
                <tr>
                  <th>Step</th>
                  <th>Status</th>
                  <th>Attempts</th>
                  <th>Rows</th>
                  <th>Detail</th>
                </tr>
              </thead>
              <tbody>
                {runSteps.map((step) => (
                  <tr key={step.key}>
                    <td>
                      {step.provider} {step.resource}
                      {step.range ? ` ${step.range.from} to ${step.range.to}` : ""}
                    </td>
                    <td>
                      <StatusPill status={STEP_SEVERITY[step.status]} /> {step.status}
                    </td>
                    <td className="tabular">{step.attempts}</td>
                    <td className="tabular">{step.status === "succeeded" ? step.written : "—"}</td>
                    <td className="muted small">{step.error ?? ""}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        ) : (
          <p className="muted">No refresh has been recorded yet.</p>
        )}
      </section>

      <section className="panel">
        <h2>Published figures</h2>
        <p className="muted small" style={{ marginTop: "-0.5rem", marginBottom: "1.25rem" }}>
          The dashboard calculates on read, so it always reflects the costs approved now.
          <code>daily_financials</code> is the separate published record — what was reported at
          the time — used for restatement audit and the Sheets export. A stale publication here
          does not mean the dashboard is stale.
        </p>
        <table>
          <tbody>
            <tr>
              <td>Latest published date</td>
              <td>{latestPublished ? (latestPublished.business_date as string) : "never published"}</td>
            </tr>
            <tr>
              <td>Calculation version</td>
              <td>{latestPublished ? (latestPublished.calculation_version as string) : "—"}</td>
            </tr>
            <tr>
              <td>Calculated</td>
              <td>{relativeTime((latestPublished?.calculated_at as string | null) ?? null)}</td>
            </tr>
          </tbody>
        </table>
      </section>
    </>
  );
}
