# Build roadmap

Written against the code at `0ccf4e3`. The system measures one channel accurately and
everything else partially; this is the order to change that. The stated priority is **make the
numbers true**, and the sequencing reflects it.

## Four findings that shape the plan

### 1. Chunking is not the alternative to Vercel Pro. It is required either way.

`refreshEverything` is one invocation that grows with history: Shopify catalogue, paginated
orders, Meta hierarchy, four levels of Meta insights across an eight-day restatement window,
then a 45-day republish. A 300-second budget raises the ceiling; it does not remove it.

Pro also does not fix the failure mode. A platform kill runs neither the `succeeded` nor the
`failed` branch in `sync-runner.ts`, so the `sync_runs` row is stranded in `running`.
`STALE_RUN_MS` is six hours — six hours after 03:00 is 09:00, and the cron does not fire again
until 03:00. **One timeout costs a full day of data**, with partial rows already written and
the data-quality page reporting stale rather than failed.

Build the chunked orchestrator first. The plan decision then concerns Deployment Protection
alone, which is a much smaller question.

### 2. Per-campaign reporting is mostly built. Nothing reads it.

`pipeline.ts` already syncs Meta insights at account, campaign, adset *and* ad level, and
`ad_entities` holds every campaign name and parent. Those rows are in the database now.

`loadAdSpend` (`lib/reporting/reporting-repository.ts:328`) filters `.is("entity_id", null)` —
account level only, deliberately, so summing levels cannot multiply spend. What is missing is a
second reader and a page, not a connector.

### 3. On Shopify Basic, order-to-campaign attribution is not in the API.

`Order.customerJourneySummary` — the field carrying UTM parameters and touchpoints — is gated
to Shopify Plus. Per-campaign CM2 measured from QNCH orders cannot be built from Shopify data
alone on this plan.

The workaround works on every plan: capture UTM parameters client-side into cart attributes,
which arrive on the order as `Order.customAttributes`. It needs a storefront theme change, and
it only works going forward — nothing retro-attributes an order already placed. **That is the
argument for shipping the snippet in week one even though nothing reads it for a month.**

### 4. Nothing writes the monitoring tables, so there is no history.

`data_quality_results`, `reconciliation_results` and `alerts` all exist in the schema. Nothing
writes to any of them: the data-quality page recomputes every check on request and discards the
answer. So "how long has Meta been stale" and "did the reconciliation drift last week" are
unanswerable. Persisting the checks costs about a day and is the precondition for alerting,
which is why it moves from last to third.

## Where the numbers stand

| Line | Status | Source, and what is wrong with it |
|---|---|---|
| Net revenue | Measured | Shopify orders, VAT-exclusive, discounts from `discountAllocations`. Pending the Phase 0 gap. |
| Product COGS | Measured | Effective-dated `variant_cost_profiles`; a missing profile warns rather than costing nothing. |
| **CM1** | **Measured** | Both inputs are real facts from one source. |
| Advertising spend | Partial | Meta only. TikTok is wired through the engine but never connected, so its spend is a silent zero. |
| **CM2** | **Partial** | Correct for the spend it knows about. Blended only. |
| Variable operating costs | Manual | Typed into `cost_assumptions`. Xero mapping rules exist; no Xero data has ever been imported. |
| **CM3** | **Manual** | As accurate as the last hand-updated assumption. |
| Fixed operating costs | Manual | Same, and nothing checks them against what left the bank. |
| **Operating profit** | **Manual** | Rests on two typed numbers. Directional, not reportable. |
| Cash balance | Not real | `operations-repository.ts:129` says so itself: a running total of imported movements, and there are none. Currently `null`. |
| Max CAC ceiling | Conservative | First-order CM3 only. If customers repeat, the real ceiling is higher. |

Three headline figures depend on numbers a person types. That is what Phase 4 ends, and why
Xero outranks the campaign breakdown despite the campaign breakdown being the more exciting
build.

## Phase 0 — Prove the revenue figure

A gate, not a phase. The unexplained £320.70 against £377.82 is an unfound bug, and every phase
after this inherits it. **1–2 days. Blocks everything.**

- Restore `.env.local`, re-run `npm run backfill:shopify` over the full trading history, then
  `npm run reconcile:shopify` until nothing is unmatched.
- Re-run the backfill over all history specifically to populate `is_test` and `cancelled_at`.
  Migration 0005 defaulted existing rows to not-excluded, so until then a test order still
  reads as revenue.
- Export Shopify's own Sales report for the exact window and business timezone; compare gross
  sales, discounts, returns, shipping and tax line by line.
- **First hypothesis for the gap:** the engine is VAT-exclusive throughout —
  `reporting-repository.ts:465` subtracts tax from refund totals — while Shopify's reports are
  usually VAT-inclusive. At 20% that alone produces a gap of roughly this shape. Rule it in or
  out before looking elsewhere.

Done when every pound of the difference has a named cause and `reconcile:shopify` runs clean
over the whole history.

## Phase 1 — Make the nightly run survive being killed

Break the pipeline into steps, each its own invocation with its own time budget. The
300-second question stops mattering, a killed step retries the same night, and the
data-quality page can name the stuck step. **3–4 days.**

- Migration `0008_pipeline_runs.sql`: a `pipeline_runs` table holding the ordered step list as
  `jsonb`, the current step, and `heartbeat_at`.
- Refactor `lib/connectors/pipeline.ts` from one loop into `planSteps()` and `runStep()`.
  Steps: Shopify variants, Shopify orders, Meta entities, Meta insights × 4 levels, publish.
- Chunk `calculateAndPublish` into 15-day slices, so the republish is bounded too — it is the
  step most likely to outgrow the budget as history accumulates.
- `/api/cron/daily` becomes: reap orphaned runs, claim or resume today's run, execute **one**
  step, schedule the next hop with `after()` from `next/server`, return 200 immediately.
- Drop `STALE_RUN_MS` from six hours to roughly 45 minutes and heartbeat during long steps, so
  a killed run is reclaimable the same night.
- Extend `tests/connector-framework.test.ts` with a fake that kills a step mid-run and asserts
  the next hop resumes from the stored cursor rather than restarting or skipping.

**Gotcha:** with Deployment Protection on, a self-invocation is blocked — Vercel's own cron
calls bypass protection automatically, ours will not. Set `VERCEL_AUTOMATION_BYPASS_SECRET` and
send it as `x-vercel-protection-bypass`. If `after()` proves unreliable, the fallback is
Supabase `pg_cron` + `pg_net` posting to a step endpoint every minute, which also sidesteps
Vercel's once-per-day cron limit on the free plan.

Done when a run whose middle step is killed completes on its own, that night, visibly.

## Phase 2 — Deploy

Small, and the difference between a script you run and a system that runs. **Half a day.**

- Import to Vercel with the build override `npm install --legacy-peer-deps && npm run build`.
- Six environment variables for Production, plus the bypass secret. `SUPABASE_DB_URL` is not
  needed; only the migration scripts use it.
- Turn on Deployment Protection. `app/robots.ts` already refuses crawlers, but robots.txt is a
  request, not access control — the login gate and Deployment Protection are the boundary.
- Verify by calling `/api/cron/daily` by hand with the bearer and watching it hop through every
  step to a published result.

## Phase 3 — Persist the checks, then alert on them

Moved up from last. The data-quality page is good and nobody will look at it; that is a reason
to make the system come to you, not to build more pages. Cheap, because the tables exist.
**2 days.**

- New `lib/monitoring/persist.ts` writing every check into `data_quality_results` and
  `reconciliation_results`, as the pipeline's final step.
- Raise **and resolve** rows in `alerts`, so the table shows current state rather than growing
  into a log nobody reads.
- Notify on the transition into red only, never on every run. An alert that fires nightly is an
  alert muted within a week.
- Slack incoming webhook as the channel: no domain verification, no cost, reaches a phone.
- Give the data-quality page its history: "Meta stale since ___", a 30-day strip per check, and
  which pipeline step is stuck and for how long.

Done when a broken sync reaches you without you opening the dashboard.

## Phase 4 — Xero, closing the cost side

The biggest gap and the one matching the stated priority. It turns cash, CM3 and net profit
from estimates into facts, and it makes "Xero is behind" a number on the dashboard.
**1.5–2 weeks.**

- **The first connector needing a real token refresh loop.** Xero access tokens last 30
  minutes; Shopify and Meta use long-lived tokens, so nothing here refreshes anything yet.
  Needs `/api/connect/xero/callback`, a `refreshAccessToken` in the client, and the
  `xero-tenant-id` header per request. `integration_tokens` and `lib/connectors/crypto.ts`
  already handle encrypted storage.
- Sync in order: chart of accounts, bank transactions, invoices (`ACCPAY`/`ACCREC`), balances.
- Migration `0009`: add `balance` and `balance_as_of` to `xero_accounts`, so `loadCash` reports
  a balance Xero states rather than one it accumulated.
- Map the chart of accounts into `expense_mapping_rules` — acquisition, variable operating,
  fixed operating, cash commitment, excluded — effective-dated, so restating a category does
  not rewrite history.
- **Because the books are behind:** add a coverage check reporting imported spend landing in
  accounts with no approved mapping rule, as an amount and a percentage. Unmapped spend must
  never silently vanish out of CM3. This gives a number to watch fall as the books are caught
  up, instead of a vague sense that they are out of date.
- Reconcile Shopify payouts (`shopify_payouts` already exists) against Xero bank receipts, and
  surface the difference rather than closing it.

Done when the cash page matches Xero's own dashboard to the penny, CM3 no longer depends on a
typed number, and the unmapped-spend figure is small and known.

## Phase 5 — Per-campaign economics, in two halves

Blended CAC of £44.94 against a £22.83 ceiling says acquisition is losing money but not where.
Half of this ships in a day; the other half must start collecting now to be useful later.
**2 days plus a theme edit.**

- **5a, immediately.** A `loadAdSpendByEntity()` reader for campaign-level rows joined to
  `ad_entities`, and a `/marketing/campaigns` page: spend, platform-claimed purchases,
  platform-claimed CAC, and a modelled CM2 from real contribution per new customer. Flag every
  campaign whose claimed CAC exceeds `maximumCac`. Label it as modelled from the platform's own
  attribution, in the tone `lib/financial/marketing.ts` already uses — directional, not
  measured.
- **5b, week one, pays off in a month.** A storefront snippet writing `utm_source`,
  `utm_medium`, `utm_campaign`, `utm_content` and `fbclid` into cart attributes.
- Set Meta's URL parameters to pass the **campaign ID**, not the name. Joining on
  `ad_entities.external_id` is reliable; string-matching a name breaks on the first rename.
- Then: extend `ORDERS_QUERY` with `customAttributes`, migration `0010` adds attribution
  columns to `shopify_orders`, and the reporting layer produces per-campaign CM2 measured from
  QNCH orders.
- Show the measured figure next to the platform's claim. The gap between them is itself a
  finding: it is how much of Meta's reported performance is real.

**Limit to state on the page:** cart-attribute capture is last-click and misses cleared
cookies, cross-device purchases, and arrivals without parameters. Materially better than
nothing, materially worse than Plus-level journey data.

## Phase 6 — TikTok

A repeat of a solved problem. The engine already accounts for it: `calculateMarketingPeriod`
loops both platforms and the `ad_accounts` constraint permits `tiktok`. Today its spend reads
as a silent zero. **4–5 days.**

- Mirror the Meta connector: client, queries, normalise, sync, repository, registration script,
  backfill script.
- Add it as pipeline steps — after Phase 1, that is appending to the step list.
- After Phase 5a, so the per-campaign view exists to receive it. Before then it only makes a
  blended number bigger.

## Phase 7 — An LTV-informed CAC ceiling

Today's ceiling is first-order CM3, deliberately conservative. If customers repeat, ads that
are actually fine may be getting cut. **3 days.**

- `buildCustomerCohorts` and `loadFullHistory` already collect what this needs. The work is a
  second max-CAC, not a new data source.
- Extend `calculateMarketingPeriod` with an optional expected repeat contribution derived from
  cohorts, over a chosen payback window.
- **Show both ceilings; never replace the first-order one.** One is measured and one is a
  forecast, and the engine's whole discipline is refusing to blur those.
- Register the payback window in `docs/financial-policy-decision-register.md`, like every other
  policy decision.

## Phase 8 — Presentation

Surfaces onto data that has to be right first. **3–4 days.**

- Export from `daily_financials`, not from a recomputation: it is the published, versioned
  record, so the sheet and the dashboard cannot disagree about a past period.
- Carry `calculation_version` into the sheet, so a restated period is visibly restated.
- The custom reports page already deferred.

## Suggested order

| When | What |
|---|---|
| Week 1 | Phase 0 in full, and ship the UTM snippet on day one though nothing reads it. Start Phase 1. |
| Week 2 | Phases 1 and 2. Live and self-healing. |
| Week 3 | Phase 3. It now tells you when it breaks. |
| Weeks 4–5 | Phase 4, Xero. Start the credentials early; app approval is not instant. |
| Week 6 | Phase 5a, by which point 5b has six weeks of attribution data waiting. |
| Then | Phases 6, 7, 8, re-prioritised against what the first six weeks taught. |

## Outstanding inputs

| Needed | For | Note |
|---|---|---|
| The date window and metric behind £320.70, plus Shopify's Sales export for it | Phase 0 | Blocks everything |
| Shopify theme access | Phase 5b | Every week it waits is attribution data that cannot be recovered |
| Xero app credentials — Web app, redirect `/api/connect/xero/callback`, scopes `offline_access accounting.transactions.read accounting.settings.read accounting.reports.read` | Phase 4 | The long pole; start early |
| Slack webhook URL or an email address | Phase 3 | Slack is simpler and reaches a phone |
| Vercel plan decision | Phase 2 | Phase 1 removes the timeout as a reason to upgrade; Deployment Protection remains Pro-only |
| Working through the Xero backlog | Phase 4 | Not an engineering task, but Phase 4's output is only as good as the coding behind it |
