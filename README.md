# QNCH Control Centre

Internal business intelligence and financial-control system for QNCH. Supabase/Postgres is the canonical historical data store; Google Sheets and the Next.js dashboard are presentation surfaces.

## What is built

| Area | State |
|---|---|
| Supabase schema, RLS, migrations | Complete |
| Financial engine (CM1/CM2/CM3, CAC, MER, break-even, cohorts, inventory, cash) | Complete, pure functions |
| Shopify connector | Complete, backfilled |
| Reporting layer — database to engine to published figures | Complete |
| Dashboard, authentication, nightly cron | Complete |
| Meta, TikTok, Xero connectors | Not built — credentials outstanding |
| Google Sheets export | Not built — service account outstanding |

The engine is pure: nothing in `lib/financial` reads the database or decides policy. Connectors
write source facts, `lib/reporting` shapes them into engine inputs, and the dashboard presents
the result. See `docs/financial-engine.md`.

## Local setup

1. Create `.env.local` with the variables below.
2. Run `npm install --legacy-peer-deps`. Plain `npm install` fails on the current peer tree.
3. Apply the database migrations — see below.
4. Seed the organisation: `node scripts/seed-organisation.mjs`, and put the printed
   `ORGANISATION_ID` into `.env.local`.
5. Connect Shopify and backfill — see below.
6. Record the approved financial policy: `npm run seed:policy`.
7. Grant yourself dashboard access: `npm run grant:access -- you@example.com`.
8. Run `npm test`, then `npm run dev`.

Steps 6 and 7 are not optional. Until the policy is approved the dashboard shows the
outstanding decisions instead of figures, and until access is granted every query returns
nothing — row-level security, not a bug.

## Environment variables

There is deliberately no committed `.env.example`. Every `.env*` file is ignored without
exception, so nothing in the repository can hold a value that looks like real configuration.
The list below is the template.

| Variable | Where it comes from |
|---|---|
| `NEXT_PUBLIC_SUPABASE_URL` | Supabase → Project Settings → API. Public; ships to the browser. |
| `NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY` | Same page. Public by design; RLS is the boundary, not this key. |
| `SUPABASE_SERVICE_ROLE_KEY` | Same page, `service_role`. **Bypasses RLS** — server-side only. |
| `SUPABASE_DB_URL` | Supabase → Connect → Session pooler. Used only by the scripts, never by the app. Percent-encode `@ : / ?` in the password. |
| `ORGANISATION_ID` | Printed by `node scripts/seed-organisation.mjs`. Every table is keyed on it. |
| `TOKEN_ENCRYPTION_KEY` | Generate 32 random bytes, base64. Encrypts provider tokens at rest — if lost, every stored token must be reconnected. |
| `CRON_SECRET` | Generate 32 random bytes. Bearer token for `/api/cron/daily`. |
| `VERCEL_AUTOMATION_BYPASS_SECRET` | Optional. Vercel → Deployment Protection → Protection Bypass for Automation. Only needed when protection is on, so the refresh can call its own next step. |
| `PIPELINE_STEP_BUDGET_MS` | Optional, default 45000. How long one invocation takes steps before handing over. Keep it under the plan's function limit. |
| `SHOPIFY_SHOP_DOMAIN` | The `myshopify.com` host, no scheme and no trailing slash. |
| `SHOPIFY_ADMIN_TOKEN` | Admin API access token, begins `shpat_`. Not the `shpss_` secret key, and shown only once. |

Meta, TikTok, Xero and Google Sheets credentials are added when those connectors are built.

`lib/env.ts` validates the server set at startup with zod, so a missing or malformed value
fails immediately rather than surfacing as an empty dashboard later.

## Database migrations

`SUPABASE_DB_URL` in `.env.local` holds the session-pooler connection string. It is used
only by the scripts below; the application talks to Supabase over PostgREST.

```
npm run migrate:status          # what is applied and what is pending
npm run migrate                 # apply everything pending, in filename order
node scripts/verify-schema.mjs  # inspect the database itself, independent of the above
node scripts/diagnose-db.mjs    # work out why a connection is being refused
```

Applied migrations are recorded in `public.schema_migrations`. Each migration commits
together with its bookkeeping row, so one can never be marked applied unless it was.

Migration files also carry their own `begin;`/`commit;`, so they remain safe to paste into
the Supabase SQL editor if the direct connection is unavailable. The runner strips that
wrapper and supplies its own, covering both the migration and its bookkeeping row.

`migrate:status` reports what the runner *recorded*. `verify-schema` inspects the database
itself and will disagree if the two ever drift — worth running after any manual change.

`node scripts/show-tenant.mjs` reports the organisation, its policy settings, its provider
connections and current row counts, and checks that `ORGANISATION_ID` resolves to a real row.

## Connecting Shopify

Create a custom app in the Shopify admin (Settings → Apps and sales channels → Develop apps),
grant it `read_orders`, `read_all_orders`, `read_products`, `read_inventory`, `read_locations`,
`read_customers` and `read_shopify_payments_payouts`, install it, then put the **Admin API
access token** — the value beginning `shpat_`, not the `shpss_` API secret key — into
`.env.local` along with the `myshopify.com` host.

```
npm run shopify:connect
```

It verifies the token against the Admin API, warns on any currency or timezone disagreement
with the organisation, stores the token encrypted, and reports whether orders older than 60
days are readable. `read_orders` alone only exposes the last 60 days: without `read_all_orders`
a historical backfill appears to succeed and silently returns nothing older.

Then backfill, starting with a dry run that writes nothing:

```
npm run probe:shopify -- --timeline      # scopes, catalogue, orders by month
npm run backfill:shopify -- --dry-run    # fetch one page, write nothing
npm run backfill:shopify -- --created-since 2026-01-01
```

The backfill syncs the product catalogue first, then orders. That order matters: order lines
resolve their variant against `product_variants`, so running orders into an empty catalogue
writes every line unattributed — and because the order job key would then be recorded as
succeeded, a corrective re-run would be skipped rather than fixing it.

Use `--created-since` to bound a backfill to a period of trading, and `--since` for an
incremental run. They filter different fields: `created_at` bounds a window, `updated_at`
catches orders edited later, such as one refunded weeks after it was placed.

Re-running a window that already succeeded is a no-op, which is what makes a retried cron
safe. Pass `--force` to re-read it anyway after a connector fix — orders upsert on their
Shopify id, so it restates rather than duplicates.

Every run checks that each order's gross, discounts, shipping and tax add back to the total
Shopify charged, and reports the orders that do not. That identity is what catches a money
field being read from the wrong place while each figure still looks plausible alone.
`npm run reconcile:shopify` runs the same check against everything already stored.

Scripts that import application code run through `tsx`. The library uses extensionless
imports and the `@/` alias — bundler-style resolution that Node's own ESM resolver does not
implement, and whose strip-only TypeScript mode also rejects parameter properties.

## Financial policy and costs

`npm run seed:policy` records the thirteen decisions of the register as approved, writes the
per-unit landed cost and the cost assumptions, and only then flips
`business_settings.financial_policy_status` to `approved` — all in one transaction, so the
status can never claim approval without the costs behind it.

Costs are dated from the first order in the data rather than from today, so the whole trading
history is costed on one approved basis instead of being restated against a cost nobody had
agreed at the time.

```
npm run seed:policy -- --dry-run   # print what would be written
npm run seed:policy                # apply
```

## Calculating

```
npm run calculate -- --dry-run                        # figures only, writes nothing
npm run calculate -- --from 2026-01-01 --to 2026-08-26
npm run calculate                                     # last 30 days, publishes
```

The dashboard **calculates on read**, so it always reflects the costs approved now — a
corrected COGS shows immediately rather than after the next job. `daily_financials` is a
separate, versioned record of what was *published* at the time, for restatement audit and the
Sheets export. The data-quality page shows both so a divergence is visible.

Publishing swaps a period atomically through `public.replace_daily_financials`. Superseded
rows are retained rather than deleted; exactly one row per date is ever current.

## Dashboard access

Authentication uses Supabase Auth. The tokens are held in httpOnly cookies and replayed as a
bearer token on every server-side query, so dashboard reads execute **as the signed-in user**
and RLS is the real access boundary rather than an application check that could be forgotten.
The service-role client is reserved for connector workers and the cron job.

Signing up grants nothing on its own — every read policy checks `organisation_members`:

```
npm run grant:access -- you@example.com          # owner
npm run grant:access -- someone@example.com viewer
npm run grant:access -- --list
```

## Deploying to Vercel

Supabase is already hosted, so only the Next.js application needs deploying.

1. Push this repository to GitHub, then **vercel.com → Add New → Project** and import it.
2. Set the build command override to `npm install --legacy-peer-deps && npm run build`. The
   default `npm install` fails on the current peer tree.
3. Add every variable from the table above as an **Environment Variable**, for Production.
   `SUPABASE_DB_URL` is not needed — it is only used by the local migration scripts.
4. If Deployment Protection is on, generate a Protection Bypass for Automation secret and add
   it as `VERCEL_AUTOMATION_BYPASS_SECRET`. Without it the refresh completes only its first
   step — see **Scheduled refresh** below.
5. Deploy. The cron in `vercel.json` runs `/api/cron/daily` at 03:00 UTC and Vercel supplies
   the `CRON_SECRET` bearer token automatically.

Nothing needs to change in Supabase: the deployment connects to the same project the local
environment does, so the data is the same data.

### Giving someone else access

Two steps, and both are required — the first alone grants nothing.

1. **Supabase → Authentication → Users → Add user**, with their email. Tick *Auto Confirm*,
   or they cannot sign in however correct their password is.
2. `npm run grant:access -- them@example.com viewer`

Roles: `owner` and `finance_admin` may edit costs, targets and policy; `operator` and `viewer`
read only. Membership is what every row-level security policy checks, so an account without it
signs in successfully and sees nothing.

Then `npm run set:password -- them@example.com`, or let them use the Supabase password-reset
email.

## Scheduled refresh

`POST` or `GET` `/api/cron/daily`, authenticated with `CRON_SECRET` as a bearer token. Vercel
Cron is configured in `vercel.json` for 03:00 daily and supplies that header automatically.

It **fetches from every connected provider and then recalculates** — Shopify catalogue and
orders, Meta hierarchy and insights, then the contribution walk. The same steps are behind the
**Refresh now** button on the data-quality page, so a manual refresh and the nightly one cannot
drift apart.

Providers run in sequence and one failing does not stop the others: Meta being down must not
prevent Shopify orders importing. A run where any provider failed reports `partial`, never
`ok`, and the per-provider outcome is shown rather than collapsed into a single tick.

### It runs as steps, across several invocations

A serverless function has a wall-clock budget, and exceeding it kills the process outright — no
catch block runs, nothing is recorded. So the refresh is not one call. It is an ordered list of
steps stored in `pipeline_runs`, and the route takes as many as fit in `PIPELINE_STEP_BUDGET_MS`
before asking itself to continue. Each hand-over is a new invocation with a new budget.

That matters because of what the old behaviour cost. A killed run left `sync_runs` holding a row
in `running` that nothing would complete, and the staleness window was six hours — longer than
the gap to the next nightly cron. **One timeout silently cost a full day of data.** Now a killed
step is retried within the same run, and a step that repeatedly fails to complete is given up on
after three attempts so the steps after it, including the publish, still run.

The endpoint returns `202` as soon as it has claimed the run, before the work starts. Vercel's
cron log will therefore always show success; the run's real outcome is in `pipeline_runs` and on
the **Last refresh** panel of the data-quality page.

Two things follow for deployment:

- Set `VERCEL_AUTOMATION_BYPASS_SECRET` if Deployment Protection is on. Vercel exempts its own
  cron invocations from protection but **not** a request the deployment makes to itself, so
  without it the second hop is answered with the authentication page and the run stops after one
  step. Vercel → Project → Settings → Deployment Protection → Protection Bypass for Automation.
- Keep `PIPELINE_STEP_BUDGET_MS` comfortably under the plan's function limit. A step that starts
  inside the budget still has to finish, so the default of 45s suits a 60s limit.

The publish is sliced into 15-day chunks, oldest first. If a run is cut short the unpublished
days are the most recent ones, which reads as a coverage gap rather than a hole in the middle of
the period.

It republishes a trailing 45-day window rather than only yesterday: a refund processed today
lands on today, but an order edited in Shopify changes a past day, and a restated cost changes
every day it applies to. Recomputing one day would leave those corrections unpublished.

Meta is re-fetched over a trailing week on every run, because it restates conversions for
several days as attribution settles. The upsert makes that converge rather than accumulate.

## Inventory settings

```
npm run seed:inventory -- --lead-time 28    # supplier lead time, all variants
npm run seed:inventory -- --list
```

Without a lead time no reorder alert can fire. A variant is flagged when its days of cover
fall to or below the lead time — the point at which ordering today still beats the stockout.
A fixed reorder point in units is optional and off by default, because the lead-time rule
adapts to how fast a SKU is actually selling and a unit threshold does not.

## Guardrails

- Never commit `.env`, service-account files, OAuth tokens, PII exports or financial credentials.
- Treat raw provider payloads as restricted; dashboard users should access derived, RLS-protected reporting views only.
- Re-runnable syncs must upsert by provider external ID and record a `sync_runs.job_key`.
- A production dashboard must show freshness/failed sync state and never coerce reconciliation differences to zero.
