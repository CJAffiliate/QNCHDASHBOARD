-- Makes the nightly refresh survive being killed mid-run.
--
-- The refresh was one serverless invocation doing everything: Shopify catalogue, paginated
-- orders, Meta hierarchy, four levels of Meta insights, then a 45-day republish. When the
-- platform kills that invocation for exceeding its wall-clock budget, no application code
-- runs -- not the catch block, not a `finally`. So `sync_runs` was left holding a row in
-- `running` that nothing would ever complete, and `STALE_RUN_MS` did not release it for six
-- hours. Six hours after the 03:00 cron is 09:00, and the cron does not fire again until the
-- next 03:00, so a single timeout silently cost a whole day of data.
--
-- Two structures fix that. `pipeline_runs` records the refresh as an ordered list of steps
-- with the outcome of each, so a killed run is resumable from the step it reached rather
-- than restartable from the beginning. And `sync_runs.heartbeat_at` lets a job that is
-- genuinely alive say so while it works, which is what allows the staleness window to be
-- short enough to retry the same night without stealing a long backfill from a live worker.
--
-- The step list is stored rather than recomputed on resume. Recomputing it would silently
-- change the plan mid-run if a connection were added or deactivated between hops, and a run
-- that changes shape halfway through cannot be reasoned about afterwards.

begin;

create table public.pipeline_runs (
  id uuid primary key default gen_random_uuid(),
  organisation_id uuid not null references public.organisations(id) on delete cascade,
  -- 'cron' is the scheduled nightly run; 'manual' is the dashboard button.
  trigger text not null check (trigger in ('cron', 'manual')),
  -- Stable identity for this logical refresh, e.g. `cron:2026-09-03`. Unique per tenant, so
  -- a cron retried within the same day resumes the existing run instead of starting a
  -- second one alongside it.
  run_key text not null,
  status text not null default 'running'
    check (status in ('running', 'succeeded', 'partial', 'failed')),
  -- The ordered step list with each step's outcome. See lib/connectors/pipeline.ts.
  steps jsonb not null default '[]'::jsonb,
  -- Bumped on every write. A worker claiming the next step does a compare-and-set on this,
  -- so two hops arriving together cannot both run the same step -- the same optimistic
  -- concurrency the sync store already uses on `attempt_count`, and for the same reason:
  -- exactly one winner without a database function or an advisory lock.
  revision integer not null default 0,
  started_at timestamptz not null default now(),
  -- Refreshed as each step completes. A run whose heartbeat has stopped is resumable.
  heartbeat_at timestamptz not null default now(),
  finished_at timestamptz,
  unique (organisation_id, run_key)
);

create index pipeline_runs_org_started_idx
  on public.pipeline_runs (organisation_id, started_at desc);

-- Finding the run to resume: the reaper looks for unfinished runs by heartbeat age.
create index pipeline_runs_unfinished_idx
  on public.pipeline_runs (organisation_id, heartbeat_at)
  where status = 'running';

comment on table public.pipeline_runs is
  'One refresh as an ordered, resumable list of steps. A run killed mid-step resumes from the step it reached.';
comment on column public.pipeline_runs.revision is
  'Optimistic concurrency token. Claiming a step compares and sets it, so concurrent hops cannot double-run a step.';
comment on column public.pipeline_runs.status is
  'partial means the run finished with at least one failed step; the recalculation still ran on whatever did arrive.';

-- Lets a live job prove it is alive -------------------------------------------------------------
--
-- `claimRun` decided staleness from `started_at`, which cannot distinguish a job that has been
-- working for forty minutes from one that died forty minutes ago. That forced the staleness
-- window to be longer than the longest imaginable backfill. With a heartbeat the window can be
-- short, because a working job keeps moving its own deadline.

alter table public.sync_runs
  add column heartbeat_at timestamptz;

comment on column public.sync_runs.heartbeat_at is
  'Refreshed after each page is durably written. Staleness is measured from this, falling back to started_at.';

-- Access control -------------------------------------------------------------------------------
-- Same model as every other operational table: members read, connector workers use the service
-- role, which bypasses row-level security entirely.

alter table public.pipeline_runs enable row level security;

create policy member_read on public.pipeline_runs
  for select to authenticated
  using (public.is_organisation_member(organisation_id));

commit;
