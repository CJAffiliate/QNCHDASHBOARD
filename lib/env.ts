import { z } from "zod";

const publicEnvironmentSchema = z.object({
  NEXT_PUBLIC_SUPABASE_URL: z.url(),
  NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY: z.string().min(20),
});

const serverEnvironmentSchema = publicEnvironmentSchema.extend({
  SUPABASE_SERVICE_ROLE_KEY: z.string().min(20),
  TOKEN_ENCRYPTION_KEY: z.string().min(32),
  CRON_SECRET: z.string().min(32),
  /** The tenant every server-side query is scoped to. Every table is keyed on it. */
  ORGANISATION_ID: z.uuid(),
  /**
   * Lets the pipeline call its own next step through Deployment Protection.
   *
   * Vercel exempts its own cron invocations from Deployment Protection, but not a request the
   * deployment makes to itself — so without this the second hop of a refresh is answered with
   * the authentication page instead of running. Vercel → Project → Settings → Deployment
   * Protection → Protection Bypass for Automation generates it. Optional: unset is correct
   * both locally and on a deployment with no protection enabled.
   */
  VERCEL_AUTOMATION_BYPASS_SECRET: z.string().min(1).optional(),
  /**
   * How long one invocation keeps taking steps before handing over to the next.
   *
   * Must sit comfortably under the platform's kill threshold, since a step that starts inside
   * the budget still has to finish. The default suits a 60-second limit; raise it where the
   * plan allows a longer `maxDuration`.
   */
  PIPELINE_STEP_BUDGET_MS: z.coerce.number().int().positive().default(45_000),
});

export function getPublicEnvironment() {
  return publicEnvironmentSchema.parse({
    NEXT_PUBLIC_SUPABASE_URL: process.env.NEXT_PUBLIC_SUPABASE_URL,
    NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY: process.env.NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY,
  });
}
/** Only use in server-side workers and protected route handlers. */
export function getServerEnvironment() {
  return serverEnvironmentSchema.parse({
    ...getPublicEnvironment(),
    SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY,
    TOKEN_ENCRYPTION_KEY: process.env.TOKEN_ENCRYPTION_KEY,
    CRON_SECRET: process.env.CRON_SECRET,
    ORGANISATION_ID: process.env.ORGANISATION_ID,
    VERCEL_AUTOMATION_BYPASS_SECRET: process.env.VERCEL_AUTOMATION_BYPASS_SECRET,
    PIPELINE_STEP_BUDGET_MS: process.env.PIPELINE_STEP_BUDGET_MS,
  });
}
