# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

**mb-batch** is the NestJS 11 background **worker** for **MB Auto** — an internal Meta Ads management platform for Thành Vinh Holding (domain `ads.3fastvn.com`). It has **no HTTP controllers**; it runs `@Cron` schedulers that enqueue Bull (Redis) jobs, processed against a shared PostgreSQL database via Prisma.

It is one of three services that share the same Postgres DB and Prisma schema:
- **mb-frontend** (Next.js 16 dashboard) — `../mb-frontend`
- **mb-ads** (NestJS 11 REST API) — `../mb-ads`
- **mb-batch** (this repo, the worker)

A fourth repo, **mb-database**, owns the canonical Prisma schema but is **not present on this machine**.

## Commands

```bash
yarn install            # install deps (this project uses yarn, not npm)
yarn start:dev          # run with watch (the normal dev loop)
yarn build              # nest build -> dist/src/main.js
yarn lint               # eslint --fix over src/test
yarn format             # prettier --write
yarn migration:generate # = npx prisma generate (regenerate Prisma client; does NOT run migrations)
```

- **Running the production build**: use `node dist/src/main.js` (this is what the Dockerfile does). The `start:prod` script (`node dist/main`) points at the wrong path and will fail — `nest build` emits to `dist/src/` because the root-level `prisma.config.ts` is included in compilation, so tsc preserves the `src/` prefix.
- **Tests**: `yarn test` (jest) is configured but there are currently **no `*.spec.ts` files** in the repo.
- **CI/CD** (`.github/workflows/ci-cd.yml`): on push/PR to `master`, CI does `yarn install --frozen-lockfile` + `yarn prisma generate` (against a mock `DATABASE_URL` — no build, no tests run). On push to `master` only, CD builds the Docker image, pushes to `ghcr.io/<repo>:latest` + `:<sha>`, then SSHes to the server and `docker compose pull mb-batch && docker compose up -d mb-batch`. A `concurrency` group cancels in-progress runs on the same branch so only the newest commit deploys.
- The worker listens on port **3030** (a health port only — there are no routes), hardcoded in `main.ts`. Three numbers float around and only **3030** is real: the `main.ts` startup log says "5000", the `Dockerfile` declares `EXPOSE 3000`, and `app.config.ts` reads a `PORT` env var — all three are ignored. Health-check / port-map **3030**.

## Hard rules (do not violate)

1. **Never run database migrations.** Do not run `npx prisma migrate ...`, `prisma db push`, or any `migration:run`. The user applies migrations manually against the real DB. You may only edit `schema.prisma` and run `npx prisma generate`.
2. **Keep `prisma/schema.prisma` in sync across all three backend repos** (`mb-ads`, `mb-batch`, `mb-database`) whenever you change it. Critical difference in the `generator client` block:
   - `mb-database/prisma/schema.prisma` **must** have `output = "../src/generated/prisma"`.
   - `mb-ads` and `mb-batch` schemas **must NOT** have any `output` line (they generate to the default `node_modules/.prisma/client`). *(mb-batch has a stale `src/generated/prisma/` dir left over from an earlier config — it is not the active client; leave the `output` line absent.)*
   - After syncing, run `npx prisma generate` in each repo that has `node_modules`.
3. **Update the project overview after meaningful changes** (new/changed cron, schema change, new/removed module, sync-logic change, queue config, dependency change). Edit `.gemini/skills/mb-auto-project-overview.md`, then copy it to the sibling repos at `../mb-ads/.gemini/skills/` and `../mb-frontend/.gemini/skills/`. (Note: `GEMINI.md` lists stale Linux `/home/thispc/...` paths — the real siblings live next to this repo.)
4. **Be a critical collaborator, not a yes-man** (per `GEMINI.md`): push back on flawed requests, surface hidden tech debt / perf / security trade-offs, and explain the reasoning before implementing.

## Architecture

### The core pattern (most sync modules)

`@Cron` **Scheduler** → enqueues a **Bull** job → **Processor** consumes it → **Service** does the real work. Seven **Bull-backed** modules use this in full — `meta-sync`, `insight-sync`, `media-sync`, `meta-media-sync`, `meta-media-upload`, `lark-sync`, `entity-sync` (plus the dormant `creative-refresh`) — each a self-contained module under `src/modules/<name>/` with these files:

- `*.module.ts` — imports `PrismaModule` + `BullModule.registerQueue(...)`, wires the trio
- `*.scheduler.ts` — `@Cron(expr, { timeZone: 'Asia/Ho_Chi_Minh' })` methods that `queue.add(JOB, {}, opts)`
- `*.processor.ts` — `@Processor(QUEUE)` class with `@Process({ name: JOB })` handlers that call the service
- `*.service.ts` — business logic (Meta/Drive/Lark calls + Prisma writes)
- `*.constants.ts` — exports the `*_QUEUE` name and a `*_JOBS` map

**Modules that deviate — don't assume the full pattern:**
- `help-ai` — `@Cron` scheduler calls the service **directly (no Bull, no processor)**; AI key-pool logic lives in `gemini-api-key-manager.service.ts`.
- `user-lark-sync` — **no Bull, no processor, no `*.constants.ts`**; a single `@Cron` (`EVERY_DAY_AT_3AM`, VN tz) calls the service directly, guarded by an in-process `running` boolean (overlap → skip). Syncs `User` ↔ Lark contacts via `lark-contact.service.ts`.
- `app-config` — **not a scheduler**. `AppConfigReader` is a lightweight **read-only** reader for product "knobs" in `SystemConfig` (precedence: DB override → env fallback → default, cached 30s). It is the batch-side **parity** of mb-ads' `AppConfigService` (no registry/admin API/UI here) — keys + defaults **must** match mb-ads `app-config.registry.ts`; change one side, check the other.
- `batch-run-log` — **not a scheduler module** (except a `0 3 * * *` cleanup cron in `batch-log-cleanup.scheduler.ts`, retention from `SystemConfig` key `run_log_retention_days`). `BatchRunLogger` wraps a job body and appends one JSON line per run to `<base>/batch-logs/runs-YYYY-MM-DD.jsonl` (VN date) on the `shared_files` volume, plus a `running/<id>.json` marker while in flight — **consumed by the mb-ads dashboard**, so keep the record shape (`batch-run-log.types.ts`) in sync with mb-ads' reader.
- `draft-automation` — **no processor, no `*.constants.ts`**; services are called directly (no Bull). Three scheduler files, but only **two carry `@Cron`**: `draft-automation-cron.scheduler.ts` (the `*/30` reconcile scan — the one cron that omits `timeZone`) and `draft-cleanup.scheduler.ts` (`0 2 * * *`, deletes unpublished draft `SystemCampaign`s untouched >7 days). The middle file `draft-automation.scheduler.ts` (`DraftAutomationScheduler`, ~1870 lines, `processAutomation`) has **no `@Cron`** — it is the asset-selection/draft-generation engine, invoked *by* the cron scheduler. Generation is a **dynamic per-template scheduler**: the cron scheduler registers one self-rescheduling `CronJob` per active `TemplateCampaign` via `SchedulerRegistry` (cron + tz come from `TemplateCampaign.data.automation`, ≥30-min interval enforced) — don't assume a fixed `@Cron` cadence. Publishing is in `draft-automation-meta-publisher.service.ts`; CID helpers in `src/common/utils/cid.util.ts`.
  - **Duplicate-publish guard (commit `badc101`)** — `publishDraftCampaign()` atomically claims the row with `updateMany({ where: { id, isPublishing: false, meta_id: null }, data: { isPublishing: true } })`; if `count === 0` it returns `{ skipped: true }` **before any Meta call**, so two overlapping runs (cron+cron or cron+manual) can't create duplicate budget-spending campaigns. The flag resets to `false` on success and on rollback. It is a **DB-row claim — not** an advisory lock or in-memory flag. Preserve it when touching publish logic.
- `meta-api` — a shared `@Global` client, not a scheduler module (see Globals below).

Conventions that matter:
- **Crons are pinned to `Asia/Ho_Chi_Minh`** — pass `{ timeZone: 'Asia/Ho_Chi_Minh' }` to `@Cron` (help-ai uses the `HELP_AI_TIME_ZONE` constant). Keep new ones in that timezone; one existing cron (`draft-automation-cron.scheduler.ts`) omits it — don't copy that.
- **Idempotent enqueue**: most jobs set a `jobId` so a duplicate enqueue within the same window is dropped, plus `removeOnComplete: true`, `attempts` (2–3), and exponential backoff. The `jobId` strategy **varies by job** — preserve whichever one a job already uses:
  - **hour bucket** — `${JOB}:${bucket}` with `bucket = new Date().toISOString().slice(0,13)` (most hourly syncs)
  - **day bucket** — `slice(0,10)` (e.g. insight-sync `SYNC_MISSING_DAILY`/`SYNC_AUDIENCE`, URL-expiry recalc jobs)
  - **singleton** — `${JOB}:singleton`, at most one in flight (media-sync, meta-media-upload)
  - **composite** — account id + levels + ranges **+ hour bucket** folded into the id (insight-sync `SYNC_ACCOUNT`; dedup window is hourly)
  - a few (e.g. lark-sync) set **no `jobId`** at all.
- Concurrency is controlled per-processor (`@Process({ concurrency })`) and via `p-limit` inside services — except `insight-sync`, which uses a hand-rolled `runWithLimit` helper rather than the `p-limit` library.
- **Startup enqueue**: several schedulers also enqueue/run a job once in `onModuleInit` (not only on the cron tick) — `insight-sync` (full sequential sync, guarded by a 5-min Redis lock `lock:insight-sync:startup-cooldown`), `media-sync`, `meta-media-sync`, `meta-media-upload`, `help-ai`, and `creative-refresh` if enabled. The production-gated ones (`insight-sync`, `media-sync`) run only when `NODE_ENV==='production'` **and** `DISABLE_STARTUP_SYNC!=='true'`. This is why jobs fire right after a deploy/restart — set `DISABLE_STARTUP_SYNC=true` to suppress.

### Globals — inject these anywhere

- `PrismaService` (`src/modules/prisma/`, `@Global`): the single Prisma client, built on the `@prisma/adapter-pg` driver adapter over `DATABASE_URL`.
- `MetaApiService` (`src/modules/meta-api/`, `@Global`): the gateway for **all** Meta Graph API (v24.0) traffic. Use its `request()`, `fetchAllPages()`, and `getAccountInsights()` rather than calling axios/SDK directly.

### Modules wired in `AppModule`

`batch-run-log` (JSONL run-logging infra + daily cleanup — see deviations above), `meta-api`, `insight-sync` (Today/3D/7D/Max/Audience performance + a `SYNC_LIFETIME_BACKFILL` job; note it also has one direct-call cron, `scheduleInactiveSlidingWindow` at `10 0 * * *`, that bypasses the queue — so even this "pure trio" module has a deviation), `meta-sync` (hourly campaign core), `entity-sync` (daily `0 1 * * *` sync of Meta entity metadata — Ad Account/Fanpage+IG+WhatsApp/Pixel/Custom Audience/Product Catalog — full Bull trio, runs before the insight jobs), `lark-sync` (Lark Bitable records + Google Drive permission audit, every 30m), `media-sync` (Drive folders → Meta), `draft-automation` (generate `System*` drafts from `TemplateCampaign`, publish to Meta, cleanup), `meta-media-sync` (daily image/video sync + error-video recovery), `meta-media-upload`, `help-ai` (chatbot knowledge snapshots + AI provider key-pool management + AI triage of user-submitted knowledge contributions; three direct `@Cron`s), `user-lark-sync` (daily `User` ↔ Lark contact sync, direct-call). `app-config` (`AppConfigModule` → `AppConfigReader`) is a read-only config helper — **not** in `AppModule`; it's imported by the modules that need it (`help-ai`, `batch-run-log`, `draft-automation`).

⚠️ **`src/modules/creative-refresh/` exists but is NOT imported in `AppModule`** — it is dormant and its crons do not run. Wire it into `AppModule` if you intend to enable it. If you revive it, route its Meta calls through `MetaApiService` — it currently hand-rolls `FacebookAdsApi.init` and would bypass the `META_AUTH_CONFIG` token and the cooldown gate.

ℹ️ **`src/modules/mail/` is dead scaffolding** — it holds only empty `templates/{auth,group}/` dirs and **zero `.ts` files**; it is not a module, is not imported, and nothing uses it (like the stale `src/generated/prisma/` dir). Ignore it.

### Meta API resilience (in `MetaApiService` + `src/common/utils`)

- **Auth**: the SDK is initialized with env `SDK_FACEBOOK_ACCESS_TOKEN`; per-request token + cookie come from the DB `SystemConfig` row keyed `META_AUTH_CONFIG` (cached 30s in-process).
- **Error handling**: auth errors (codes **190/102**) clear `META_AUTH_CONFIG` from the DB; rate-limit errors (codes **4, 17, 32, 368, 613, 80004**, or subcode `2446079`) write a `META_API_COOLDOWN` row in `SystemConfig`. `assertMetaApiAvailable()` enforces the cooldown (hard-block only when `META_API_COOLDOWN_HARD_BLOCK=true`).
- **Three different rate-limit code sets** (keep in sync if you add a code): the cooldown detector uses **4, 17, 32, 368, 613, 80004** + subcode `2446079`; the SDK retry path (`executeMetaApiWithRetry`/`isRetryableError`) uses **1, 2, 4, 17, 32, 613, 80004** (+ `is_transient` — omits 368, adds transient 1/2); `fetchAll` retries only **4, 17**.
- **Retry helpers** (always reuse these, don't hand-roll retries): `executeMetaApiWithRetry`, `executeDbWithRetry` (handles Postgres `57P03` startup), and `fetchAll` (cursor pagination with rate-limit backoff).
- ⚠️ **`getAccountInsights()` is the exception**: unlike `request()`/`fetchAllPages()`, it uses the **SDK** path (token from `SDK_FACEBOOK_ACCESS_TOKEN` via `FacebookAdsApi`), does **not** read `META_AUTH_CONFIG`, and does **not** call `assertMetaApiAvailable()`. So `insight-sync` is unaffected by the `META_API_COOLDOWN` row and by `META_AUTH_CONFIG` rotation.

### Data model

`prisma/schema.prisma` is large (~80 models). Rough groupings:
- **Meta mirror**: `Campaign`, `AdSet`, `Ad`, `Creative`, `AdImage`, `AdVideo` + their `*Insight` and `*AudienceInsight` tables. Plain insight tables use a composite-key upsert of (entity FK — `adId`/`campaignId`/`adSetId`/`creativeId`, abstracted as `entityId` — `dateStart`, `range`) with an ID-reuse/overwrite strategy to avoid churn; the `*AudienceInsight` tables key differently, on `age`/`gender`(/`level`) instead.
- **Draft system**: `TemplateCampaign` → `SystemCampaign`/`SystemAdSet`/`SystemAd`/`SystemCreative`, `DraftAutomationHistory`, `PublishHistory`.
- **Automation engine**: `AutomationRule`, `AutomationFilter*`, `AutomationTask*`, `AutomationRuleRun*`, `AutomationInsight*`.
- **Assets & sources**: `CreativeFolder`, `CreativeAsset`, `CreativeAssetMapping`, `LarkRecord`, `DriveFile`.
- **Platform**: `User`/`Project`/`Permission` (RBAC), `Help*` (chatbot + `HelpAiApiKey` provider pool), `SystemConfig` (runtime config incl. Meta auth/cooldown).

## Conventions

- TypeScript, **single quotes, trailing commas** (`.prettierrc`). ESLint extends `@typescript-eslint/recommended`; `no-explicit-any` is off and `strictNullChecks` is off — `any` and non-null assertions are common here.
- **User-facing strings, log messages, and cron descriptions are written in Vietnamese.** Match that.
- Standalone maintenance script: `src/scripts/recalculate-historical-metrics.ts` (backfills/recomputes historical insight metrics with its own pg `Pool`) — run with `ts-node`, not part of the app lifecycle.

## Environment

`.env` is gitignored. Required to boot: `DATABASE_URL`, `REDIS_HOST`/`REDIS_PORT`, `SDK_FACEBOOK_ACCESS_TOKEN`, `SDK_FACEBOOK_BUSINESS`. Integrations: `GOOGLE_SERVICE_ACCOUNT_JSON` + `GOOGLE_ALLOWED_SHARED_DRIVE_IDS` (Drive permission audit — see `README.md`), `LARK_APP_ID`/`LARK_APP_SECRET`, `GEMINI_API_KEYS`/`DEEPSEEK_API_KEYS` (Help AI). Behavior flags: `NODE_ENV=production` enables the boot-time startup syncs and `DISABLE_STARTUP_SYNC=true` suppresses them (see "Startup enqueue" above). Most sync modules also expose many optional tuning knobs (`INSIGHT_SYNC_*`, `META_MEDIA_UPLOAD_*`, `CREATIVE_*`, `META_API_*`) read directly via `process.env` in their constants/services.
