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
- The worker listens on port **3030** (a health port only — there are no routes). The `main.ts` startup log says "5000"; ignore it, the actual port is 3030.

## Hard rules (do not violate)

1. **Never run database migrations.** Do not run `npx prisma migrate ...`, `prisma db push`, or any `migration:run`. The user applies migrations manually against the real DB. You may only edit `schema.prisma` and run `npx prisma generate`.
2. **Keep `prisma/schema.prisma` in sync across all three backend repos** (`mb-ads`, `mb-batch`, `mb-database`) whenever you change it. Critical difference in the `generator client` block:
   - `mb-database/prisma/schema.prisma` **must** have `output = "../src/generated/prisma"`.
   - `mb-ads` and `mb-batch` schemas **must NOT** have any `output` line (they generate to the default `node_modules/.prisma/client`).
   - After syncing, run `npx prisma generate` in each repo that has `node_modules`.
3. **Update the project overview after meaningful changes** (new/changed cron, schema change, new/removed module, sync-logic change, queue config, dependency change). Edit `.gemini/skills/mb-auto-project-overview.md`, then copy it to the sibling repos at `../mb-ads/.gemini/skills/` and `../mb-frontend/.gemini/skills/`. (Note: `GEMINI.md` lists stale Linux `/home/thispc/...` paths — the real siblings live next to this repo.)
4. **Be a critical collaborator, not a yes-man** (per `GEMINI.md`): push back on flawed requests, surface hidden tech debt / perf / security trade-offs, and explain the reasoning before implementing.

## Architecture

### The one pattern every sync module follows

`@Cron` **Scheduler** → enqueues a **Bull** job → **Processor** consumes it → **Service** does the real work. Each feature is a self-contained module under `src/modules/<name>/` with these files:

- `*.module.ts` — imports `PrismaModule` + `BullModule.registerQueue(...)`, wires the trio
- `*.scheduler.ts` — `@Cron(expr, { timeZone: 'Asia/Ho_Chi_Minh' })` methods that `queue.add(JOB, {}, opts)`
- `*.processor.ts` — `@Processor(QUEUE)` class with `@Process({ name: JOB })` handlers that call the service
- `*.service.ts` — business logic (Meta/Drive/Lark calls + Prisma writes)
- `*.constants.ts` — exports the `*_QUEUE` name and a `*_JOBS` map

Conventions that matter:
- **All crons are pinned to `Asia/Ho_Chi_Minh`.** Keep new ones in that timezone.
- **Idempotent enqueue**: jobs are added with `jobId: \`${JOB}:${hourBucket}\`` (hour bucket from `new Date().toISOString().slice(0,13)`), plus `removeOnComplete: true`, `attempts: 3`, and exponential backoff. This dedupes a job within its time window — preserve it.
- Concurrency is controlled per-processor (`@Process({ concurrency })`) and via `p-limit` inside services.

### Globals — inject these anywhere

- `PrismaService` (`src/modules/prisma/`, `@Global`): the single Prisma client, built on the `@prisma/adapter-pg` driver adapter over `DATABASE_URL`.
- `MetaApiService` (`src/modules/meta-api/`, `@Global`): the gateway for **all** Meta Graph API (v24.0) traffic. Use its `request()`, `fetchAllPages()`, and `getAccountInsights()` rather than calling axios/SDK directly.

### Modules wired in `AppModule`

`meta-sync` (hourly campaign core), `insight-sync` (Today/3D/7D/Max/Audience performance), `media-sync` (Drive folders → Meta), `meta-media-sync` (daily image/video sync + error-video recovery), `meta-media-upload`, `lark-sync` (Lark Bitable records + Google Drive permission audit, every 30m), `draft-automation` (generate `System*` drafts from `TemplateCampaign`, publish to Meta, cleanup), `help-ai` (chatbot knowledge snapshots + AI provider key-pool management), `meta-api`.

⚠️ **`src/modules/creative-refresh/` exists but is NOT imported in `AppModule`** — it is dormant and its crons do not run. Wire it into `AppModule` if you intend to enable it.

### Meta API resilience (in `MetaApiService` + `src/common/utils`)

- **Auth**: the SDK is initialized with env `SDK_FACEBOOK_ACCESS_TOKEN`; per-request token + cookie come from the DB `SystemConfig` row keyed `META_AUTH_CONFIG` (cached 30s in-process).
- **Error handling**: auth errors (codes **190/102**) clear `META_AUTH_CONFIG` from the DB; rate-limit errors (codes **4, 17, 32, 368, 613, 80004**, or subcode `2446079`) write a `META_API_COOLDOWN` row in `SystemConfig`. `assertMetaApiAvailable()` enforces the cooldown (hard-block only when `META_API_COOLDOWN_HARD_BLOCK=true`).
- **Retry helpers** (always reuse these, don't hand-roll retries): `executeMetaApiWithRetry`, `executeDbWithRetry` (handles Postgres `57P03` startup), and `fetchAll` (cursor pagination with rate-limit backoff).

### Data model

`prisma/schema.prisma` is large (~90 models). Rough groupings:
- **Meta mirror**: `Campaign`, `AdSet`, `Ad`, `Creative`, `AdImage`, `AdVideo` + their `*Insight` and `*AudienceInsight` tables. Insights use composite-key upsert (`entityId`, `dateStart`, `range`) and an ID-reuse/overwrite strategy to avoid churn.
- **Draft system**: `TemplateCampaign` → `SystemCampaign`/`SystemAdSet`/`SystemAd`/`SystemCreative`, `DraftAutomationHistory`, `PublishHistory`.
- **Automation engine**: `AutomationRule`, `AutomationFilter*`, `AutomationTask*`, `AutomationRuleRun*`, `AutomationInsight*`.
- **Assets & sources**: `CreativeFolder`, `CreativeAsset`, `CreativeAssetMapping`, `LarkRecord`, `DriveFile`.
- **Platform**: `User`/`Project`/`Permission` (RBAC), `Help*` (chatbot + `HelpAiApiKey` provider pool), `SystemConfig` (runtime config incl. Meta auth/cooldown).

## Conventions

- TypeScript, **single quotes, trailing commas** (`.prettierrc`). ESLint extends `@typescript-eslint/recommended`; `no-explicit-any` is off and `strictNullChecks` is off — `any` and non-null assertions are common here.
- **User-facing strings, log messages, and cron descriptions are written in Vietnamese.** Match that.
- Standalone maintenance script: `src/scripts/recalculate-historical-metrics.ts` (backfills/recomputes historical insight metrics with its own pg `Pool`) — run with `ts-node`, not part of the app lifecycle.

## Environment

`.env` is gitignored. Required to boot: `DATABASE_URL`, `REDIS_HOST`/`REDIS_PORT`, `SDK_FACEBOOK_ACCESS_TOKEN`, `SDK_FACEBOOK_BUSINESS`. Integrations: `GOOGLE_SERVICE_ACCOUNT_JSON` + `GOOGLE_ALLOWED_SHARED_DRIVE_IDS` (Drive permission audit — see `README.md`), `LARK_APP_ID`/`LARK_APP_SECRET`, `GEMINI_API_KEYS`/`DEEPSEEK_API_KEYS` (Help AI). Most sync modules also expose many optional tuning knobs (`INSIGHT_SYNC_*`, `META_MEDIA_UPLOAD_*`, `CREATIVE_*`, `META_API_*`) read directly via `process.env` in their constants/services.
