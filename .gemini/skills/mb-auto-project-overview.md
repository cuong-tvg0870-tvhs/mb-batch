# MB Auto — Shared Project Overview

Last updated: 2026-07-16

## Service map

- `mb-frontend`: Next.js 16 App Router UI. Browser requests go through the `/api` proxy and use `lib/api.ts`.
- `mb-ads`: NestJS 11 request/response API and Prisma reader/writer.
- `mb-batch`: background Meta sync and automation jobs.
- `mb-db`: canonical Prisma schema and migrations shared by both backend services.

## Marketing dashboards

The same role-aware marketing dashboard is mounted at three levels:

- Main dashboard: `/dashboard`, tab `Hiệu suất Ads`, scope `global`.
- Project detail: `/dashboard/projects/:id`, first report tab, scope `project`.
- Ad-account detail: `/dashboard/ad-accounts/:id`, default `Hiệu suất Ads` tab, scope `account`.

Frontend widgets live in `components/dashboard/marketing/` and are exported through `index.ts`. `MarketingDashboard` owns URL-backed filters, SWR fetching, loading/error/empty states, KPI cards, the Spend + CPA/ROAS trend, funnel, age/gender breakdowns, account comparison and the sortable campaign heatmap table. It has no mock fallback data.

The report filter is a compact collapsible toolbar: date presets stay immediately accessible, while Project/User/Account/Objective live under `Lọc nâng cao`. Long single-select lists use searchable, height-bounded popovers. Account multi-select stages changes and performs one URL/SWR update only after `Áp dụng`; active filters appear as removable chips. Hidden account filters are ignored in account scope, and a stale user filter is validated against backend capabilities before it is ever sent to the API.

Loading UX is shared by all three scopes. The initial request reserves the filter, KPI, chart and table layout with skeletons. On filter changes or background revalidation, SWR keeps the previous response visible, shows an explicit live refresh status and temporarily dims the report body instead of blanking it. Treat `isValidating && data` as a refresh even when SWR also reports `isLoading=true` for an uncached key with `keepPreviousData`.

The frontend reads one endpoint for all three screens:

`GET /dashboard/marketing`

Supported query parameters:

- `scope=global|project|account`
- fixed scope: `projectId`, `accountId`
- optional drill-down: `accountIds` (CSV or repeated), `ownerUserId` (`userId` alias), `objective`
- backward-compatible aliases: `filter_projectId`, `filter_accountIds`, `filter_userId`, `filter_objective`
- dates: `filter_startDate`, `filter_endDate` (`YYYY-MM-DD`)

Without explicit dates the endpoint uses the last 30 calendar days. A request is capped at 366 days. It also computes a previous period with the same number of days for KPI deltas.

### Authorization

Access is resolved on the backend before any insight query:

- `ADMIN`: all ad accounts and projects.
- Project `MANAGER`: every account in the managed project; portfolio/team filters.
- Project `LEADER`: every account in the led project; read-oriented portfolio/team view.
- Project `MEMBER`: only accounts assigned directly through `AccountMember`; personal view.
- Direct account `OWNER`/`MANAGER` maps to manager-level account reporting; `EDITOR`/`VIEWER` maps to member reporting.

Requested account IDs must be a subset of the resolved account scope. A user filter is accepted only when **every selected account** is managed by that viewer: system admin, project manager/leader for that account, or direct account owner/manager. Creator names/emails and campaign owners are omitted entirely for ordinary members and for mixed selections that include an unmanaged account. UI hiding is never treated as authorization.

The response returns `viewer.effectiveRole`, `canViewPortfolio`, `canFilterByUser` and `isPersonalView`; frontend presentation follows those capabilities.

### Metric semantics

Additive fields (`spend`, impressions, daily reach, clicks, outbound clicks, results, purchases and purchase value) are aggregated in the database from `CampaignInsight` rows with `range=DAILY`, grouped separately by day and campaign. Ratio metrics are always recomputed from totals:

- `CPM = spend / impressions × 1000`
- `CTR = clicks / impressions × 100`
- `CPC = spend / clicks`
- `CPA/CPR = spend / results`
- `ROAS = purchaseValue / spend`

Do not average stored ratio columns when extending this dashboard.

`reach` across multiple daily rows is non-additive and can count the same person more than once. The API returns `dataQuality.reachIsEstimated`; the UI labels this limitation. Age/gender breakdowns come from `CampaignAudienceInsight` `MAX` snapshots and are marked as such. Structured placement/publisher-platform data does not exist yet, so the UI must not invent a placement chart.

If selected accounts use multiple currencies or any selected account has no known currency, aggregate monetary fields (`spend`, revenue, CPM, CPC, CPA/CPR and ROAS) are returned as `null`; the API reports `mixedCurrencies`, `hasUnknownCurrency` and `monetaryTotalsUnavailable`. Campaign/account rows retain their own currency and are never formatted using a fabricated fallback.

When unfiltered data contains several campaign objectives (or an unclassified objective), aggregate CPA/CPR is returned as `null` because “result” is heterogeneous. The API sets `mixedObjectives`; the funnel uses outbound clicks and always sets `funnelIsIndicative` because aggregate insights are not a user-level cohort.

### Account-detail capabilities

`GET /accounts/:id` includes `viewerCapabilities`:

- `canViewTeam`
- `canSync`
- `effectiveRole`

Plain account members receive only their own membership row, not the full team roster. The account metadata, Pixel and Audience sync routes call `AccountService.assertCanSync()` server-side; their buttons fail closed unless `canSync === true`.

### Legacy dashboard hardening

The original `/dashboard` payload now scopes `topCampaigns` with the same account access filter. `dailyLogins` is queried and returned only for system admins rather than merely being hidden by the frontend.
