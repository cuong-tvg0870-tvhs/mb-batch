# MB Auto — Shared Project Overview

Last updated: 2026-07-21

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

## Campaign template playbooks

`TemplateCampaign` keeps the existing `data` skeleton and adds three dedicated fields so launch behavior is not lost when the structure is re-saved:

- `purpose`: `STANDARD`, `TESTING_CONTENT`, or `SCALE_POST_WIN`.
- `launchContract`: versioned JSON containing `allowedStructureModes`, `defaultStructureMode`, and `contentMode`.
- `locked`: defaults to `true`. Full structure updates are rejected once saved; metadata and launch behavior remain editable through quick-edit.

The canonical migration is `mb-db/prisma/migrations/20260721090000_add_template_purpose_and_launch_contract`. It classifies legacy templates from the existing TestingContent naming convention or pinned-post signal, then locks existing populated templates. Do not run migrations automatically; apply them through the normal deployment process.

The backend normalizes the JSON allow-list in `draft-campaign/template-contract.ts`. `SCALE_POST_WIN` is constrained to `REUSE_EXISTING_POST`; `TESTING_CONTENT` is constrained to `NEW_CREATIVE`. Template create, clone, list, and quick-edit preserve these fields. `PATCH /draft-campaigns/template/:id/quick-edit` is the supported way to change name, description, purpose, or launch contract without changing objective, targeting, placement, or the campaign/ad-set structure.

The frontend shared contract is `lib/campaign-template.ts`. Save and quick-edit dialogs use `TemplateModeFields`. When content is already selected, `TemplatePickerButton` opens `TemplateLaunchPlanPanel` before applying a template:

- `KEEP_TEMPLATE`: fill only the existing creative slots; surplus content is explicitly reported.
- `ADD_ADS`: preserve the ad-set structure and append enough ads to use all selected content.
- `CLONE_ADSETS`: clone the first strategic ad-set per fanpage or per selected content.

`SCALE_POST_WIN` must resolve or already contain a compatible `object_story_id`; it fails closed when a selected item has no reusable post on the target Page. `AUTO` may fall back to a new creative and tells the marketer that social proof will not be retained. Multiple source Pages require ad-set cloning or one explicit target Page, and Page-promotion permissions are checked before the wizard can confirm.

Automation routing also uses `purpose` rather than template-name substrings. Draft Automation (watch folders and fill new image/video assets) accepts only `TESTING_CONTENT`; Auto Launch Rule (rank winning posts and reuse `object_story_id`) accepts only `SCALE_POST_WIN`. Both frontend pickers filter to the compatible purpose, while API create/update/preview/run and the `mb-batch` scheduler fail closed if a template is later changed to an incompatible purpose. The sidebar presents these workflows as `Tự động hóa Ads` → `Scale bài hiệu quả`, `Test content tự động`, and `Mẫu lịch chạy`.

Auto Launch preview and execution share `packGroupPure` and default to `DELIVERY_CAP` with six ads per ad set, including legacy rules that have no packing field. A run creates one campaign, groups reusable Post-ID ads by source Page, and splits another ad set for the same Page only when that Page exceeds the cap. `launchFromTemplateWithPostGroups` clones the first template ad set as the strategic blueprint, assigns `promoted_object.page_id` per generated ad set, and gives repeated Page groups a sequence suffix. Preview therefore reflects the structure that is actually persisted and published. CBO keeps the campaign budget; ABO preserves the template's total ad-set budget and redistributes it across generated ad sets, so Page grouping never multiplies spend.

The Auto Launch create/edit form keeps submit errors inside the dialog. Client validation is collected into one inline summary, affected controls receive inline messages and destructive styling, and the left form pane scrolls/focuses the first issue. API messages are mapped to a known field when possible and otherwise focus the summary; save errors must not use an external destructive toast. Success confirmation may still use the standard toast.

The Scale template picker uses the rich mode of `components/ui/searchable-select.tsx`. `listLaunchTemplates()` keeps the encoded technical name as hidden search keywords but also returns a cleaned display name, Vietnamese objective/optimization labels, budget mode, ad-set count, and ad count. The trigger shows the short name plus strategic summary; dropdown options render wrapped card rows with reuse-post, CBO/ABO, group, and ad badges. Compact consumers of the shared searchable select retain their original single-line layout.

## Budget rule templates

`CampaignRuleTemplate` is the account-agnostic reusable shape for conditional budget rules. Its `config` stores the task tree, filter groups, and default notification flags; the concrete `CampaignRule` supplies account/campaign/ad-set scope, timezone, scan schedule, status, and recipients. `sourceTemplateId` is the exact usage stamp. Older rules without that stamp may still be traced approximately by the rolling-budget fingerprint and must remain labelled as a configuration match rather than proven template usage.

`GET /campaign-rule-templates/:id/campaigns?page&pageSize` returns a server-paginated list of distinct campaigns plus the matching rule rows for each campaign. Every nested rule includes status, level, `adSetId`/ad-set name, and whether it is stamped or only a legacy configuration match. `stampedCount` and `configMatchCount` remain totals across the accessible result set, while `pagination` describes the current page. The frontend rule-template editor renders this as a fixed right sidebar beside the independently scrollable editor body.

`GET /campaign-rule-templates/:id/candidates?search&page&pageSize` searches accessible campaigns with database pagination and returns `alreadyLinked` for the current template. The sidebar uses this endpoint to attach the current template without bypassing the normal campaign-rule contract. Both lists show an initial skeleton, keep stale rows visible while revalidating, expose retryable error states, and disable pagination controls while a request is pending. Quick attach intentionally creates a `DRAFT` conditional rule, stamped with `sourceTemplateId`, scanning every 60 minutes. CBO campaigns use `level=CAMPAIGN`; ABO campaigns use `level=ADSET` scoped by `campaignId`. It never activates budget changes automatically: the marketer reviews and enables the rule from campaign detail.
