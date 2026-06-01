# Skill: NestJS Batch Schedulers & Draft Automation (mb-batch)

## Tech Stack & Architecture
- **Framework**: NestJS (v11) console application for cron batch tasks.
- **ORM**: Prisma (v7) with PostgreSQL.
- **ORM Instance**: Always import/inject `PrismaService` from `src/modules/prisma/prisma.service.ts` (note the batch-specific modules folder path).
- **Meta SDK**: Uses `facebook-nodejs-business-sdk` version `24.0.1` (Graph API v24.0).
- **Scheduling**: Enforce NestJS Schedule (`@nestjs/schedule` module) using `@Cron` or `@Interval` decorators.

## Draft Automation Scheduler Specification
The batch job includes `DraftAutomationScheduler` under `src/modules/draft-automation/draft-automation.scheduler.ts`. When creating or updating automated cron rules or media mappings, adhere to the following workflow:

### 1. Template Scanning
- Query all active templates from `TemplateCampaign` where `deletedAt` is null.
- Identify active automation configurations within the template data matching:
  `data.automation.enabled === true && data.automation.folderId`

### 2. Asset Retrieval & Validation
- Query `CreativeAsset` where `folderId === automation.folderId`.
- Match ownership: The asset creator's `larkRecord.employee_id` must match the template creator's `employee_id`.
- Exclude already published assets present in `CreativeAssetMapping` table.
- Exclude assets currently utilized in active drafts (`SystemCampaign` where status is `DRAFT` and the asset's ID/hash is parsed anywhere in the campaign, adset, or ad data).
- Filter by name rule using `automation.nameRule` if specified.

### 3. Placeholders & Substitution
- Replaces placeholder keys: `"VIDEO_1"`, `"VIDEO_2"`, etc. and `"IMAGE_1"`, `"IMAGE_2"`, etc. within template configs with matching eligible assets.
- Updates entity names dynamically replacing date formats and mapping employee identifiers.
- Keeps track of used assets in `substitutedValues.automation_used_assets`.

### 4. Database Persistence
- Performs transactional writes (`this.prisma.$transaction`) to save the campaign draft, generating linked rows in:
  - `SystemCampaign`
  - `SystemAdSet`
  - `SystemAd`
- All rows are saved with status `Status.DRAFT`.
