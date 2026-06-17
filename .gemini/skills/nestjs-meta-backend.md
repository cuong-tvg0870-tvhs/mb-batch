# Skill: NestJS Batch Schedulers & Draft Automation (mb-batch)

## Tech Stack & Architecture
- **Framework**: NestJS (v11) console application for cron batch tasks.
- **ORM**: Prisma (v7) with PostgreSQL.
- **ORM Instance**: Always import/inject `PrismaService` from `src/modules/prisma/prisma.service.ts`.
- **Meta SDK**: Uses `facebook-nodejs-business-sdk` version `24.0.1` (Graph API v24.0).
- **Scheduling**: Enforce NestJS Schedule (`@nestjs/schedule` module) using `@Cron` or `@Interval` decorators.
- **Queue**: Uses `@nestjs/bull` for Redis job queues.

## Draft Automation Workflow
The batch job includes `DraftAutomationScheduler` under `src/modules/draft-automation/draft-automation.scheduler.ts`. When working on automation rules or media mappings, refer directly to this file for the implementation of:
1. **Template Scanning**: Querying active templates and automation configs.
2. **Asset Retrieval**: Fetching eligible `CreativeAsset` and validation logic.
3. **Substitution**: Replacing placeholders (`VIDEO_1`, `IMAGE_1`) with actual assets.
4. **Persistence**: Transactional writes (`this.prisma.$transaction`) to save drafts (`SystemCampaign`, `SystemAdSet`, `SystemAd`).
