import { InjectQueue } from '@nestjs/bull';
import {
  Injectable,
  InternalServerErrorException,
  Logger,
} from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { InsightRange, LevelInsight, Status } from '@prisma/client';
import { Queue } from 'bull';
import * as dayjs from 'dayjs';
import { Ad, AdAccount, FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import groupBy from 'lodash/groupBy';
import {
  chunk,
  extractCampaignMetrics,
  fetchAll,
  parseMetaError,
  sleep,
} from 'src/common/utils';
import {
  AD_INSIGHT_FIELDS,
  ADSET_FIELDS,
  CAMPAIGN_FIELDS,
  CREATIVE_FIELDS,
} from 'src/common/utils/meta-field';
import { UpsertService } from 'src/modules/campaign-sync-service/upsert.service';
import { PrismaService } from 'src/modules/prisma/prisma.service';
import { TaskService } from './task.service';
@Injectable()
export class TaskCron {
  private readonly logger = new Logger(TaskCron.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly taskService: TaskService,
    private upsertDataService: UpsertService,
    @InjectQueue('meta-sync')
    private readonly queue: Queue,
  ) {}

  private initialized = false;

  private init() {
    if (!this.initialized) {
      FacebookAdsApi.init(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);
      this.initialized = true;
    }
  }

  async onModuleInit() {
    this.logger.log('🚀 App started → scan video immediately');
    // await this.syncDailyCampaignInsights();
    await this.SyncCampaignService();
  }

  // /**
  //  * Scan planning & enqueue job đúng giờ
  //  * IDLE → WAITING
  //  */
  // @Cron(CronExpression.EVERY_MINUTE)
  // async scanPlanning() {
  //   const now = Date.now();
  //   const lookAheadMs = 60_000;

  //   this.logger.log('⏰ Scan planning...');

  //   const plans = await this.prisma.planning.findMany({
  //     where: {
  //       enabled: true,
  //       status: { in: [PlanningStatus.IDLE, PlanningStatus.FAILED] },
  //       nextRunAt: {
  //         lte: new Date(now + lookAheadMs),
  //       },
  //     },
  //   });

  //   for (const plan of plans) {
  //     const jobId = `planning:${plan.id}`;
  //     const delay = Math.max(plan.nextRunAt.getTime() - now, 0);

  //     const existingJob = await this.queue.getJob(jobId);
  //     if (existingJob) continue;

  //     const locked = await this.prisma.planning.updateMany({
  //       where: {
  //         id: plan.id,
  //         enabled: true,
  //         status: PlanningStatus.IDLE,
  //       },
  //       data: {
  //         status: PlanningStatus.WAITING,
  //         lastRunAt: new Date(),
  //       },
  //     });

  //     if (!locked.count) continue;

  //     await this.queue.add(
  //       'run-planning',
  //       { planningId: plan.id },
  //       {
  //         jobId,
  //         delay,
  //         attempts: 3,
  //         backoff: { type: 'exponential', delay: 5000 },
  //         removeOnComplete: true,
  //       },
  //     );

  //     this.logger.log(`📤 Enqueued planning ${plan.id} (delay=${delay})`);
  //   }
  // }

  // /**
  //  * Reconcile zombie state
  //  * RUNNING / WAITING nhưng không có job
  //  */
  // @Cron(CronExpression.EVERY_5_SECONDS)
  // async reconcilePlanning() {
  //   const now = new Date();
  //   this.logger.log('🧹 Reconcile planning state...');

  //   const plans = await this.prisma.planning.findMany({
  //     where: {
  //       enabled: true,
  //       OR: [
  //         {
  //           status: {
  //             in: [PlanningStatus.WAITING, PlanningStatus.RUNNING],
  //           },
  //         },
  //         { status: PlanningStatus.IDLE, nextRunAt: { lte: now } },
  //       ],
  //     },
  //   });

  //   for (const plan of plans) {
  //     const jobId = `planning:${plan.id}`;
  //     const job = await this.queue.getJob(jobId);

  //     // CASE 1: zombie WAITING / RUNNING
  //     if (
  //       plan.status === PlanningStatus.WAITING ||
  //       plan.status === PlanningStatus.RUNNING
  //     ) {
  //       if (!job) {
  //         await this.prisma.planning.update({
  //           where: { id: plan.id },
  //           data: {
  //             status: PlanningStatus.IDLE,
  //             nextRunAt:
  //               plan.nextRunAt && plan.nextRunAt > now
  //                 ? plan.nextRunAt
  //                 : this.taskService.calculateNextRun(plan.schedule),
  //           },
  //         });

  //         this.logger.warn(`🧟 Reset planning ${plan.id} → IDLE`);
  //       }
  //       continue;
  //     }

  //     // CASE 2: IDLE overdue
  //     if (
  //       plan.status === PlanningStatus.IDLE &&
  //       plan.nextRunAt <= now &&
  //       !job
  //     ) {
  //       const locked = await this.prisma.planning.updateMany({
  //         where: {
  //           id: plan.id,
  //           enabled: true,
  //           status: PlanningStatus.IDLE,
  //         },
  //         data: { status: PlanningStatus.WAITING },
  //       });

  //       if (!locked.count) continue;

  //       await this.queue.add(
  //         'run-planning',
  //         { planningId: plan.id },
  //         {
  //           jobId,
  //           delay: 0,
  //           removeOnComplete: true,
  //         },
  //       );

  //       this.logger.log(`⏰ Re-enqueue overdue planning ${plan.id}`);
  //     }
  //   }
  // }

  @Cron(CronExpression.EVERY_DAY_AT_MIDNIGHT, { timeZone: 'Asia/Ho_Chi_Minh' })
  async SyncCampaignService() {
    this.logger.log('⏰ Sync Campaign Data...');
    await this.init();

    try {
      const accounts = await this.prisma.account.findMany({});

      for (const acc of accounts) {
        console.log('acc id', acc.name, acc.id);
        const adAccount = new AdAccount(acc.id);
        const campaignCursor = await adAccount.getCampaigns(
          [
            ...CAMPAIGN_FIELDS,
            `insights.date_preset(maximum).limit(1).level(campaign){${AD_INSIGHT_FIELDS.join(',')}}`,
          ],
          {
            limit: 100,
            filtering: [
              {
                field: 'created_time',
                operator: 'GREATER_THAN',
                value: Math.floor(
                  (Date.now() - 60 * 24 * 60 * 60 * 1000) / 1000,
                ),
              },
            ],
          },
          true,
        );

        const campaigns = await fetchAll(campaignCursor, {
          context: { accountId: acc.id, step: 'FETCH_CAMPAIGNS', sleep: 60000 },
        });

        console.log('campaign length', campaigns?.length);

        // GET ALL ADSET
        const adsetCursor = await adAccount.getAdSets(
          [
            ...ADSET_FIELDS,
            `insights.date_preset(maximum).limit(1).level(adset){${AD_INSIGHT_FIELDS.join(',')}}`,
          ],
          {
            limit: 50,
            filtering: [
              {
                field: 'created_time',
                operator: 'GREATER_THAN',
                value: Math.floor(
                  (Date.now() - 30 * 24 * 60 * 60 * 1000) / 1000,
                ),
              },
            ],
          },
          true,
        );
        const adSets = await fetchAll(adsetCursor, {
          context: { accountId: acc.id, step: 'FETCH ADSETS', sleep: 60000 },
        });

        console.log('adSets length', adSets?.length);

        // GET ALL AD
        const adCursor = await adAccount.getAds(
          [
            Ad.Fields.id,
            Ad.Fields.account_id,
            Ad.Fields.campaign_id,
            Ad.Fields.adset_id,
            Ad.Fields.name,
            Ad.Fields.status,
            Ad.Fields.effective_status,
            Ad.Fields.creative_asset_groups_spec,
            Ad.Fields.bid_amount,
            Ad.Fields.priority,
            Ad.Fields.created_time,
            Ad.Fields.updated_time,
            `creative{${CREATIVE_FIELDS.join(',')}}`,
            `insights.date_preset(maximum).limit(1).level(ad){${AD_INSIGHT_FIELDS.join(',')}}`,
          ],
          {
            limit: 50,
            filtering: [
              {
                field: 'updated_time',
                operator: 'GREATER_THAN',
                value: Math.floor(
                  (Date.now() - 30 * 24 * 60 * 60 * 1000) / 1000,
                ),
              },
            ],
          },
          true,
        );
        const ads = await fetchAll(adCursor, {
          context: {
            accountId: acc.id,
            step: 'FETCH Ads',
            sleep: 60000,
          },
        });

        console.log('ads length', ads?.length);

        const adSetsByCampaign = groupBy(adSets, (as) => as.campaign_id);
        const adsByAdSet = groupBy(ads, (ad) => ad.adset_id);

        for (const ad of ads) {
          await this.prisma.$transaction(async (tx) => {
            await this.upsertDataService.syncAdAssetsLegacy(
              tx,
              adAccount,
              acc.id,
              ad,
            );
          });

          await this.prisma.$transaction(async (tx) => {
            await this.upsertDataService.upsertCreativeLegacy(tx, acc.id, ad);
          });
        }

        for (const campaign of campaigns) {
          console.log('campaign id', campaign?.id);

          await this.prisma.$transaction(async (tx) => {
            const accountId = acc.id;
            await this.upsertDataService.upsertCampaign(
              tx,
              accountId,
              campaign,
            );

            const campaignAdSets = adSetsByCampaign[campaign.id] ?? [];

            console.log('campaignAdSets', campaignAdSets?.length);
            for (const adset of campaignAdSets) {
              await this.upsertDataService.upsertAdSet(
                tx,
                accountId,
                campaign.id,
                adset,
              );

              const adsetAds = adsByAdSet[adset.id] ?? [];
              console.log('adsetAds', adsetAds?.length);

              for (const ad of adsetAds) {
                await this.upsertDataService.syncCidLegacy(tx, accountId, ad);

                await this.upsertDataService.upsertAdLegacy(
                  tx,
                  accountId,
                  campaign.id,
                  adset.id,
                  ad,
                );
              }
            }
          });
        }
      }
      this.logger.log('--- END Campaign Data ---');
      return { status: 'DONE' };
    } catch (err) {
      throw new InternalServerErrorException(parseMetaError(err));
    }
  }

  @Cron('0 6,12,18 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncDailyCampaignInsights() {
    this.logger.log('⏰ Sync campaign daily insight');
    this.init();
    const campaigns = await this.prisma.campaign.findMany({
      where: {
        deletedAt: null,
        status: { in: [Status.ACTIVE, Status.PAUSED] },
      },
      select: { id: true, accountId: true, createdAt: true },
    });
    if (!campaigns.length) {
      this.logger.log('No campaign need sync');
      return;
    }

    /**
     * Group campaign by account
     */
    const byAccount = campaigns.reduce<Record<string, any[]>>((acc, c) => {
      acc[c.accountId] ||= [];
      acc[c.accountId].push(c);
      return acc;
    }, {});

    /**
     * 2️⃣ Loop từng ad account
     */
    for (const [accountId, list] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      /**
       * Chunk campaign id (<= 50)
       */
      for (const chunkCampaigns of chunk(list, 50)) {
        const campaignIds = chunkCampaigns.map((c) => c.id);

        /**
         * 3️⃣ Tính date range
         */
        const latestInsight = await this.prisma.campaignInsight.findFirst({
          where: { campaignId: { in: campaignIds } },
          orderBy: { dateStop: 'desc' },
          select: { dateStop: true },
        });

        const since = latestInsight
          ? dayjs(latestInsight.dateStop).subtract(2, 'day')
          : dayjs(
              Math.min(
                ...chunkCampaigns.map((c) => new Date(c.createdAt).getTime()),
              ),
            );

        const until = dayjs();

        const dates: string[] = [];
        let d = since.clone();
        while (d.isSame(until, 'day') || d.isBefore(until, 'day')) {
          dates.push(d.format('YYYY-MM-DD'));
          d = d.add(1, 'day');
        }

        /**
         * 4️⃣ Fetch insight (NO SUMMARY)
         */
        const cursor = await adAccount.getInsights(
          AD_INSIGHT_FIELDS,
          {
            level: 'campaign',
            time_increment: 1,
            filtering: [
              {
                field: 'campaign.id',
                operator: 'IN',
                value: campaignIds,
              },
            ],
            time_range: {
              since: since.format('YYYY-MM-DD'),
              until: until.format('YYYY-MM-DD'),
            },
          },
          true,
        );

        const insights = await fetchAll(cursor);

        /**
         * 5️⃣ Map insight theo campaign|date
         */
        const insightMap = new Map<string, any>();
        for (const i of insights) {
          insightMap.set(`${i.campaign_id}|${i.date_start}`, i);
        }

        /**
         * 6️⃣ Upsert DB (kể cả empty day)
         */

        for (const c of chunkCampaigns) {
          for (const date of dates) {
            const key = `${c.id}|${date}`;
            const insight = insightMap.get(key);

            await this.prisma.campaignInsight.upsert({
              where: {
                campaignId_dateStart_dateStop_range: {
                  campaignId: c.id,
                  dateStart: date,
                  dateStop: date,
                  range: InsightRange.DAILY,
                },
              },
              update: {
                ...(insight ? extractCampaignMetrics(insight) : {}),
                rawPayload: insight ?? null,
              },
              create: {
                campaignId: c.id,
                level: LevelInsight.CAMPAIGN,
                range: InsightRange.DAILY,
                dateStart: date,
                dateStop: date,
                ...(insight ? extractCampaignMetrics(insight) : {}),
                rawPayload: insight ?? null,
              },
            });
          }
        }

        await sleep(800); // throttle
      }
    }

    this.logger.log('✅ Campaign Daily Insight DONE');
  }
}
