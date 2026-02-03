import { Injectable, Logger } from '@nestjs/common';
import CronExpressionParser from 'cron-parser';
import { AdAccount, FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import groupBy from 'lodash/groupBy';
import { fetchAll } from 'src/common/utils';
import {
  AD_FIELDS,
  ADSET_FIELDS,
  CAMPAIGN_FIELDS,
} from 'src/common/utils/meta-field';
import { UpsertService } from 'src/modules/campaign-sync-service/upsert.service';
import { MetaService } from 'src/modules/meta/meta.service';
import { PrismaService } from 'src/modules/prisma/prisma.service';

@Injectable()
export class TaskService {
  private readonly logger = new Logger(TaskService.name);

  constructor(
    private readonly meta: MetaService,
    private upsertDataService: UpsertService,
    private prisma: PrismaService,
  ) {}
  private initialized = false;

  private init() {
    if (!this.initialized) {
      FacebookAdsApi.init(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);
      this.initialized = true;
    }
  }

  /**
   * ============================
   * SYNC CAMPAIGN
   * ============================
   */
  async syncCampaign(plan: any): Promise<void> {
    this.logger.log(`🚀 [SYNC_CAMPAIGN] Start | planning=${plan.id}`);
    const start = Date.now();

    const { accounts, campaigns } = plan.config || {};

    if (!Array.isArray(accounts) || accounts.length === 0) {
      this.logger.warn(
        `⚠️ [SYNC_CAMPAIGN] No accountIds provided | planning=${plan.id}`,
      );
      return;
    }

    try {
      await this.init();

      for (const acc of accounts) {
        const adAccount = new AdAccount(acc.id);

        const campaignCursor = await adAccount.getCampaigns(
          CAMPAIGN_FIELDS,
          { limit: 20 },
          true,
        );

        const campaigns = await fetchAll(campaignCursor, {
          context: { accountId: acc.id, step: 'FETCH_CAMPAIGNS' },
        });

        // GET ALL ADSET
        const adsetCursor = await adAccount.getAdSets(
          ADSET_FIELDS,
          { limit: 20 },
          true,
        );
        const adSets = await fetchAll(adsetCursor, {
          context: { accountId: acc.id, step: 'FETCH ADSETS' },
        });

        // GET ALL AD
        const adCursor = await adAccount.getAds(AD_FIELDS, { limit: 20 }, true);
        const ads = await fetchAll(adCursor, {
          context: { accountId: acc.id, step: 'FETCH Ads' },
        });

        const adSetsByCampaign = groupBy(adSets, (as) => as.campaign_id);
        const adsByAdSet = groupBy(ads, (ad) => ad.adset_id);

        await this.prisma.$transaction(async (tx) => {
          const accountId = acc;
          for (const campaign of campaigns) {
            await this.upsertDataService.upsertCampaign(
              tx,
              accountId,
              campaign,
            );

            const campaignAdSets = adSetsByCampaign[campaign.id] ?? [];
            for (const adset of campaignAdSets) {
              await this.upsertDataService.upsertAdSet(
                tx,
                accountId,
                campaign.id,
                adset,
              );

              const adsetAds = adsByAdSet[adset.id] ?? [];
              for (const ad of adsetAds) {
                await this.upsertDataService.upsertCreativeLegacy(
                  tx,
                  accountId,
                  ad,
                );

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
          }
        });
      }

      this.logger.log(`✅ [SYNC_CAMPAIGN] Done in ${Date.now() - start}ms`);
    } catch (error) {
      this.logger.error(
        `❌ [SYNC_CAMPAIGN] Failed | planning=${plan.id}`,
        error?.stack || error,
      );
      throw error;
    }
  }

  /**
   * ============================
   * SYNC INSIGHT
   * ============================
   */
  async syncInsight(plan: any): Promise<void> {
    this.logger.log(`🚀 [SYNC_INSIGHT] Start | planning=${plan.id}`);
    const start = Date.now();

    const { accountIds, level, datePreset } = plan.config || {};

    if (!Array.isArray(accountIds) || !level) {
      this.logger.warn(
        `⚠️ [SYNC_INSIGHT] Missing accountIds or level | planning=${plan.id}`,
      );
      return;
    }

    try {
      for (const accountId of accountIds) {
        this.logger.log(
          `📊 Sync insight | account=${accountId} | level=${level}`,
        );

        // await this.metaService.syncAdInsights({
        //   accountId,
        //   level,
        //   datePreset,
        // });
      }

      this.logger.log(`✅ [SYNC_INSIGHT] Done in ${Date.now() - start}ms`);
    } catch (error) {
      this.logger.error(
        `❌ [SYNC_INSIGHT] Failed | planning=${plan.id}`,
        error?.stack || error,
      );
      throw error;
    }
  }

  /**
   * ============================
   * AUTO TOGGLE CAMPAIGN
   * ============================
   */
  async autoToggleCampaign(plan: any): Promise<void> {
    this.logger.log(`🚀 [AUTO_TOGGLE_CAMPAIGN] Start | planning=${plan.id}`);
    const start = Date.now();

    const { conditions, action, campaignIds } = plan.config || {};

    if (!Array.isArray(campaignIds) || !action) {
      this.logger.warn(
        `⚠️ [AUTO_TOGGLE_CAMPAIGN] Missing campaignIds or action | planning=${plan.id}`,
      );
      return;
    }

    try {
      // const campaigns = await this.metaService.getCampaignStats(campaignIds);

      // const matched = campaigns.filter((c) => {
      //   if (conditions?.spendGt && c.spend <= conditions.spendGt) return false;
      //   if (conditions?.roasLt && c.roas >= conditions.roasLt) return false;
      //   if (conditions?.ctrLt && c.ctr >= conditions.ctrLt) return false;
      //   return true;
      // });

      // for (const campaign of matched) {
      //   await this.metaService.updateCampaignStatus(
      //     campaign.id,
      //     action === 'PAUSE' ? 'PAUSED' : 'ACTIVE',
      //   );
      // }

      this.logger.log(
        `✅ [AUTO_TOGGLE_CAMPAIGN] Done in ${Date.now() - start}ms`,
      );
    } catch (error) {
      this.logger.error(
        `❌ [AUTO_TOGGLE_CAMPAIGN] Failed | planning=${plan.id}`,
        error?.stack || error,
      );
      throw error;
    }
  }

  /**
   * ============================
   * CALCULATE NEXT RUN
   * ============================
   */

  calculateNextRun(schedule: any): Date {
    if (!schedule) {
      throw new Error('Schedule is required');
    }

    /**
     * ===== CRON =====
     */
    if (schedule.cron) {
      try {
        const interval = CronExpressionParser.parse(schedule.cron);

        return interval.next().toDate();
      } catch (e) {
        throw new Error(`Invalid cron expression: ${schedule.cron}`);
      }
    }

    /**
     * ===== INTERVAL =====
     */
    const every = Number(schedule.every);
    const unit = schedule.unit;

    if (!every || every <= 0) {
      throw new Error('schedule.every must be > 0');
    }

    let ms: number;

    switch (unit) {
      case 'minute':
        ms = every * 60_000;
        break;
      case 'hour':
        ms = every * 3_600_000;
        break;
      case 'day':
        ms = every * 86_400_000;
        break;
      default:
        throw new Error(`Invalid schedule.unit: ${unit}`);
    }

    return new Date(Date.now() + ms);
  }
}
