import { Injectable, Logger } from '@nestjs/common';
import CronExpressionParser from 'cron-parser';
import { AdAccount, FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import { fetchAll } from 'src/common/utils';
import { CAMPAIGN_FIELDS } from 'src/common/utils/meta-field';
import { UpsertService } from 'src/modules/campaign-sync-service/upsert.service';
import { MetaService } from 'src/modules/meta/meta.service';

@Injectable()
export class TaskService {
  private readonly logger = new Logger(TaskService.name);

  constructor(
    private readonly metaService: MetaService,
    private upsertDataService: UpsertService,
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

      for (const accountId of accounts) {
        const adAccount = new AdAccount(accountId);
        this.logger.log(
          `🔄 Sync campaigns | account=${accountId} | planning=${plan.id}`,
        );

        const campaignCursor = await adAccount.getCampaigns(
          CAMPAIGN_FIELDS,
          {
            filtering: [
              {
                field: 'updated_time',
                operator: 'GREATER_THAN',
                value: Math.floor(
                  (Date.now() - 30 * 24 * 60 * 60 * 1000) / 1000,
                ),
              },
            ],
            limit: 1,
          },
          true,
        );

        const campaigns = await fetchAll(campaignCursor, {
          context: { accountId: accountId, step: 'FETCH_CAMPAIGNS' },
        });

        console.log(campaigns.length);
        return;
        for (const c of campaigns) {
          // fetch full tree: campaign -> adsets -> ads -> creative
          const campaignTree = await this.metaService.fetchCampaignData(c);

          if (!campaignTree) continue;

          await this.upsertDataService.syncCampaignTree(
            accountId,
            adAccount,
            campaignTree,
          );
        }
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
