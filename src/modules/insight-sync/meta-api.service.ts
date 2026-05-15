import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { AdAccount, FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import { executeMetaApiWithRetry, fetchAll } from '../../common/utils';
import { AD_INSIGHT_FIELDS } from '../../common/utils/meta-field';

@Injectable()
export class MetaApiService implements OnModuleInit {
  private readonly logger = new Logger(MetaApiService.name);
  private initialized = false;

  onModuleInit() {
    this.init();
  }

  private init() {
    if (!this.initialized) {
      const token = process.env.SDK_FACEBOOK_ACCESS_TOKEN;
      if (!token) {
        throw new Error(
          '❌ SDK_FACEBOOK_ACCESS_TOKEN is missing in environment variables!',
        );
      }
      FacebookAdsApi.init(token);
      this.initialized = true;
      this.logger.log('✅ Meta SDK Initialized successfully');
    }
  }

  /**
   * Fetch insights from Meta for a specific account
   */
  async getAccountInsights(
    accountId: string,
    params: {
      level: 'campaign' | 'adset' | 'ad';
      date_preset?: string;
      time_range?: { since: string; until: string };
      time_increment?: number | 'all_days';
      ids?: string[];
      limit?: number;
      breakdowns?: string[];
    },
  ) {
    this.init();
    const { level, ids, ...rest } = params;
    const adAccount = new AdAccount(accountId);

    const filtering = [];
    if (ids && ids.length > 0) {
      const fieldMap = {
        campaign: 'campaign.id',
        adset: 'adset.id',
        ad: 'ad.id',
      };
      filtering.push({
        field: fieldMap[level],
        operator: 'IN',
        value: ids,
      });
    }

    const cursor = await executeMetaApiWithRetry(
      () =>
        adAccount.getInsights(
          AD_INSIGHT_FIELDS,
          {
            ...rest,
            level,
            filtering,
            action_attribution_windows: '7d_click',
            action_breakdowns: 'action_type',
          },
          true, // iterate
        ),
      { logger: this.logger },
    );

    return fetchAll(cursor);
  }
}
