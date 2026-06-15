import {
  BadRequestException,
  Injectable,
  Logger,
  OnModuleInit,
} from '@nestjs/common';
import axios from 'axios';
import { AdAccount, FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import { PrismaService } from '../prisma/prisma.service';
import { executeMetaApiWithRetry, fetchAll } from '../../common/utils';
import { AD_INSIGHT_FIELDS } from '../../common/utils/meta-field';

@Injectable()
export class MetaApiService implements OnModuleInit {
  private readonly logger = new Logger(MetaApiService.name);
  public readonly businessId =
    process.env.SDK_FACEBOOK_BUSINESS || '1916878948527753';
  private initialized = false;
  private metaAuthConfigCache: { value: any; expiresAt: number } | null = null;

  constructor(private readonly prisma: PrismaService) {}

  onModuleInit() {
    this.initSdk();
  }

  private initSdk() {
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

  public getHeaders(authConfig: any) {
    return {
      accept: '*/*',
      'accept-language': 'en,vi;q=0.9,en-US;q=0.8,vi-VN;q=0.7',
      'content-type': 'application/x-www-form-urlencoded',
      origin: 'https://business.facebook.com',
      referer: 'https://business.facebook.com/',
      'sec-ch-ua':
        '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"macOS"',
      'sec-fetch-dest': 'empty',
      'sec-fetch-mode': 'cors',
      'sec-fetch-site': 'same-site',
      'user-agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      Cookie: authConfig?.cookie || '',
    };
  }

  public async handleMetaError(errorResponse: any) {
    if (!errorResponse) return;
    const error = errorResponse.error || errorResponse;
    if (!error || !error.code) return;

    const code = error.code;
    const type = error.type;

    const isAuthError = code === 190 || code === 102;
    const isLimitError =
      code === 17 || code === 4 || code === 32 || code === 613;

    if (isAuthError || isLimitError) {
      this.logger.warn(
        `Meta API Error [${code}]: ${error.message}. Clearing META_AUTH_CONFIG.`,
      );
      this.metaAuthConfigCache = null;
      await this.prisma.systemConfig.deleteMany({
        where: { key: 'META_AUTH_CONFIG' },
      });
    }
  }

  public async fetchAllPages(initialUrl: string, authConfig: any) {
    let results: any[] = [];
    let nextUrl = initialUrl;

    while (nextUrl) {
      try {
        const response = await axios.get(nextUrl, {
          headers: this.getHeaders(authConfig),
        });
        if (response.data.error) {
          await this.handleMetaError(response.data);
          throw new Error(
            response.data.error.message || 'Meta API Error in fetchAllPages',
          );
        }
        const data = response.data.data || [];
        results = results.concat(data);
        nextUrl = response.data.paging?.next;
      } catch (err: any) {
        await this.handleMetaError(err.response?.data);
        this.logger.error(
          'Fetch All Pages Error:',
          err.response?.data || err.message,
        );
        throw err;
      }
    }
    return results;
  }

  public async getMetaAuthConfig() {
    if (
      this.metaAuthConfigCache &&
      this.metaAuthConfigCache.expiresAt > Date.now()
    ) {
      return this.metaAuthConfigCache.value;
    }

    const config = await this.prisma.systemConfig.findUnique({
      where: { key: 'META_AUTH_CONFIG' },
    });
    const value = (config?.value as any) || {};
    this.metaAuthConfigCache = {
      value,
      expiresAt: Date.now() + 30_000,
    };
    return value;
  }

  public async getFacebookToken(userId: string) {
    const [globalConfig] = await Promise.all([this.getMetaAuthConfig()]);

    const token = globalConfig?.accessToken;
    if (!token) {
      throw new BadRequestException(
        'Hệ thống đang cập nhật và admin đã nhận được thông báo này, vui lòng chọn những tài nguyên đã có',
      );
    }
    return token;
  }

  public async request(
    method: 'get' | 'post' | 'delete',
    endpoint: string,
    params: any = {},
    data?: any,
    configOptions?: any,
  ) {
    const authConfig = await this.getMetaAuthConfig();
    const token = authConfig?.accessToken;

    const url = endpoint.startsWith('http')
      ? endpoint
      : `https://graph.facebook.com/v24.0/${endpoint.replace(/^\//, '')}`;

    const headers = this.getHeaders(authConfig);
    if (configOptions?.headers) {
      Object.assign(headers, configOptions.headers);
    }

    try {
      const response = await axios({
        method,
        url,
        params: { access_token: token, ...params },
        data,
        headers,
        ...configOptions,
      });

      if (response.data?.error) {
        await this.handleMetaError(response.data);
        const ex = new BadRequestException(
          response.data.error.message || 'Meta API Error',
        );
        (ex as any).metaError = response.data.error;
        throw ex;
      }
      return response.data;
    } catch (err: any) {
      await this.handleMetaError(err.response?.data || err);
      if (err.response?.data?.error) {
        (err as any).metaError = err.response.data.error;
      }
      throw err;
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
      retryOptions?: {
        maxRetries?: number;
        initialSleepMs?: number;
        networkSleepMs?: number;
      };
    },
  ) {
    this.initSdk();
    const { level, ids, retryOptions, ...rest } = params;
    const adAccount = new AdAccount(accountId);

    const filtering = [];
    if (ids && ids.length > 0) {
      const fieldMap: Record<string, string> = {
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
      {
        logger: this.logger,
        maxRetries: retryOptions?.maxRetries ?? 3,
        initialSleepMs: retryOptions?.initialSleepMs,
        networkSleepMs: retryOptions?.networkSleepMs ?? 10000,
        context: {
          accountId,
          level,
          ids: ids?.length || 0,
          datePreset: rest.date_preset,
          timeRange: rest.time_range,
          timeIncrement: rest.time_increment,
        },
      },
    );

    return fetchAll(cursor, {
      maxRetries: retryOptions?.maxRetries ?? 3,
      context: {
        accountId,
        level,
        ids: ids?.length || 0,
        datePreset: rest.date_preset,
        timeRange: rest.time_range,
        timeIncrement: rest.time_increment,
      },
    });
  }
}
