import { Injectable, Logger } from '@nestjs/common';
import {
  AdAccount,
  FacebookAdsApi,
  User,
} from 'facebook-nodejs-business-sdk';
import { fetchAll, parseMetaError, sleep } from '../../common/utils';
import {
  AD_ACCOUNT_FIELDS,
  AD_PIXEL_FIELDS,
  PAGE_FIELDS,
} from '../../common/utils/meta-field';
import { BatchRunContext } from '../batch-run-log/batch-run-log.types';
import { PrismaService } from '../prisma/prisma.service';
import { ENTITY_SYNC_CONFIG } from './entity-sync.constants';

/** Fields của custom audience — giữ khớp với mb-ads MetaService để parity. */
const CUSTOM_AUDIENCE_FIELDS = [
  'id',
  'name',
  'description',
  'approximate_count_lower_bound',
  'approximate_count_upper_bound',
  'subtype',
  'delivery_status',
  'operation_status',
];

export interface EntitySyncStats {
  accountsTotal: number;
  accountsOk: number;
  accountsFailed: number;
  fanpages: number;
  fanpagesWithWhatsapp: number;
  fanpagesWithInstagram: number;
  pixels: number;
  audiences: number;
  catalogs: number;
  productSets: number;
  productFeeds: number;
  errors: string[];
}

/**
 * Đồng bộ metadata các thực thể Meta (TKQC / Fanpage / Pixel / Audience / Catalog)
 * về Postgres, 1 ngày/lần.
 *
 * Logic được port 1:1 từ `mb-ads` `MetaService` (các nút Sync chạy tay ở màn list)
 * để hai writer ghi cùng shape — đây là ràng buộc parity của platform. Khác biệt
 * duy nhất: bổ sung field WhatsApp/Instagram vào lượt lấy fanpage, và bọc try/catch
 * theo từng account để một TKQC lỗi token không làm hỏng cả lượt chạy.
 *
 * Mọi field Meta trả về được lưu nguyên vào cột JSON (`Account.rawPayload/pixels/
 * customAudiences/pages`, `Fanpage.rawPayload`) — không tách cột (theo chốt của user).
 */
@Injectable()
export class EntitySyncService {
  private readonly logger = new Logger(EntitySyncService.name);
  private initialized = false;

  constructor(private readonly prisma: PrismaService) {}

  private init() {
    if (this.initialized) return;
    const token = process.env.SDK_FACEBOOK_ACCESS_TOKEN;
    if (!token) {
      throw new Error('SDK_FACEBOOK_ACCESS_TOKEN is missing in environment');
    }
    FacebookAdsApi.init(token);
    this.initialized = true;
  }

  private buildAccountRawPayload(
    accountPayload: any,
    extras: Record<string, any> = {},
  ) {
    const base =
      accountPayload &&
      typeof accountPayload === 'object' &&
      !Array.isArray(accountPayload)
        ? accountPayload
        : {};
    return { ...base, ...extras };
  }

  /**
   * Kéo toàn bộ fanpage user quản lý (edge me/accounts) + Instagram + số WhatsApp,
   * upsert vào bảng `Fanpage`. Trả về mảng payload để nhồi vào `Account.pages`.
   */
  private async syncFanpages(
    me: User,
    stats: EntitySyncStats,
    ctx?: BatchRunContext,
  ): Promise<any[]> {
    const pageCursor = await me.getAccounts([...PAGE_FIELDS], {
      limit: ENTITY_SYNC_CONFIG.pageLimit,
    });
    const pages = await fetchAll(pageCursor);

    for (const page of pages) {
      if (!page?.id) continue;
      try {
        await this.prisma.fanpage.upsert({
          where: { id: page.id },
          create: { id: page.id, name: page.name, rawPayload: page },
          update: { name: page.name, rawPayload: page },
        });
        stats.fanpages += 1;
        if (page.whatsapp_number || page.has_whatsapp_number) {
          stats.fanpagesWithWhatsapp += 1;
        }
        if (page.instagram_business_account || page.connected_instagram_account) {
          stats.fanpagesWithInstagram += 1;
        }
      } catch (error) {
        const msg = `Fanpage ${page.id}: ${parseMetaError(error).message}`;
        stats.errors.push(msg);
        ctx?.error(msg);
      }
    }

    this.logger.log(
      `📄 Fanpages: ${stats.fanpages} (WhatsApp ${stats.fanpagesWithWhatsapp}, IG ${stats.fanpagesWithInstagram})`,
    );
    return pages;
  }

  /**
   * Liệt kê tất cả ad account (edge me/adaccounts), với mỗi TKQC kéo thêm pixel +
   * custom audience rồi upsert vào bảng `Account`. Bọc try/catch theo account.
   */
  private async syncAccounts(
    me: User,
    pagePayloads: any[],
    stats: EntitySyncStats,
    ctx?: BatchRunContext,
  ): Promise<void> {
    const accountsCursor = await me.getAdAccounts([...AD_ACCOUNT_FIELDS], {
      limit: ENTITY_SYNC_CONFIG.pageLimit,
    });

    for await (const account of accountsCursor) {
      const acc = (account as any)._data;
      if (!acc?.id) continue;
      stats.accountsTotal += 1;

      try {
        const adAccount = new AdAccount(acc.id);

        const pixelCursor = await adAccount.getAdsPixels([...AD_PIXEL_FIELDS]);
        const pixels = await fetchAll(pixelCursor);

        const audienceCursor = await adAccount.getCustomAudiences(
          [...CUSTOM_AUDIENCE_FIELDS],
          { limit: ENTITY_SYNC_CONFIG.audienceLimit },
        );
        const customAudiences = await fetchAll(audienceCursor);

        const rawPayload = this.buildAccountRawPayload(acc, {
          pages: pagePayloads,
          pixels,
          customAudiences,
        });

        await this.prisma.account.upsert({
          where: { id: acc.id },
          update: {
            name: acc.name,
            currency: acc.currency,
            timezone: acc.timezone_name,
            businessId: acc?.business?.id,
            businessName: acc?.business?.name,
            pages: pagePayloads,
            pixels,
            customAudiences,
            rawPayload,
            needsReauth: false,
            lastFetchedAt: new Date(),
          },
          create: {
            id: acc.id,
            name: acc.name,
            accountType: 'AD_ACCOUNT',
            currency: acc.currency,
            timezone: acc.timezone_name,
            businessId: acc?.business?.id,
            businessName: acc?.business?.name,
            pages: pagePayloads,
            pixels,
            customAudiences,
            rawPayload,
            needsReauth: false,
            lastFetchedAt: new Date(),
          },
        });

        stats.accountsOk += 1;
        stats.pixels += pixels.length;
        stats.audiences += customAudiences.length;
      } catch (error) {
        stats.accountsFailed += 1;
        const msg = `Account ${acc.id}: ${parseMetaError(error).message}`;
        stats.errors.push(msg);
        ctx?.error(msg);
        this.logger.warn(`❌ ${msg}`);
      }

      await sleep(ENTITY_SYNC_CONFIG.accountSleepMs);
    }
  }

  /**
   * Đồng bộ product catalog (owned/client/assigned) + product set + product feed.
   * Port 1:1 từ mb-ads `syncCatalogs`.
   */
  private async syncCatalogs(
    stats: EntitySyncStats,
    ctx?: BatchRunContext,
  ): Promise<void> {
    const accessToken = process.env.SDK_FACEBOOK_ACCESS_TOKEN;
    const businessId = process.env.SDK_FACEBOOK_BUSINESS;
    if (!accessToken || !businessId) {
      const msg = 'Catalog sync skipped: thiếu SDK_FACEBOOK_ACCESS_TOKEN/SDK_FACEBOOK_BUSINESS';
      stats.errors.push(msg);
      ctx?.warn(msg);
      return;
    }

    try {
      const api = new FacebookAdsApi(accessToken);
      const catalogFields =
        'id,name,product_sets.limit(100){id,name,filter},business{id,name},product_feeds.limit(100){id,name,country,schedule}';
      const catalogParams = () => ({ limit: 100, fields: catalogFields });

      const [ownedRes, clientRes, assignedRes] = await Promise.allSettled([
        api.call('GET', [businessId, 'owned_product_catalogs'], catalogParams()),
        api.call('GET', [businessId, 'client_product_catalogs'], catalogParams()),
        api.call('GET', ['me', 'assigned_product_catalogs'], catalogParams()),
      ]);

      const catalogsMap = new Map<string, any>();
      for (const res of [ownedRes, clientRes, assignedRes]) {
        // api.call trả về unknown → cast để đọc .data (giống mb-ads syncCatalogs).
        const value = res.status === 'fulfilled' ? (res.value as any) : null;
        if (!Array.isArray(value?.data)) continue;
        for (const item of value.data) catalogsMap.set(item.id, item);
      }

      const catalogs = Array.from(catalogsMap.values());
      for (const catalog of catalogs) {
        await this.prisma.productCatalog.upsert({
          where: { id: catalog.id },
          update: {
            name: catalog.name,
            businessId: catalog.business?.id || null,
            accountId: null,
          },
          create: {
            id: catalog.id,
            name: catalog.name,
            businessId: catalog.business?.id || null,
            accountId: null,
          },
        });
        stats.catalogs += 1;

        for (const set of catalog.product_sets?.data || []) {
          await this.prisma.productSet.upsert({
            where: { id: set.id },
            update: { name: set.name, filter: set.filter || null },
            create: {
              id: set.id,
              catalogId: catalog.id,
              name: set.name,
              filter: set.filter || null,
            },
          });
          stats.productSets += 1;
        }

        for (const feed of catalog.product_feeds?.data || []) {
          await this.prisma.productFeed.upsert({
            where: { id: feed.id },
            update: {
              name: feed.name,
              country: feed.country || null,
              schedule: feed.schedule || null,
            },
            create: {
              id: feed.id,
              catalogId: catalog.id,
              name: feed.name,
              country: feed.country || null,
              schedule: feed.schedule || null,
            },
          });
          stats.productFeeds += 1;
        }
      }
      this.logger.log(
        `🛒 Catalogs: ${stats.catalogs} (sets ${stats.productSets}, feeds ${stats.productFeeds})`,
      );
    } catch (error) {
      const msg = `Catalog sync: ${parseMetaError(error).message}`;
      stats.errors.push(msg);
      ctx?.error(msg);
      this.logger.warn(`❌ ${msg}`);
    }
  }

  /**
   * Orchestrator cho cron daily: fanpage → account (+pixel+audience) → catalog.
   * Fanpage lấy trước vì payload của nó được nhồi vào `Account.pages`.
   */
  async syncAll(ctx?: BatchRunContext): Promise<EntitySyncStats> {
    this.init();
    const me = new User('me');

    const stats: EntitySyncStats = {
      accountsTotal: 0,
      accountsOk: 0,
      accountsFailed: 0,
      fanpages: 0,
      fanpagesWithWhatsapp: 0,
      fanpagesWithInstagram: 0,
      pixels: 0,
      audiences: 0,
      catalogs: 0,
      productSets: 0,
      productFeeds: 0,
      errors: [],
    };

    const pagePayloads = await this.syncFanpages(me, stats, ctx);
    await this.syncAccounts(me, pagePayloads, stats, ctx);
    await this.syncCatalogs(stats, ctx);

    return stats;
  }
}
