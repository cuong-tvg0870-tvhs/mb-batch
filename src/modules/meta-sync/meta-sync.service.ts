import {
  Injectable,
  InternalServerErrorException,
  Logger,
} from '@nestjs/common';
import dayjs from 'dayjs';
import { AdAccount } from 'facebook-nodejs-business-sdk';
import pLimit from 'p-limit';
import { MetaTransformHelper } from '../../common/helpers/meta-transform.helper';
import { PrismaBatchHelper } from '../../common/helpers/prisma-batch.helper';
import {
  chunk,
  executeMetaApiWithRetry,
  fetchAll,
  parseMetaError,
} from '../../common/utils';
import {
  AD_FIELDS,
  ADSET_FIELDS,
  CAMPAIGN_FIELDS,
  CREATIVE_FIELDS,
} from '../../common/utils/meta-field';
import { PrismaService } from '../prisma/prisma.service';
import { META_SYNC_CONFIG } from './meta-sync.constants';

/** Models whose freshness/change is tracked by id + timestamp columns. */
type CoreModel = 'campaign' | 'adSet' | 'ad' | 'creative';

/**
 * Narrow view over the four core Prisma delegates. They all expose id +
 * lastFetchedAt + remoteUpdatedAt, so the change/hydration queries can be typed
 * through one signature instead of an `any` cast at every call site.
 */
interface CoreModelDelegate {
  findMany(args: {
    where: { id: { in: string[] } };
    select: { id: true; lastFetchedAt?: true; remoteUpdatedAt?: true };
  }): Promise<
    Array<{ id: string; lastFetchedAt?: Date | null; remoteUpdatedAt?: Date | null }>
  >;
}

type EntityChanges<T> = { creates: T[]; updates: T[] };

@Injectable()
export class MetaSyncService {
  private readonly logger = new Logger(MetaSyncService.name);

  constructor(private readonly prisma: PrismaService) {}

  /** Ad fields plus the nested creative sub-tree, built once and reused. */
  private readonly adFieldsWithCreative = [
    ...AD_FIELDS.filter((f) => f !== 'creative'),
    `creative{${CREATIVE_FIELDS.join(',')}}`,
  ];

  private coreModel(model: CoreModel): CoreModelDelegate {
    return this.prisma[model] as unknown as CoreModelDelegate;
  }

  private async findIdsNeedingHydration(model: CoreModel, ids: string[]) {
    if (!ids.length) return [];

    const rows = await this.coreModel(model).findMany({
      where: { id: { in: ids } },
      select: { id: true, lastFetchedAt: true },
    });

    const freshCutoff = Date.now() - META_SYNC_CONFIG.hydrationMaxAgeMs;
    const rowMap = new Map(rows.map((row) => [row.id, row]));

    return ids.filter((id) => {
      const row = rowMap.get(id);
      if (!row?.lastFetchedAt) return true;
      return row.lastFetchedAt.getTime() < freshCutoff;
    });
  }

  private async splitChangedRows<
    T extends { id: string; remoteUpdatedAt?: Date | null },
  >(model: CoreModel, rows: T[]): Promise<EntityChanges<T>> {
    if (!rows.length) return { creates: [], updates: [] };

    const existing = await this.coreModel(model).findMany({
      where: { id: { in: rows.map((row) => row.id) } },
      select: { id: true, remoteUpdatedAt: true },
    });

    const existingMap = new Map(existing.map((row) => [row.id, row]));
    const creates: T[] = [];
    const updates: T[] = [];

    for (const row of rows) {
      const current = existingMap.get(row.id);
      if (!current) {
        creates.push(row);
        continue;
      }

      if (
        row.remoteUpdatedAt &&
        current.remoteUpdatedAt &&
        current.remoteUpdatedAt.getTime() >= row.remoteUpdatedAt.getTime()
      ) {
        continue;
      }

      updates.push(row);
    }

    return { creates, updates };
  }

  /** Fetch campaigns/adsets/ads changed since the given unix timestamp. */
  private async fetchChangedSince(adAccount: AdAccount, sinceUnix: number) {
    const baseFilter = {
      limit: META_SYNC_CONFIG.pageLimit,
      filtering: [
        { field: 'updated_time', operator: 'GREATER_THAN', value: sinceUnix },
      ],
    };

    const [campaignsCursor, adsetsCursor, adsCursor] = await Promise.all([
      executeMetaApiWithRetry(
        () => adAccount.getCampaigns(CAMPAIGN_FIELDS, baseFilter, true),
        { logger: this.logger },
      ),
      executeMetaApiWithRetry(
        () => adAccount.getAdSets(ADSET_FIELDS, baseFilter, true),
        { logger: this.logger },
      ),
      executeMetaApiWithRetry(
        () => adAccount.getAds(this.adFieldsWithCreative, baseFilter, true),
        { logger: this.logger },
      ),
    ]);

    const [campaigns, adsets, ads] = await Promise.all([
      fetchAll(campaignsCursor),
      fetchAll(adsetsCursor),
      fetchAll(adsCursor),
    ]);

    return { campaigns, adsets, ads };
  }

  /**
   * Fetch all adsets + ads belonging to the given campaigns. Used when a
   * campaign itself changed, so the draft-copy UI gets a complete tree without
   * waiting for each child's own updated_time to move.
   */
  private async hydrateChildrenOf(adAccount: AdAccount, campaignIds: string[]) {
    const adsets: any[] = [];
    const ads: any[] = [];

    for (const chunkIds of chunk(campaignIds, META_SYNC_CONFIG.idChunkSize)) {
      const filter = {
        limit: META_SYNC_CONFIG.pageLimit,
        filtering: [{ field: 'campaign.id', operator: 'IN', value: chunkIds }],
      };

      const [adsetsCursor, adsCursor] = await Promise.all([
        executeMetaApiWithRetry(
          () => adAccount.getAdSets(ADSET_FIELDS, filter, true),
          { logger: this.logger },
        ),
        executeMetaApiWithRetry(
          () => adAccount.getAds(this.adFieldsWithCreative, filter, true),
          { logger: this.logger },
        ),
      ]);

      const [adsetsData, adsData] = await Promise.all([
        fetchAll(adsetsCursor),
        fetchAll(adsCursor),
      ]);
      adsets.push(...adsetsData);
      ads.push(...adsData);
    }

    return { adsets, ads };
  }

  /** Fetch campaigns or adsets by explicit id list (parent backfill). */
  private async hydrateByIds(
    adAccount: AdAccount,
    entity: 'campaign' | 'adSet',
    ids: string[],
  ) {
    const rows: any[] = [];

    for (const chunkIds of chunk(ids, META_SYNC_CONFIG.idChunkSize)) {
      const filter = {
        limit: META_SYNC_CONFIG.pageLimit,
        filtering: [{ field: 'id', operator: 'IN', value: chunkIds }],
      };

      const cursor = await executeMetaApiWithRetry(
        () =>
          entity === 'campaign'
            ? adAccount.getCampaigns(CAMPAIGN_FIELDS, filter, true)
            : adAccount.getAdSets(ADSET_FIELDS, filter, true),
        { logger: this.logger },
      );

      rows.push(...(await fetchAll(cursor)));
    }

    return rows;
  }

  /**
   * Backfill any adset referenced by a fetched ad but not yet present. Mutates
   * `allAdSets` in place. Runs BEFORE campaign backfill so a freshly hydrated
   * adset can contribute its campaign_id to the campaign requirement set.
   */
  private async hydrateMissingAdSets(
    adAccount: AdAccount,
    allAdSets: any[],
    allAds: any[],
  ) {
    const fetched = new Set(allAdSets.map((as) => as.id));
    const required = [...new Set(allAds.map((ad) => ad.adset_id))].filter(
      (id) => id && !fetched.has(id),
    );
    if (!required.length) return;

    const missing = await this.findIdsNeedingHydration('adSet', required);
    if (!missing.length) return;

    allAdSets.push(...(await this.hydrateByIds(adAccount, 'adSet', missing)));
  }

  /**
   * Backfill any campaign referenced by a fetched adset/ad but not yet present.
   * Mutates `allCampaigns` in place. Guarantees every adset's parent campaign
   * exists before upsert, so the adset FK never fails.
   */
  private async hydrateMissingCampaigns(
    adAccount: AdAccount,
    allCampaigns: any[],
    allAdSets: any[],
    allAds: any[],
  ) {
    const fetched = new Set(allCampaigns.map((c) => c.id));
    const required = [
      ...new Set([
        ...allAdSets.map((as) => as.campaign_id),
        ...allAds.map((ad) => ad.campaign_id),
      ]),
    ].filter((id) => id && !fetched.has(id));
    if (!required.length) return;

    const missing = await this.findIdsNeedingHydration('campaign', required);
    if (!missing.length) return;

    allCampaigns.push(
      ...(await this.hydrateByIds(adAccount, 'campaign', missing)),
    );
  }

  async syncCampaignData() {
    this.logger.log('⏰ Starting Batch Sync Campaign Data...');

    try {
      const accounts = await this.prisma.account.findMany({
        where: { needsReauth: false, accountType: 'AD_ACCOUNT' as any },
        select: { id: true, lastFetchedAt: true },
      });

      const limit = pLimit(META_SYNC_CONFIG.accountConcurrency);

      const syncTasks = accounts.map((account) =>
        limit(async () => {
          try {
            const adAccount = new AdAccount(account.id);
            const syncFrom = account.lastFetchedAt
              ? new Date(
                  account.lastFetchedAt.getTime() -
                    META_SYNC_CONFIG.overlapHours * 60 * 60 * 1000,
                )
              : new Date(
                  Date.now() -
                    META_SYNC_CONFIG.lookbackDays * 24 * 60 * 60 * 1000,
                );
            const lastSyncUnix = Math.floor(syncFrom.getTime() / 1000);

            this.logger.log(
              `🔄 [${account.id}] Syncing data updated since ${dayjs
                .unix(lastSyncUnix)
                .format('YYYY-MM-DD HH:mm:ss')}`,
            );

            const {
              campaigns: allCampaigns,
              adsets: allAdSets,
              ads: allAds,
            } = await this.fetchChangedSince(adAccount, lastSyncUnix);

            // If a campaign itself changed, hydrate its children so draft-copy
            // UI has a complete tree without waiting for child updates.
            const changedCampaignIds = allCampaigns.map((c) => c.id);
            if (changedCampaignIds.length > 0) {
              const children = await this.hydrateChildrenOf(
                adAccount,
                changedCampaignIds,
              );
              allAdSets.push(...children.adsets);
              allAds.push(...children.ads);
            }

            // Backfill missing parents in dependency order: adsets first (so a
            // hydrated adset's campaign_id is known), then campaigns. This keeps
            // every FK target present before the upsert.
            await this.hydrateMissingAdSets(adAccount, allAdSets, allAds);
            await this.hydrateMissingCampaigns(
              adAccount,
              allCampaigns,
              allAdSets,
              allAds,
            );

            const summary = await this.upsertFlatStructure(
              allCampaigns,
              allAdSets,
              allAds,
              account.id,
            );

            await this.prisma.account.update({
              where: { id: account.id },
              data: { lastFetchedAt: new Date() },
            });

            this.logger.log(
              `✅ [${account.id}] campaigns +${summary.campaigns.created}/~${summary.campaigns.updated}, ` +
                `adsets +${summary.adsets.created}/~${summary.adsets.updated}, ` +
                `ads +${summary.ads.created}/~${summary.ads.updated}, ` +
                `creatives +${summary.creatives.created}/~${summary.creatives.updated}, ` +
                `images +${summary.images}, videos +${summary.videos}`,
            );
          } catch (error) {
            this.logger.error(
              `❌ Account ${account.id}: ${parseMetaError(error).message}`,
            );
          }
        }),
      );

      await Promise.all(syncTasks);
      this.logger.log('✅ Batch Sync Campaign Data Completed.');
    } catch (err) {
      this.logger.error('🔥 Critical Sync Failure', err);
      throw new InternalServerErrorException(parseMetaError(err));
    }
  }

  async upsertFlatStructure(
    campaigns: any[],
    adsets: any[],
    ads: any[],
    accountId: string,
  ) {
    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const uniqueCampaigns = Array.from(
      new Map(campaigns.map((c) => [c.id, c])).values(),
    );
    const uniqueAdSets = Array.from(
      new Map(adsets.map((as) => [as.id, as])).values(),
    );
    const uniqueAds = Array.from(
      new Map(ads.map((ad) => [ad.id, ad])).values(),
    );

    // Link synced campaigns to system campaigns by meta_id
    const systemCampaigns = uniqueCampaigns.length
      ? await this.prisma.systemCampaign.findMany({
          where: { meta_id: { in: uniqueCampaigns.map((c) => c.id) } },
          select: { id: true, meta_id: true },
        })
      : [];
    const systemCampaignMap = new Map(
      systemCampaigns.map((sc) => [sc.meta_id, sc.id]),
    );

    const campaignData = uniqueCampaigns.map((c) => {
      const mapped = MetaTransformHelper.campaign(c, accountId);
      const systemCampaignId = systemCampaignMap.get(c.id);
      if (systemCampaignId) {
        mapped.systemCampaignId = systemCampaignId;
      }
      return mapped;
    });
    const adsetData = uniqueAdSets.map((as) =>
      MetaTransformHelper.adset(as, accountId, as.campaign_id),
    );
    const adData = uniqueAds.map((ad) =>
      MetaTransformHelper.ad(ad, accountId, ad.campaign_id, ad.adset_id),
    );

    const creativeData = [];
    for (const ad of uniqueAds) {
      const creative = MetaTransformHelper.creative(ad, accountId);
      if (creative) creativeData.push(creative);
    }

    const uniqueCreatives = Array.from(
      new Map(creativeData.map((c) => [c.id, c])).values(),
    );

    const [campaignChanges, adsetChanges, creativeChanges, adChanges] =
      await Promise.all([
        this.splitChangedRows('campaign', campaignData),
        this.splitChangedRows('adSet', adsetData),
        this.splitChangedRows('creative', uniqueCreatives),
        this.splitChangedRows('ad', adData),
      ]);

    const pageIds = [
      ...new Set(uniqueCreatives.map((c) => c.pageId).filter(Boolean)),
    ];

    const videoMap = new Map<string, any>();
    const imageMap = new Map<string, any>();

    const fanpages = await this.prisma.fanpage.findMany({
      where: { id: { in: pageIds } },
    });

    const fanpageMap = new Map(fanpages.map((f) => [f.id, f]));

    for (const item of uniqueCreatives) {
      if (item.pageId && fanpageMap.has(item.pageId)) {
        item.systemPageId = item.pageId;
      }

      if (item.videoId && !videoMap.has(item.videoId)) {
        videoMap.set(item.videoId, {
          id: item.videoId,
          accountId: item.accountId,
          thumbnailUrl: item?.thumbnailUrl,
        });
      }

      if (item.imageHash) {
        const key = `${(item.accountId as string).replaceAll('act_', '')}:${
          item.imageHash
        }`;

        // Dedup the AdImage row, but always link THIS creative to it — every
        // creative sharing an image hash must carry the imageId FK, not just
        // the first one seen in the batch.
        if (!imageMap.has(key)) {
          imageMap.set(key, {
            id: key,
            hash: item.imageHash,
            accountId: item.accountId,
            url: item?.thumbnailUrl,
          });
        }
        item.imageId = key;
      }
    }

    const newVideos = Array.from(videoMap.values());
    const newImages = Array.from(imageMap.values());

    await prismaHelper.createManySafe(this.prisma.adImage, newImages, 20);
    await prismaHelper.createManySafe(this.prisma.adVideo, newVideos, 20);

    await prismaHelper.createManySafe(
      this.prisma.campaign,
      campaignChanges.creates,
      100,
    );
    await prismaHelper.updateManyById(campaignChanges.updates, (item) =>
      this.prisma.campaign.update({ where: { id: item.id }, data: item }),
    );

    await prismaHelper.createManySafe(
      this.prisma.adSet,
      adsetChanges.creates,
      100,
    );
    await prismaHelper.updateManyById(adsetChanges.updates, (item) =>
      this.prisma.adSet.update({ where: { id: item.id }, data: item }),
    );

    await prismaHelper.createManySafe(
      this.prisma.creative,
      creativeChanges.creates,
      100,
    );
    await prismaHelper.updateManyById(creativeChanges.updates, (item) =>
      this.prisma.creative.update({ where: { id: item.id }, data: item }),
    );

    await prismaHelper.createManySafe(this.prisma.ad, adChanges.creates, 100);
    await prismaHelper.updateManyById(adChanges.updates, (item) =>
      this.prisma.ad.update({ where: { id: item.id }, data: item }),
    );

    // Auto-map synced creatives with CreativeAssets (by hash or videoId)
    const creativesToMap = uniqueCreatives.filter(
      (c) => c.imageHash || c.videoId,
    );
    if (creativesToMap.length > 0) {
      const imageHashesToFind = creativesToMap
        .map((c) => c.imageHash)
        .filter(Boolean) as string[];
      const videoIdsToFind = creativesToMap
        .map((c) => c.videoId)
        .filter(Boolean) as string[];

      const matchedAssets = await this.prisma.creativeAsset.findMany({
        where: {
          OR: [
            imageHashesToFind.length > 0
              ? { imageHash: { in: imageHashesToFind } }
              : undefined,
            videoIdsToFind.length > 0
              ? { video_id: { in: videoIdsToFind } }
              : undefined,
          ].filter(Boolean) as any,
        },
        select: { id: true, imageHash: true, video_id: true },
      });

      if (matchedAssets.length > 0) {
        const mappingData: Array<{ creativeId: string; creativeAssetId: string }> = [];

        for (const creative of creativesToMap) {
          for (const asset of matchedAssets) {
            let isMatch = false;
            if (creative.imageHash && asset.imageHash === creative.imageHash) {
              isMatch = true;
            }
            if (creative.videoId && asset.video_id === creative.videoId) {
              isMatch = true;
            }

            if (isMatch) {
              mappingData.push({
                creativeId: creative.id,
                creativeAssetId: asset.id,
              });
            }
          }
        }

        if (mappingData.length > 0) {
          await this.prisma.creativeAssetMapping.createMany({
            data: mappingData,
            skipDuplicates: true,
          });
        }
      }
    }

    return {
      campaigns: {
        created: campaignChanges.creates.length,
        updated: campaignChanges.updates.length,
      },
      adsets: {
        created: adsetChanges.creates.length,
        updated: adsetChanges.updates.length,
      },
      ads: {
        created: adChanges.creates.length,
        updated: adChanges.updates.length,
      },
      creatives: {
        created: creativeChanges.creates.length,
        updated: creativeChanges.updates.length,
      },
      images: newImages.length,
      videos: newVideos.length,
    };
  }
}
