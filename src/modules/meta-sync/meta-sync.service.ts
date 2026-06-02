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

@Injectable()
export class MetaSyncService {
  private readonly logger = new Logger(MetaSyncService.name);

  constructor(private readonly prisma: PrismaService) {}

  private readonly hydrationMaxAgeMs = 6 * 60 * 60 * 1000;

  private async findIdsNeedingHydration(
    model: 'campaign' | 'adSet',
    ids: string[],
  ) {
    if (!ids.length) return [];

    const rows = (await (this.prisma[model] as any).findMany({
      where: { id: { in: ids } },
      select: { id: true, lastFetchedAt: true },
    })) as Array<{ id: string; lastFetchedAt: Date | null }>;

    const freshCutoff = Date.now() - this.hydrationMaxAgeMs;
    const rowMap = new Map(rows.map((row) => [row.id, row]));

    return ids.filter((id) => {
      const row = rowMap.get(id);
      if (!row?.lastFetchedAt) return true;
      return row.lastFetchedAt.getTime() < freshCutoff;
    });
  }

  private async splitChangedRows<
    T extends { id: string; remoteUpdatedAt?: Date | null },
  >(model: 'campaign' | 'adSet' | 'ad' | 'creative', rows: T[]) {
    if (!rows.length) return { creates: [] as T[], updates: [] as T[] };

    const existing = (await (this.prisma[model] as any).findMany({
      where: { id: { in: rows.map((row) => row.id) } },
      select: { id: true, remoteUpdatedAt: true },
    })) as Array<{ id: string; remoteUpdatedAt: Date | null }>;

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

  async syncCampaignData() {
    this.logger.log('⏰ Starting Batch Sync Campaign Data...');

    try {
      const accounts = await this.prisma.account.findMany({
        where: { needsReauth: false, accountType: 'AD_ACCOUNT' as any },
        select: { id: true, lastFetchedAt: true },
      });

      const limit = pLimit(4);

      const syncTasks = accounts.map((account) => {
        return limit(async () => {
          try {
            const adAccount = new AdAccount(account.id);
            const lookbackDays = Number(
              process.env.META_CORE_SYNC_LOOKBACK_DAYS || 14,
            );
            const overlapHours = Number(
              process.env.META_CORE_SYNC_OVERLAP_HOURS || 6,
            );
            const syncFrom = account.lastFetchedAt
              ? new Date(
                  account.lastFetchedAt.getTime() -
                    overlapHours * 60 * 60 * 1000,
                )
              : new Date(Date.now() - lookbackDays * 24 * 60 * 60 * 1000);
            const lastSyncUnix = Math.floor(syncFrom.getTime() / 1000);

            this.logger.log(
              `🔄 [${account.id}] Syncing data updated since ${dayjs.unix(lastSyncUnix).format('YYYY-MM-DD HH:mm:ss')}`,
            );

            const baseFilter = {
              limit: 50,
              filtering: [
                {
                  field: 'updated_time',
                  operator: 'GREATER_THAN',
                  value: lastSyncUnix,
                },
              ],
            };

            const adFields = [
              ...AD_FIELDS.filter((f) => f !== 'creative'),
              `creative{${CREATIVE_FIELDS.join(',')}}`,
            ];

            const [campaignsCursor, changedAdsetsCursor, changedAdsCursor] =
              await Promise.all([
                executeMetaApiWithRetry(
                  () =>
                    adAccount.getCampaigns(CAMPAIGN_FIELDS, baseFilter, true),
                  { logger: this.logger },
                ),
                executeMetaApiWithRetry(
                  () => adAccount.getAdSets(ADSET_FIELDS, baseFilter, true),
                  { logger: this.logger },
                ),
                executeMetaApiWithRetry(
                  () => adAccount.getAds(adFields, baseFilter, true),
                  { logger: this.logger },
                ),
              ]);

            const [allCampaigns, allAdSets, allAds] = await Promise.all([
              fetchAll(campaignsCursor),
              fetchAll(changedAdsetsCursor),
              fetchAll(changedAdsCursor),
            ]);
            const campaignIds = allCampaigns.map((c) => c.id);

            // If a campaign itself changed, hydrate its children so draft-copy UI
            // has a complete tree without waiting for child updated_time changes.
            if (campaignIds.length > 0) {
              for (const chunkIds of chunk(campaignIds, 50)) {
                const [asCursor, aCursor] = await Promise.all([
                  executeMetaApiWithRetry(
                    () =>
                      adAccount.getAdSets(
                        ADSET_FIELDS,
                        {
                          limit: 50,
                          filtering: [
                            {
                              field: 'campaign.id',
                              operator: 'IN',
                              value: chunkIds,
                            },
                          ],
                        },
                        true,
                      ),
                    { logger: this.logger },
                  ),
                  executeMetaApiWithRetry(
                    () =>
                      adAccount.getAds(
                        adFields,
                        {
                          limit: 50,
                          filtering: [
                            {
                              field: 'campaign.id',
                              operator: 'IN',
                              value: chunkIds,
                            },
                          ],
                        },
                        true,
                      ),
                    { logger: this.logger },
                  ),
                ]);

                const [asData, aData] = await Promise.all([
                  fetchAll(asCursor),
                  fetchAll(aCursor),
                ]);
                allAdSets.push(...asData);
                allAds.push(...aData);
              }
            }

            // Missing parent fetching logic
            const fetchedCampaignIds = new Set(allCampaigns.map((c) => c.id));
            const requiredCampaignIds = [
              ...new Set([
                ...allAdSets.map((as) => as.campaign_id),
                ...allAds.map((ad) => ad.campaign_id),
              ]),
            ].filter((id) => id && !fetchedCampaignIds.has(id));

            if (requiredCampaignIds.length > 0) {
              const missingCampaignIds = await this.findIdsNeedingHydration(
                'campaign',
                requiredCampaignIds,
              );

              if (missingCampaignIds.length > 0) {
                for (const chunkIds of chunk(missingCampaignIds, 50)) {
                  const cursor = await executeMetaApiWithRetry(() =>
                    adAccount.getCampaigns(
                      CAMPAIGN_FIELDS,
                      {
                        limit: 50,
                        filtering: [
                          { field: 'id', operator: 'IN', value: chunkIds },
                        ],
                      },
                      true,
                    ),
                  );
                  allCampaigns.push(...(await fetchAll(cursor)));
                }
              }
            }

            const fetchedAdSetIds = new Set(allAdSets.map((as) => as.id));
            const requiredAdSetIds = [
              ...new Set(allAds.map((ad) => ad.adset_id)),
            ].filter((id) => id && !fetchedAdSetIds.has(id));

            if (requiredAdSetIds.length > 0) {
              const missingAdSetIds = await this.findIdsNeedingHydration(
                'adSet',
                requiredAdSetIds,
              );

              if (missingAdSetIds.length > 0) {
                for (const chunkIds of chunk(missingAdSetIds, 50)) {
                  const cursor = await executeMetaApiWithRetry(
                    () =>
                      adAccount.getAdSets(
                        ADSET_FIELDS,
                        {
                          limit: 50,
                          filtering: [
                            { field: 'id', operator: 'IN', value: chunkIds },
                          ],
                        },
                        true,
                      ),
                    { logger: this.logger },
                  );
                  allAdSets.push(...(await fetchAll(cursor)));
                }
              }
            }

            await this.upsertFlatStructure(
              allCampaigns,
              allAdSets,
              allAds,
              account.id,
            );

            await this.prisma.account.update({
              where: { id: account.id },
              data: { lastFetchedAt: new Date() },
            });
          } catch (error) {
            this.logger.error(
              `❌ Account ${account.id}: ${parseMetaError(error).message}`,
            );
          }
        });
      });

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

    const campaignData = uniqueCampaigns.map((c) =>
      MetaTransformHelper.campaign(c, accountId),
    );
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

        if (!imageMap.has(key)) {
          imageMap.set(key, {
            id: key,
            hash: item.imageHash,
            accountId: item.accountId,
            url: item?.thumbnailUrl,
          });
          item.imageId = key;
        }
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
  }
}
