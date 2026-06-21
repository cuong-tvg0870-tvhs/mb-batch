import { Injectable, Logger } from '@nestjs/common';
import { AccountType, Prisma } from '@prisma/client';
import { AdAccount, FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import { MetaTransformHelper } from '../../common/helpers/meta-transform.helper';
import { PrismaBatchHelper } from '../../common/helpers/prisma-batch.helper';
import {
  chunk,
  executeMetaApiWithRetry,
  fetchAll,
  parseMetaError,
  parseMetaUrlExpireTime,
} from '../../common/utils';
import { CREATIVE_FIELDS } from '../../common/utils/meta-field';
import { PrismaService } from '../prisma/prisma.service';

type CreativeCandidate = {
  id: string;
  accountId: string | null;
  imageUrl: string | null;
  thumbnailUrl: string | null;
  previewUrl: string | null;
  urlExpiredAt: Date | null;
  rawPayload: Prisma.JsonValue | null;
};

type RefreshOptions = {
  limit?: number;
  refreshBeforeHours?: number;
  minRefetchHours?: number;
};

@Injectable()
export class CreativeRefreshService {
  private readonly logger = new Logger(CreativeRefreshService.name);

  constructor(private readonly prisma: PrismaService) {}

  async recalculateCreativeUrlExpired(limit = this.getRecalculateLimit()) {
    const creatives = await this.prisma.creative.findMany({
      where: {
        accountId: { not: null },
        OR: [
          { thumbnailUrl: { not: null } },
          { previewUrl: { not: null } },
          { imageUrl: { not: null } },
        ],
      },
      select: {
        id: true,
        accountId: true,
        imageUrl: true,
        thumbnailUrl: true,
        previewUrl: true,
        urlExpiredAt: true,
        rawPayload: true,
      },
      orderBy: [{ urlExpiredAt: 'asc' }, { updatedAt: 'desc' }],
      take: limit,
    });

    let updated = 0;

    for (const creative of creatives) {
      const urlExpiredAt = this.resolveCreativeUrlExpiredAt(creative);

      if (!this.datesEqual(creative.urlExpiredAt, urlExpiredAt)) {
        await this.prisma.creative.update({
          where: { id: creative.id },
          data: { urlExpiredAt, updatedAt: new Date() },
        });
        updated++;
      }
    }

    this.logger.log(
      `Creative URL expiry recalculated: checked=${creatives.length}, updated=${updated}`,
    );

    return { checked: creatives.length, updated };
  }

  async refreshExpiringCreatives(options?: RefreshOptions) {
    this.initMetaSdk();

    const now = new Date();
    const limit = options?.limit ?? this.getRefreshLimit();
    const refreshBeforeHours =
      options?.refreshBeforeHours ?? this.getRefreshBeforeHours();
    const minRefetchHours =
      options?.minRefetchHours ?? this.getMinRefetchHours();
    const cutoff = new Date(now.getTime() + refreshBeforeHours * 60 * 60_000);
    const staleBefore = new Date(now.getTime() - minRefetchHours * 60 * 60_000);

    const candidates = await this.prisma.creative.findMany({
      where: {
        accountId: { not: null },
        account: {
          is: {
            needsReauth: false,
            accountType: AccountType.AD_ACCOUNT,
          },
        },
        OR: [
          { urlExpiredAt: null },
          { urlExpiredAt: { lte: cutoff } },
          { thumbnailUrl: null },
          { previewUrl: null },
          { imageUrl: null },
        ],
        AND: [
          {
            OR: [
              { urlExpiredAt: { lte: cutoff } },
              { lastFetchedAt: null },
              { lastFetchedAt: { lte: staleBefore } },
            ],
          },
        ],
      },
      select: {
        id: true,
        accountId: true,
        imageUrl: true,
        thumbnailUrl: true,
        previewUrl: true,
        urlExpiredAt: true,
        rawPayload: true,
      },
      orderBy: [{ urlExpiredAt: 'asc' }, { lastFetchedAt: 'asc' }],
      take: limit,
    });

    if (candidates.length === 0) {
      this.logger.log('No expiring or incomplete Creative records to refresh');
      return { candidates: 0, refreshed: 0, missing: 0, failedAccounts: 0 };
    }

    const byAccount = this.groupByAccount(candidates);
    let refreshed = 0;
    let missing = 0;
    let failedAccounts = 0;

    for (const [accountId, accountCreatives] of byAccount.entries()) {
      try {
        const result = await this.refreshAccountCreatives(
          accountId,
          accountCreatives.map((creative) => creative.id),
        );

        refreshed += result.refreshed;
        missing += result.missing;
      } catch (error: any) {
        failedAccounts++;
        const parsed = parseMetaError(error);
        this.logger.error(
          `[${accountId}] Failed to refresh creatives: ${parsed.message}`,
          parsed.raw,
        );
      }
    }

    this.logger.log(
      `Creative refresh finished: candidates=${candidates.length}, refreshed=${refreshed}, missing=${missing}, failedAccounts=${failedAccounts}`,
    );

    return { candidates: candidates.length, refreshed, missing, failedAccounts };
  }

  private async refreshAccountCreatives(accountId: string, creativeIds: string[]) {
    const prismaHelper = new PrismaBatchHelper(this.prisma);
    let refreshed = 0;
    let missing = 0;

    for (const ids of chunk([...new Set(creativeIds)], 50)) {
      const api = FacebookAdsApi.getDefaultApi();
      const response = (await executeMetaApiWithRetry(
        () =>
          api.call('GET', [], {
            ids: ids.join(','),
            fields: CREATIVE_FIELDS.join(','),
          }),
        {
          logger: this.logger,
          context: { accountId, ids },
        },
      )) as any;

      const rawCreatives = Object.values(response || {}) as any[];
      const returnedIds = new Set(rawCreatives.map((creative) => creative.id));
      const missingIds = ids.filter((id) => !returnedIds.has(id));

      if (missingIds.length > 0) {
        missing += missingIds.length;
        await this.prisma.creative.updateMany({
          where: { id: { in: missingIds } },
          data: { lastFetchedAt: new Date(), updatedAt: new Date() },
        });
        this.logger.warn(
          `[${accountId}] Meta returned no data for creatives: ${missingIds.join(', ')}`,
        );
      }

      const mappedCreatives = rawCreatives
        .map((creative) =>
          MetaTransformHelper.creative({ creative }, accountId),
        )
        .filter(Boolean);

      if (mappedCreatives.length === 0) continue;

      await this.prepareCreativeRelations(mappedCreatives, prismaHelper);

      await prismaHelper.updateManyById(
        mappedCreatives,
        (creative) =>
          this.prisma.creative.update({
            where: { id: creative.id },
            data: this.toCreativeUpdateData(creative),
          }),
        10,
      );

      refreshed += mappedCreatives.length;
    }

    return { refreshed, missing };
  }

  private async prepareCreativeRelations(
    creatives: any[],
    prismaHelper: PrismaBatchHelper,
  ) {
    const pageIds = [
      ...new Set(creatives.map((creative) => creative.pageId).filter(Boolean)),
    ];
    const fanpages = pageIds.length
      ? await this.prisma.fanpage.findMany({
          where: { id: { in: pageIds } },
          select: { id: true },
        })
      : [];
    const fanpageIds = new Set(fanpages.map((fanpage) => fanpage.id));
    const videoMap = new Map<string, any>();
    const imageMap = new Map<string, any>();

    for (const creative of creatives) {
      if (creative.pageId && fanpageIds.has(creative.pageId)) {
        creative.systemPageId = creative.pageId;
      }

      if (creative.videoId && !videoMap.has(creative.videoId)) {
        videoMap.set(creative.videoId, {
          id: creative.videoId,
          accountId: this.normalizeAccountId(creative.accountId),
          thumbnailUrl: creative.thumbnailUrl || creative.previewUrl || null,
          lastFetchedAt: new Date(),
        });
      }

      const imageKey = this.getImageKey(creative.accountId, creative.imageHash);
      if (imageKey && !imageMap.has(imageKey)) {
        imageMap.set(imageKey, {
          id: imageKey,
          hash: creative.imageHash,
          accountId: this.normalizeAccountId(creative.accountId),
          url:
            creative.imageUrl ||
            creative.thumbnailUrl ||
            creative.previewUrl ||
            null,
          urlExpiredAt: this.resolveCreativeUrlExpiredAt(creative),
        });
      }

      creative.imageId = imageKey;
    }

    await prismaHelper.createManySafe(
      this.prisma.adVideo,
      Array.from(videoMap.values()),
      20,
    );
    await prismaHelper.createManySafe(
      this.prisma.adImage,
      Array.from(imageMap.values()),
      20,
    );
  }

  private toCreativeUpdateData(creative: any): Prisma.CreativeUncheckedUpdateInput {
    const urlExpiredAt = this.resolveCreativeUrlExpiredAt(creative);

    return {
      accountId: this.normalizeAccountId(creative.accountId),
      pageId: creative.pageId || null,
      postId: creative.postId || null,
      objectStoryId: creative.objectStoryId || null,
      effectObjectStoryId: creative.effectObjectStoryId || null,
      name: creative.name || null,
      creativeType: creative.creativeType || null,
      imageHash: creative.imageHash || null,
      imageId: creative.imageId || null,
      imageUrl: creative.imageUrl || null,
      videoId: creative.videoId || null,
      thumbnailUrl: creative.thumbnailUrl || null,
      previewUrl: creative.previewUrl || null,
      remoteUpdatedAt: creative.remoteUpdatedAt || null,
      lastFetchedAt: new Date(),
      rawPayload: creative.rawPayload || Prisma.JsonNull,
      urlExpiredAt,
      updatedAt: new Date(),
      ...(creative.systemPageId ? { systemPageId: creative.systemPageId } : {}),
    };
  }

  private resolveCreativeUrlExpiredAt(creative: Partial<CreativeCandidate>) {
    return parseMetaUrlExpireTime(this.collectCreativeUrls(creative));
  }

  private collectCreativeUrls(value: unknown) {
    const urls = new Set<string>();
    this.collectUrlStrings(value, urls);
    return [...urls];
  }

  private collectUrlStrings(value: unknown, urls: Set<string>) {
    if (!value) return;

    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (/^https?:\/\//i.test(trimmed)) urls.add(trimmed);
      return;
    }

    if (Array.isArray(value)) {
      for (const item of value) this.collectUrlStrings(item, urls);
      return;
    }

    if (typeof value === 'object') {
      for (const item of Object.values(value as Record<string, unknown>)) {
        this.collectUrlStrings(item, urls);
      }
    }
  }

  private groupByAccount(candidates: CreativeCandidate[]) {
    const byAccount = new Map<string, CreativeCandidate[]>();

    for (const candidate of candidates) {
      const accountId = this.normalizeAccountId(candidate.accountId);
      if (!accountId) continue;

      const items = byAccount.get(accountId) || [];
      items.push(candidate);
      byAccount.set(accountId, items);
    }

    return byAccount;
  }

  private initMetaSdk() {
    const token = process.env.SDK_FACEBOOK_ACCESS_TOKEN;
    if (!token) {
      throw new Error('SDK_FACEBOOK_ACCESS_TOKEN missing');
    }

    FacebookAdsApi.init(token);
  }

  private normalizeAccountId(accountId?: string | null) {
    if (!accountId) return null;
    return accountId.startsWith('act_') ? accountId : `act_${accountId}`;
  }

  private getImageKey(accountId?: string | null, hash?: string | null) {
    const normalizedAccountId = this.normalizeAccountId(accountId);
    if (!normalizedAccountId || !hash) return null;
    return `${normalizedAccountId}:${hash}`;
  }

  private datesEqual(a?: Date | null, b?: Date | null) {
    if (!a && !b) return true;
    if (!a || !b) return false;
    return a.getTime() === b.getTime();
  }

  private getRefreshLimit() {
    return Number(process.env.CREATIVE_REFRESH_LIMIT || 200);
  }

  private getRecalculateLimit() {
    return Number(process.env.CREATIVE_EXPIRE_RECALC_LIMIT || 1000);
  }

  private getRefreshBeforeHours() {
    return Number(process.env.CREATIVE_REFRESH_BEFORE_HOURS || 24);
  }

  private getMinRefetchHours() {
    return Number(process.env.CREATIVE_MIN_REFETCH_HOURS || 6);
  }
}
