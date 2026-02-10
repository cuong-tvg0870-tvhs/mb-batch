import { Injectable } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { AdAccount, AdVideo } from 'facebook-nodejs-business-sdk';
import {
  MetaAd,
  MetaAdSet,
  MetaCampaignTree,
  MetaCreative,
} from '../../common/dtos/types.dto';
import {
  extractCampaignMetrics,
  fetchAll,
  toPrismaJson,
} from '../../common/utils';
import {
  AD_IMAGE_FIELDS,
  AD_VIDEO_FIELDS,
} from '../../common/utils/meta-field';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class UpsertService {
  constructor(private prisma: PrismaService) {}

  async syncCampaignTree(
    accountId: string,
    adAccount: AdAccount,
    data: MetaCampaignTree,
  ) {
    return this.prisma.$transaction(async (tx) => {
      await this.upsertCampaign(tx, accountId, data);

      for (const adset of data.adsets ?? []) {
        await this.upsertAdSet(tx, accountId, data.id, adset);

        for (const ad of adset.ads ?? []) {
          await this.syncAdAssetsLegacy(tx, adAccount, accountId, ad);
          await this.upsertCreativeLegacy(tx, accountId, ad);
          await this.syncCidLegacy(tx, accountId, ad);
          await this.upsertAdLegacy(tx, accountId, data.id, adset.id, ad);
        }
      }
    });
  }

  /* ======================= CAMPAIGN ======================= */

  upsertCampaign(
    tx: Prisma.TransactionClient,
    accountId: string,
    c: MetaCampaignTree,
  ) {
    return tx.campaign.upsert({
      where: { id: c.id },
      update: {
        name: c.name,
        status: c.status,
        objective: c.objective,
        buyingType: c.buying_type,
        effectiveBudget: Number(c.daily_budget ?? c.lifetime_budget ?? 0),
        dailyBudget: Number(c.daily_budget),
        lifetimeBudget: Number(c.lifetime_budget),
        rawPayload: toPrismaJson(c),
        lastFetchedAt: new Date(),
        createdAt: c.created_time ? new Date(c.created_time) : undefined,
        updatedAt: c.updated_time ? new Date(c.updated_time) : undefined,
        systemCampaignId: c.systemCampaignId || undefined,

        ...(c?.insights?.data && Number(c?.insights?.data?.length) > 0
          ? extractCampaignMetrics(c.insights.data[0])
          : {}),
      },
      create: {
        id: c.id,
        accountId,
        name: c.name,
        status: c.status,
        objective: c.objective,
        buyingType: c.buying_type,
        dailyBudget: Number(c.daily_budget),
        effectiveBudget: Number(c.daily_budget ?? c.lifetime_budget ?? 0),
        lifetimeBudget: Number(c.lifetime_budget),
        rawPayload: toPrismaJson(c),
        lastFetchedAt: new Date(),
        createdAt: c.created_time ? new Date(c.created_time) : undefined,
        updatedAt: c.updated_time ? new Date(c.updated_time) : undefined,

        systemCampaignId: c.systemCampaignId || undefined,

        ...(c?.insights?.data && Number(c?.insights?.data?.length) > 0
          ? extractCampaignMetrics(c.insights.data[0])
          : {}),
      },
    });
  }

  /* ======================= ADSET ======================= */

  upsertAdSet(
    tx: Prisma.TransactionClient,
    accountId: string,
    campaignId: string,
    as: MetaAdSet,
  ) {
    return tx.adSet.upsert({
      where: { id: as.id },
      update: {
        name: as.name,
        status: as.status,
        optimizationGoal: as.optimization_goal,
        billingEvent: as.billing_event,
        bidStrategy: as.bid_strategy,
        dailyBudget: Number(as.daily_budget),
        lifetimeBudget: Number(as.lifetime_budget),
        targeting: as.targeting,
        rawPayload: toPrismaJson(as),
        lastFetchedAt: new Date(),
        createdAt: as.created_time ? new Date(as.created_time) : undefined,
        updatedAt: as.updated_time ? new Date(as.updated_time) : undefined,
        effectiveBudget: Number(as?.daily_budget || as?.lifetime_budget || 0),
        ...(as?.insights?.data && Number(as?.insights?.data?.length) > 0
          ? extractCampaignMetrics(as.insights.data[0])
          : {}),
      },
      create: {
        id: as.id,
        accountId,
        campaignId,
        name: as.name,
        status: as.status,
        optimizationGoal: as.optimization_goal,
        billingEvent: as.billing_event,
        bidStrategy: as.bid_strategy,
        dailyBudget: Number(as.daily_budget),
        lifetimeBudget: Number(as.lifetime_budget),
        effectiveBudget: Number(as?.daily_budget || as?.lifetime_budget || 0),
        targeting: as.targeting,
        rawPayload: toPrismaJson(as),
        lastFetchedAt: new Date(),
        createdAt: as.created_time ? new Date(as.created_time) : undefined,
        updatedAt: as.updated_time ? new Date(as.updated_time) : undefined,

        ...(as?.insights?.data && Number(as?.insights?.data?.length) > 0
          ? extractCampaignMetrics(as.insights.data[0])
          : {}),
      },
    });
  }

  /* ======================= ASSETS ======================= */

  async syncAdAssetsLegacy(
    tx: Prisma.TransactionClient,
    adAccount: AdAccount,
    accountId: string,
    ad: MetaAd,
  ) {
    /** IMAGE */
    if (ad.creative?.image_hash) {
      const exists = await tx.adImage.findFirst({
        where: { hash: ad.creative.image_hash },
      });

      if (!exists) {
        const cursor = await adAccount.getAdImages(
          AD_IMAGE_FIELDS,
          { hashes: [ad.creative.image_hash] },
          true,
        );

        const image = (await fetchAll(cursor))[0];
        if (image) {
          await tx.adImage.upsert({
            where: {
              accountId_hash_id: {
                id: image.id,
                accountId,
                hash: image.hash,
              },
            },
            update: {
              name: image.name,
              url: image.permalink_url || image.url,
              permalink_url: image.permalink_url,
              createdTime: image.created_time,
              height: image.height,
              width: image.width,
              rawPayload: toPrismaJson(image),
              status: image.status,
              createdAt: image.created_time
                ? new Date(image.created_time)
                : undefined,
              updatedAt: image.updated_time
                ? new Date(image.updated_time)
                : undefined,
            },
            create: {
              id: image.id,
              accountId,
              hash: image.hash,
              name: image.name,
              url: image.permalink_url || image.url,
              permalink_url: image.permalink_url,
              createdTime: image.created_time,
              height: image.height,
              width: image.width,
              rawPayload: toPrismaJson(image),
              status: image.status,
              createdAt: image.created_time
                ? new Date(image.created_time)
                : undefined,
              updatedAt: image.updated_time
                ? new Date(image.updated_time)
                : undefined,
            },
          });
        }
      }
    }

    /** VIDEO */
    if (ad.creative?.video_id) {
      const exists = await tx.adVideo.findFirst({
        where: { id: ad.creative.video_id },
      });

      if (!exists) {
        const videoCursor = await new AdVideo(ad.creative.video_id).read(
          AD_VIDEO_FIELDS,
        );

        const uploadResult = videoCursor._data;
        await this.prisma.adVideo.upsert({
          where: { id: uploadResult.id },
          create: {
            id: uploadResult.id,
            title: uploadResult?.title,
            accountId: adAccount?.id,
            source:
              uploadResult.source ||
              `https://facebook.com/${uploadResult.permalink_url}`,
            status: uploadResult?.status?.video_status,
            thumbnailUrl: uploadResult?.source || uploadResult?.picture,
            length: uploadResult.length,

            rawPayload: uploadResult,
          },
          update: {
            title: uploadResult?.title,
            accountId: adAccount?.id,
            source:
              uploadResult.source ||
              `https://facebook.com/${uploadResult.permalink_url}`,
            status: uploadResult?.status?.video_status,
            thumbnailUrl: uploadResult?.source || uploadResult?.picture,
            length: uploadResult.length,

            rawPayload: uploadResult,
          },
        });
      }
    }
  }

  /* ======================= CREATIVE ======================= */

  async upsertCreativeLegacy(
    tx: Prisma.TransactionClient,
    accountId: string,
    ad: MetaCreative,
  ) {
    if (!ad.creative?.id) return;
    const creative = ad.creative;

    const pageId = creative?.effective_object_story_id?.split('_')[0];
    const postId = creative?.effective_object_story_id?.split('_')[1];

    const fanpage = pageId
      ? await tx.fanpage.findUnique({ where: { id: pageId } })
      : null;

    await tx.creative.upsert({
      where: { id: creative.id },
      update: {
        name: creative.name,
        creativeType: creative.object_type,
        imageHash: creative.image_hash,
        videoId: creative.video_id,
        thumbnailUrl: creative.thumbnail_url,
        objectStoryId: creative.object_story_id,
        effectObjectStoryId: creative.effective_object_story_id,
        pageId,
        postId,
        systemPageId: fanpage?.id,
        rawPayload: toPrismaJson(creative),
        lastFetchedAt: new Date(),
        createdAt: ad.created_time ? new Date(ad.created_time) : undefined,
        updatedAt: ad.updated_time ? new Date(ad.updated_time) : undefined,
      },
      create: {
        id: creative.id,
        accountId,
        name: creative.name,
        creativeType: creative.object_type,
        imageHash: creative.image_hash,
        videoId: creative.video_id,
        thumbnailUrl: creative.thumbnail_url,
        objectStoryId: creative.object_story_id,
        effectObjectStoryId: creative.effective_object_story_id,
        pageId,
        postId,
        systemPageId: fanpage?.id,
        rawPayload: toPrismaJson(creative),
        lastFetchedAt: new Date(),
        createdAt: ad.created_time ? new Date(ad.created_time) : undefined,
        updatedAt: ad.updated_time ? new Date(ad.updated_time) : undefined,
      },
    });
  }

  /* ======================= CID ======================= */

  async syncCidLegacy(
    tx: Prisma.TransactionClient,
    accountId: string,
    ad: MetaAd,
  ) {
    const match = ad.name?.match(/CID\d+/);
    if (!match) return;

    const cid = match[0];
    await tx.cidGroup.upsert({
      where: { cid },
      update: { cid, name: cid, accountId },
      create: { cid, name: cid, accountId },
    });
  }

  /* ======================= AD ======================= */

  upsertAdLegacy(
    tx: Prisma.TransactionClient,
    accountId: string,
    campaignId: string,
    adsetId: string,
    ad: MetaAd,
  ) {
    return tx.ad.upsert({
      where: { id: ad.id },
      update: {
        name: ad.name,
        status: ad.status,
        effectiveStatus: ad.effective_status,
        configuredStatus: ad.configured_status,
        creativeId: ad.creative?.id,
        rawPayload: toPrismaJson(ad),
        lastFetchedAt: new Date(),
        createdAt: ad.created_time ? new Date(ad.created_time) : undefined,
        updatedAt: ad.updated_time ? new Date(ad.updated_time) : undefined,
        ...(ad?.insights?.data && Number(ad?.insights?.data?.length) > 0
          ? extractCampaignMetrics(ad.insights.data[0])
          : {}),
      },
      create: {
        id: ad.id,
        accountId,
        campaignId,
        adsetId,
        name: ad.name,
        status: ad.status,
        effectiveStatus: ad.effective_status,
        configuredStatus: ad.configured_status,
        creativeId: ad.creative?.id,
        rawPayload: toPrismaJson(ad),
        lastFetchedAt: new Date(),
        createdAt: ad.created_time ? new Date(ad.created_time) : undefined,
        updatedAt: ad.updated_time ? new Date(ad.updated_time) : undefined,
        ...(ad?.insights?.data && Number(ad?.insights?.data?.length) > 0
          ? extractCampaignMetrics(ad.insights.data[0])
          : {}),
      },
    });
  }
}
