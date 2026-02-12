import { Injectable } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import {
  MetaAd,
  MetaAdSet,
  MetaCampaignTree,
  MetaCreative,
} from '../../common/dtos/types.dto';
import { extractCampaignMetrics, toPrismaJson } from '../../common/utils';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class UpsertService {
  constructor(private prisma: PrismaService) {}

  /* ======================= CAMPAIGN ======================= */

  async upsertCampaign(
    tx: Prisma.TransactionClient,
    accountId: string,
    c: MetaCampaignTree,
  ) {
    const campaignUpdate = await tx.campaign.upsert({
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

    return campaignUpdate;
  }

  /* ======================= ADSET ======================= */

  async upsertAdSet(
    tx: Prisma.TransactionClient,
    accountId: string,
    campaignId: string,
    as: MetaAdSet,
  ) {
    const updated = await tx.adSet.upsert({
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

    return updated;
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

  /* ======================= AD ======================= */

  async upsertAdLegacy(
    tx: Prisma.TransactionClient,
    accountId: string,
    campaignId: string,
    adsetId: string,
    ad: MetaAd,
  ) {
    const updated = await tx.ad.upsert({
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

    return updated;
  }
}
