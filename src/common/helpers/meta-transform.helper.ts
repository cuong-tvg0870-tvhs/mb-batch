import { Ad, AdSet, Campaign, Creative } from '@prisma/client';
import { extractCampaignMetrics } from '../utils';

export class MetaTransformHelper {
  static campaign(c: any, accountId: string) {
    return {
      id: c.id,
      accountId,
      name: c.name,
      status: c.status,
      objective: c.objective,
      buyingType: c.buying_type,
      dailyBudget: Number(c.daily_budget) || undefined,
      lifetimeBudget: Number(c.lifetime_budget) || undefined,
      effectiveBudget: Number(c.daily_budget ?? c.lifetime_budget ?? 0),
      rawPayload: c,
      lastFetchedAt: new Date(),
      createdAt: c.created_time ? new Date(c.created_time) : undefined,
      updatedAt: c.updated_time ? new Date(c.updated_time) : undefined,
    } as Campaign;
  }

  static adset(as: any, accountId: string, campaignId: string) {
    return {
      id: as.id,
      accountId,
      campaignId,
      name: as.name,
      status: as.status,
      destinationType: as.destination_type,
      dailyBudget: Number(as.daily_budget),
      lifetimeBudget: Number(as.lifetime_budget),
      effectiveBudget: Number(as.daily_budget ?? as.lifetime_budget ?? 0),
      rawPayload: as,
      optimizationGoal: as.optimization_goal,
      targeting: as.targeting,
      bidStrategy: as.bid_strategy,
      billingEvent: as.billing_event,

      lastFetchedAt: new Date(),
      ...(as?.insights?.data?.[0]
        ? extractCampaignMetrics(as.insights.data[0])
        : {}),
    } as AdSet;
  }

  static ad(ad: any, accountId: string, campaignId: string, adsetId: string) {
    return {
      id: ad.id,
      accountId,
      campaignId,
      adsetId,
      name: ad.name,
      status: ad.status,
      creativeId: ad.creative?.id,
      rawPayload: ad,
      lastFetchedAt: new Date(),
      ...(ad?.insights?.data?.[0]
        ? extractCampaignMetrics(ad.insights.data[0])
        : {}),
    } as Ad;
  }

  static creative(ad: any, accountId: string) {
    const c = ad.creative;
    if (!c) return null;

    const [pageId, postId] = c?.effective_object_story_id?.split('_') || [];

    return {
      id: c.id,
      accountId,
      name: c.name,
      creativeType: c.object_type,
      objectStoryId: c.object_story_id,
      imageHash: c.image_hash,
      videoId: c?.object_story_spec?.video_data?.video_id || c.video_id,
      thumbnailUrl:
        c.object_story_spec?.video_data?.image_url ||
        c.image_url ||
        c.thumbnail_url,
      pageId,
      postId,
      rawPayload: c,
      lastFetchedAt: new Date(),
    } as Creative;
  }
}
