import { Ad, AdSet, Campaign, Creative } from '@prisma/client';
import { extractCampaignMetrics, toPrismaJson } from '../utils';

const toDate = (value?: string | number | Date | null) =>
  value ? new Date(value) : undefined;

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
      rawPayload: toPrismaJson(c),
      remoteUpdatedAt: toDate(c.updated_time),
      lastFetchedAt: new Date(),
      startTime: toDate(c.start_time),
      endTime: toDate(c.stop_time),
      createdAt: toDate(c.created_time),
      updatedAt: toDate(c.updated_time) || new Date(),
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
      dailyBudget: Number(as.daily_budget) || undefined,
      lifetimeBudget: Number(as.lifetime_budget) || undefined,
      effectiveBudget: Number(as.daily_budget ?? as.lifetime_budget ?? 0),
      rawPayload: toPrismaJson(as),
      optimizationGoal: as.optimization_goal,
      targeting: as.targeting,
      bidStrategy: as.bid_strategy,
      billingEvent: as.billing_event,
      startTime: toDate(as.start_time),
      endTime: toDate(as.end_time),
      remoteUpdatedAt: toDate(as.updated_time),

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
      effectiveStatus: ad.effective_status,
      configuredStatus: ad.configured_status,
      creativeId: ad.creative?.id,
      rawPayload: toPrismaJson(ad),
      remoteUpdatedAt: toDate(ad.updated_time),
      lastFetchedAt: new Date(),
      ...(ad?.insights?.data?.[0]
        ? extractCampaignMetrics(ad.insights.data[0])
        : {}),
    } as Ad;
  }

  static creative(ad: any, accountId: string) {
    const c = ad.creative;
    if (!c) return null;

    let [pageId, postId] =
      (c?.effective_object_story_id || c?.object_story_id)?.split('_') || [];

    if (!postId && pageId) {
      postId = pageId;
      pageId = undefined;
    }

    if (!pageId) {
      pageId = c?.actor_id || c?.object_story_spec?.page_id;
    }

    return {
      id: c.id,
      accountId,
      name: c.name,
      creativeType: c.object_type,
      objectStoryId: c.object_story_id,
      effectObjectStoryId: c.effective_object_story_id,
      imageHash: c.image_hash,
      imageUrl: c.image_url || c.object_story_spec?.link_data?.image_url,
      videoId: c?.object_story_spec?.video_data?.video_id || c.video_id,
      thumbnailUrl:
        c.object_story_spec?.video_data?.image_url ||
        c.object_story_spec?.link_data?.image_url ||
        c.image_url ||
        c.thumbnail_url,
      pageId,
      postId,
      rawPayload: toPrismaJson(c),
      remoteUpdatedAt: toDate(c.updated_time || ad.updated_time),
      lastFetchedAt: new Date(),
    } as Creative;
  }
}
