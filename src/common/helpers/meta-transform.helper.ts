import { Ad, AdSet, Campaign, Creative } from '@prisma/client';
import { extractCampaignMetrics, toPrismaJson } from '../utils';

const toDate = (value?: string | number | Date | null) =>
  value ? new Date(value) : undefined;

const pickFirstString = (...values: unknown[]) => {
  for (const value of values) {
    if (typeof value === 'string' && value.trim()) return value.trim();
  }
  return undefined;
};

const firstArrayItem = (value: unknown) =>
  Array.isArray(value) && value.length > 0 ? value[0] : undefined;

const getThumbnailList = (thumbnails: any) => {
  if (Array.isArray(thumbnails?.data)) return thumbnails.data;
  if (Array.isArray(thumbnails)) return thumbnails;
  return [];
};

const getPreferredThumbnailUrl = (thumbnails: any) => {
  const list = getThumbnailList(thumbnails);
  const preferred =
    list.find((thumbnail: any) => !!thumbnail?.is_preferred) || list[0];

  return pickFirstString(
    preferred?.uri,
    preferred?.url,
    preferred?.image_url,
    preferred?.imageUrl,
  );
};

const getAssetImageUrl = (image: any) =>
  pickFirstString(
    image?.previewUrl,
    image?.preview_url,
    image?.imageUrl,
    image?.image_url,
    image?.thumbnailUrl,
    image?.thumbnail_url,
    image?.picture,
    image?.url,
  );

const getAssetVideoThumbnailUrl = (video: any) =>
  pickFirstString(
    video?.thumbnailUrl,
    video?.thumbnail_url,
    video?.imageUrl,
    video?.image_url,
    video?.previewUrl,
    video?.preview_url,
    video?.picture,
    getPreferredThumbnailUrl(video?.thumbnails),
    getPreferredThumbnailUrl(video?.list_thumbnails),
    getPreferredThumbnailUrl(video?.video_thumbnails),
    video?.selected_thumbnail?.image_url,
    video?.selected_thumbnail?.uri,
  );

const resolveCreativeMedia = (creative: any) => {
  const story = creative?.object_story_spec;
  const linkData = story?.link_data;
  const videoData = story?.video_data;
  const photoData = story?.photo_data;
  const assetFeed = creative?.asset_feed_spec;
  const assetImage = firstArrayItem(assetFeed?.images);
  const assetVideo = firstArrayItem(assetFeed?.videos);
  const childAttachment = firstArrayItem(
    linkData?.child_attachments || creative?.child_attachments,
  );

  const thumbnailUrl = pickFirstString(
    creative?.thumbnail_url,
    creative?.thumbnailUrl,
    getAssetVideoThumbnailUrl(assetVideo),
    getAssetImageUrl(assetImage),
    videoData?.image_url,
    videoData?.thumbnail_url,
    videoData?.picture,
    linkData?.picture,
    linkData?.image_url,
    linkData?.thumbnail_url,
    photoData?.image_url,
    photoData?.picture,
    photoData?.url,
    getAssetImageUrl(childAttachment),
    creative?.image_url,
    creative?.imageUrl,
    creative?.picture,
  );

  const imageUrl = pickFirstString(
    creative?.image_url,
    creative?.imageUrl,
    getAssetImageUrl(assetImage),
    linkData?.image_url,
    linkData?.picture,
    photoData?.image_url,
    photoData?.url,
    thumbnailUrl,
  );

  return {
    imageHash: pickFirstString(
      creative?.image_hash,
      linkData?.image_hash,
      photoData?.image_hash,
      assetImage?.hash,
      assetImage?.image_hash,
      childAttachment?.image_hash,
    ),
    imageUrl,
    videoId: pickFirstString(
      videoData?.video_id,
      creative?.video_id,
      assetVideo?.video_id,
      assetVideo?.id,
    ),
    thumbnailUrl,
    previewUrl: pickFirstString(thumbnailUrl, imageUrl),
  };
};

/**
 * Maps raw Meta API objects into Prisma rows for the synced core tables.
 *
 * INVARIANT — do not trim `rawPayload`. The draft-copy and detail flows in
 * mb-ads/mb-frontend reconstruct campaigns and render previews almost entirely
 * from the full Meta JSON stored here (creative.object_story_spec /
 * asset_feed_spec, adset.targeting / promoted_object, campaign bid_strategy /
 * special_ad_categories, etc.). The structured columns below are a convenience
 * subset for listing/filtering; `rawPayload` is the contract. The fetch field
 * lists in common/utils/meta-field.ts are what populate it, so keep those the
 * superset and always store `toPrismaJson(<whole object>)`.
 */
export class MetaTransformHelper {
  static campaign(c: any, accountId: string) {
    return {
      id: c.id,
      accountId,
      name: c.name,
      status: c.status,
      effectiveStatus: c.effective_status,
      configuredStatus: c.configured_status,
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
      effectiveStatus: as.effective_status,
      configuredStatus: as.configured_status,
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

    const media = resolveCreativeMedia(c);

    return {
      id: c.id,
      accountId,
      name: c.name,
      creativeType: c.object_type,
      objectStoryId: c.object_story_id,
      effectObjectStoryId: c.effective_object_story_id,
      imageHash: media.imageHash,
      imageUrl: media.imageUrl,
      videoId: media.videoId,
      thumbnailUrl: media.thumbnailUrl,
      previewUrl: media.previewUrl,
      pageId,
      postId,
      rawPayload: toPrismaJson(c),
      remoteUpdatedAt: toDate(c.updated_time || ad.updated_time),
      lastFetchedAt: new Date(),
    } as Creative;
  }
}
