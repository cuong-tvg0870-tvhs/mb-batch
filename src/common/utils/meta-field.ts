import {
  Ad,
  AdAccount,
  AdCreative,
  AdImage,
  AdSet,
  AdsInsights,
  AdsPixel,
  Campaign,
} from 'facebook-nodejs-business-sdk';

export const AD_ACCOUNT_FIELDS = [
  AdAccount.Fields.id,
  AdAccount.Fields.account_id,
  AdAccount.Fields.name,
  AdAccount.Fields.account_status,
  AdAccount.Fields.currency,
  AdAccount.Fields.timezone_name,
  AdAccount.Fields.timezone_offset_hours_utc,
  // AdAccount.Fields.business_name,
  AdAccount.Fields.owner,
  AdAccount.Fields.spend_cap,
  AdAccount.Fields.amount_spent,
  AdAccount.Fields.created_time,
  AdAccount.Fields.disable_reason,
  AdAccount.Fields.is_personal,
];

export const AD_PIXEL_FIELDS = [
  AdsPixel.Fields.id,
  AdsPixel.Fields.name,
  // AdsPixel.Fields.owner_ad_account,
  // AdsPixel.Fields.owner_business,

  AdsPixel.Fields.creation_time,
  AdsPixel.Fields.last_fired_time,
  AdsPixel.Fields.is_created_by_business,
  AdsPixel.Fields.is_unavailable,
  AdsPixel.Fields.is_restricted_use,
  AdsPixel.Fields.usage,
];

export const CAMPAIGN_FIELDS = [
  Campaign.Fields.id,
  Campaign.Fields.account_id,
  Campaign.Fields.name,
  Campaign.Fields.objective,
  Campaign.Fields.status,
  Campaign.Fields.effective_status,
  Campaign.Fields.buying_type,
  Campaign.Fields.daily_budget,
  Campaign.Fields.lifetime_budget,
  Campaign.Fields.budget_remaining,
  Campaign.Fields.bid_strategy,
  Campaign.Fields.promoted_object,
  Campaign.Fields.special_ad_category,
  Campaign.Fields.special_ad_categories,
  Campaign.Fields.is_budget_schedule_enabled,
  Campaign.Fields.start_time,
  Campaign.Fields.stop_time,
  Campaign.Fields.created_time,
  Campaign.Fields.updated_time,
];

export const ADSET_FIELDS = [
  AdSet.Fields.id,
  AdSet.Fields.account_id,
  AdSet.Fields.campaign_id,
  AdSet.Fields.destination_type,
  AdSet.Fields.name,
  AdSet.Fields.status,
  AdSet.Fields.effective_status,
  AdSet.Fields.optimization_goal,
  AdSet.Fields.billing_event,
  AdSet.Fields.bid_strategy,
  AdSet.Fields.bid_amount,
  AdSet.Fields.daily_budget,
  AdSet.Fields.lifetime_budget,
  AdSet.Fields.budget_remaining,
  AdSet.Fields.start_time,
  AdSet.Fields.end_time,
  AdSet.Fields.targeting,
  AdSet.Fields.promoted_object,
  AdSet.Fields.created_time,
  AdSet.Fields.updated_time,
];
export const AD_FIELDS = [
  Ad.Fields.id,
  Ad.Fields.account_id,
  Ad.Fields.campaign_id,
  Ad.Fields.adset_id,
  Ad.Fields.name,
  Ad.Fields.status,
  Ad.Fields.effective_status,
  Ad.Fields.creative_asset_groups_spec,
  Ad.Fields.bid_amount,
  Ad.Fields.priority,
  Ad.Fields.created_time,
  Ad.Fields.updated_time,
  Ad.Fields.creative,
];

export const CREATIVE_FIELDS = [
  AdCreative.Fields.id,
  AdCreative.Fields.account_id,
  AdCreative.Fields.actor_id,
  AdCreative.Fields.adlabels,
  AdCreative.Fields.name,
  AdCreative.Fields.object_story_id,
  AdCreative.Fields.image_hash,
  AdCreative.Fields.image_url,
  AdCreative.Fields.video_id,
  AdCreative.Fields.thumbnail_url,
  AdCreative.Fields.call_to_action,
  AdCreative.Fields.title,
  AdCreative.Fields.body,
  AdCreative.Fields.link_url,
  AdCreative.Fields.asset_feed_spec,
  AdCreative.Fields.object_store_url,
  AdCreative.Fields.status,
  AdCreative.Fields.effective_object_story_id,
];

export const AD_IMAGE_FIELDS = [
  AdImage.Fields.id,
  AdImage.Fields.account_id,
  AdImage.Fields.name,
  AdImage.Fields.hash,
  AdImage.Fields.url,
  AdImage.Fields.permalink_url,
  AdImage.Fields.width,
  AdImage.Fields.height,
  AdImage.Fields.status,
  AdImage.Fields.created_time,
  AdImage.Fields.updated_time,
];

export const AD_VIDEO_FIELDS = [
  'id',
  'title',
  'description',
  'length',
  'status',
  'thumbnails',
  'permalink_url',
  'source',
  'picture',
  'created_time',
];
export const AD_INSIGHT_FIELDS = [
  // identity
  AdsInsights.Fields.account_id,
  AdsInsights.Fields.campaign_id,
  AdsInsights.Fields.adset_id,
  AdsInsights.Fields.ad_id,

  // time
  AdsInsights.Fields.date_start,
  AdsInsights.Fields.date_stop,

  // delivery
  AdsInsights.Fields.impressions,
  AdsInsights.Fields.reach,
  AdsInsights.Fields.frequency,

  // clicks
  AdsInsights.Fields.clicks,
  AdsInsights.Fields.unique_clicks,
  AdsInsights.Fields.outbound_clicks,
  AdsInsights.Fields.unique_outbound_clicks,

  // rates & cost
  AdsInsights.Fields.ctr,
  AdsInsights.Fields.unique_ctr,
  AdsInsights.Fields.cpc,
  AdsInsights.Fields.cpm,
  AdsInsights.Fields.cost_per_outbound_click,

  // spend
  AdsInsights.Fields.spend,

  // conversion
  AdsInsights.Fields.actions,
  AdsInsights.Fields.unique_actions,
  AdsInsights.Fields.action_values,
  AdsInsights.Fields.cost_per_action_type,

  AdsInsights.Fields.conversions,
  AdsInsights.Fields.conversion_values,

  AdsInsights.Fields.results,
  AdsInsights.Fields.cost_per_result,

  AdsInsights.Fields.purchase_roas,

  // quality
  AdsInsights.Fields.quality_ranking,
  AdsInsights.Fields.engagement_rate_ranking,
  AdsInsights.Fields.conversion_rate_ranking,

  // video
  AdsInsights.Fields.video_play_actions,
  AdsInsights.Fields.video_thruplay_watched_actions,
  AdsInsights.Fields.video_avg_time_watched_actions,
  AdsInsights.Fields.video_p25_watched_actions,
  AdsInsights.Fields.video_p50_watched_actions,
  AdsInsights.Fields.video_p75_watched_actions,
  AdsInsights.Fields.video_p95_watched_actions,
  AdsInsights.Fields.video_p100_watched_actions,
  AdsInsights.Fields.video_30_sec_watched_actions,

  AdsInsights.Fields.marketing_messages_delivered,
  AdsInsights.Fields.inline_post_engagement,
];

export const SUMMARY_AD_INSIGHT_FIELDS = [
  AdsInsights.Fields.date_start,
  AdsInsights.Fields.date_stop,

  // delivery
  AdsInsights.Fields.impressions,
  AdsInsights.Fields.reach,
  AdsInsights.Fields.frequency,

  // clicks
  AdsInsights.Fields.clicks,
  AdsInsights.Fields.unique_clicks,
  AdsInsights.Fields.outbound_clicks,
  AdsInsights.Fields.unique_outbound_clicks,

  // rates & cost
  AdsInsights.Fields.ctr,
  AdsInsights.Fields.unique_ctr,
  AdsInsights.Fields.cpc,
  AdsInsights.Fields.cpm,
  AdsInsights.Fields.cost_per_outbound_click,

  // spend
  AdsInsights.Fields.spend,

  // conversion
  AdsInsights.Fields.actions,
  AdsInsights.Fields.unique_actions,
  AdsInsights.Fields.action_values,
  AdsInsights.Fields.cost_per_action_type,

  AdsInsights.Fields.conversions,
  AdsInsights.Fields.conversion_values,

  AdsInsights.Fields.results,
  AdsInsights.Fields.cost_per_result,

  AdsInsights.Fields.purchase_roas,

  // quality

  // video
  AdsInsights.Fields.video_play_actions,
  AdsInsights.Fields.video_thruplay_watched_actions,
  AdsInsights.Fields.video_avg_time_watched_actions,
  AdsInsights.Fields.video_p25_watched_actions,
  AdsInsights.Fields.video_p50_watched_actions,
  AdsInsights.Fields.video_p75_watched_actions,
  AdsInsights.Fields.video_p95_watched_actions,
  AdsInsights.Fields.video_p100_watched_actions,
  AdsInsights.Fields.video_30_sec_watched_actions,

  AdsInsights.Fields.inline_post_engagement,
];

export const AUDIENCE_FIELDS = ['id', 'name', 'subtype', 'approximate_count'];
export const PAGE_FIELDS = ['id', 'name', 'access_token'];
