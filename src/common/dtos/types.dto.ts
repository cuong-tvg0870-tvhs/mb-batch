export interface MetaCampaignTree {
  id: string;
  name?: string;
  status?: string;
  objective?: string;
  buying_type?: string;
  daily_budget?: string;
  lifetime_budget?: string;
  start_time?: string;
  stop_time?: string;
  updated_time?: string;
  created_time?: string;

  adsets?: MetaAdSet[];

  insights?: Record<string, any>;

  systemCampaignId?: string;

  account_id?: string;
}

/* ================= ADSET ================= */

export interface MetaAdSet {
  id: string;
  name?: string;
  status?: string;
  optimization_goal?: string;
  billing_event?: string;
  bid_strategy?: string;
  targeting?: Record<string, any>;
  daily_budget?: string;
  lifetime_budget?: string;
  start_time?: string;
  end_time?: string;
  updated_time?: string;
  created_time?: string;
  ads?: MetaAd[];
  insights?: Record<string, any>;
}

/* ================= AD ================= */

export interface MetaAd {
  id: string;
  name?: string;
  status?: string;
  effective_status?: string;
  configured_status?: string;
  updated_time?: string;
  created_time?: string;

  creative?: MetaCreative;

  insights?: Record<string, any>;
}

/* ================= CREATIVE ================= */

export interface MetaCreative {
  id: string;
  name?: string;
  object_story_id?: string;
  effective_object_story_id?: string;
  page_id?: string;
  image_hash?: string;
  video_id?: string;
  updated_time?: string;

  insights?: Record<string, any>;

  // Meta hay nhét thêm stuff → giữ mở
  [key: string]: any;
}
