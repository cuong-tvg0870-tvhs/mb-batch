import { BadRequestException, HttpException, HttpStatus } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import Cursor from 'facebook-nodejs-business-sdk/src/cursor';
import { MetaFatalError, normalizeMetaError } from './meta-mapping.util';
export * from './meta-mapping.util';
export * from './password.util';

export const LIMIT_DATA = 50;
export enum BudgetType {
  DAILY = 'DAILY',
  LIFETIME = 'LIFETIME',
}

export function toPrismaJson(data: unknown): Prisma.InputJsonValue {
  return JSON.parse(JSON.stringify(data));
}
export type PrismaErrorKey =
  | 'UniqueConstraintViolation'
  | 'ForeignKeyViolation'
  | 'NotNullViolation'
  | 'CheckViolation';

export interface DriverAdapterError {
  cause?: {
    kind?: string;
    constraint?: { fields?: string[] };
    originalMessage?: string;
  };
}

export function metaError(e: any) {
  return e?.response?.error || e;
}

export function isRateLimit(e: any) {
  const err = metaError(e);
  return (
    [4, 17, 32, 613, 80004].includes(err?.code) ||
    err?.error_subcode === 2446079 ||
    err?.message?.includes('reduce the amount of data')
  );
}

export function isPermissionError(e: any) {
  const err = metaError(e);
  return [10, 200].includes(err?.code);
}

export function isNotFound(e: any) {
  const err = metaError(e);
  return err?.code === 100;
}

export const parseMetaError = (err: any) => {
  const e = err?.response;
  console.log(err);
  return {
    message: e?.error_user_msg || e?.message || 'Meta API error',
    title: e?.error_user_title,
    code: e?.code,
    subcode: e?.error_subcode,
    type: e?.type,
    fbtrace_id: e?.fbtrace_id,
    raw: e,
  };
};

export const ThrowErrorWithFormDatabase = (
  key?: PrismaErrorKey,
  driverAdapterError?: DriverAdapterError,
) => {
  switch (key) {
    case 'UniqueConstraintViolation': {
      const fields = driverAdapterError?.cause?.constraint?.fields || [];
      const errors = fields.reduce<Record<string, string>>((acc, field) => {
        acc[field] = 'This value already exists';
        return acc;
      }, {});
      throw new HttpException(
        {
          status: HttpStatus.CONFLICT,
          message: 'Unique constraint violation',
          errors,
        },
        HttpStatus.CONFLICT,
      );
    }

    case 'ForeignKeyViolation': {
      const fields = driverAdapterError?.cause?.constraint?.fields || [];
      const errors = fields.reduce<Record<string, string>>((acc, field) => {
        acc[field] = 'Related record not found';
        return acc;
      }, {});
      throw new HttpException(
        {
          status: HttpStatus.BAD_REQUEST,
          message: 'Foreign key violation',
          errors,
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    case 'NotNullViolation': {
      const fields = driverAdapterError?.cause?.constraint?.fields || [];
      const errors = fields.reduce<Record<string, string>>((acc, field) => {
        acc[field] = 'This field cannot be null';
        return acc;
      }, {});
      throw new HttpException(
        {
          status: HttpStatus.BAD_REQUEST,
          message: 'Not null constraint violation',
          errors,
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    case 'CheckViolation': {
      const message =
        driverAdapterError?.cause?.originalMessage || 'Check constraint failed';
      throw new HttpException(
        {
          status: HttpStatus.BAD_REQUEST,
          message,
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    default:
      throw new BadRequestException(driverAdapterError);
  }
};

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// 2. Từ khóa phổ biến
export const commonKeywords = [
  'food',
  'drink',
  'coffee',
  'tea',
  'restaurant',
  'travel',
  'tourism',
  'fitness',
  'gym',
  'health',
  'sport',
  'football',
  'music',
  'movie',
  'game',
  'technology',
  'marketing',
  'finance',
  'business',
  'education',
  'baby',
  'parenting',
  'job',
  'career',
  'shopping',
  'luxury',
  'pet',
  'dog',
  'cat',
  'car',
  'motorbike',
];

// 3. Từ khóa chuyên ngành (mỹ phẩm, áo quần – EN + VI)
export const beautyFashionKeywords = [
  // English
  'makeup',
  'cosmetics',
  'beauty',
  'skincare',
  'lipstick',
  'foundation',
  'serum',
  'moisturizer',
  'cleanser',
  'perfume',
  'fragrance',
  'nail',
  'haircare',
  'korean beauty',
  'japanese cosmetics',
  'natural cosmetics',
  'fashion',
  'clothing',
  'luxury',
  'handbag',
  'shoes',
  'accessories',
  'eyeshadow',
  'concealer',
  'bb cream',

  // Vietnamese
  'mỹ phẩm',
  'son môi',
  'son dưỡng',
  'kem nền',
  'kem chống nắng',
  'nước hoa',
  'chăm sóc da',
  'trang điểm',
  'phấn má',
  'phấn phủ',
  'serum dưỡng da',
  'kem dưỡng ẩm',
  'chăm sóc tóc',
  'dầu gội',
  'dầu xả',
  'nail',
  'thời trang',
  'quần áo',
  'váy',
  'giày dép',
  'túi xách',
  'phụ kiện',
  'trang sức',
];
export function today() {
  return new Date().toISOString().slice(0, 10);
}

export function yearStart() {
  return `2023-01-01`;
}

export function daysAgo(days: number, from = new Date()) {
  const d = new Date(from);
  d.setDate(d.getDate() - days);
  return d;
}

export function chunk<T>(arr: T[], size: number): T[][] {
  const res = [] as any;
  for (let i = 0; i < arr.length; i += size) {
    res.push(arr.slice(i, i + size));
  }
  return res;
}
export function getAction(actions: any[], type: string): number {
  const a = actions?.find((x) => x.action_type === type);
  return Number(a?.value || 0);
}

export function getActionValue(values: any[], type: string): number {
  const v = values?.find((x) => x.action_type === type);
  return Number(v?.value || 0);
}

export function addDays(date: Date, days: number) {
  const d = new Date(date);
  d.setDate(d.getDate() + days);
  return d;
}

export function metaFormatDate(date: Date) {
  return date.toISOString().slice(0, 10);
}

export async function fetchAll(
  cursor: Cursor,
  options?: {
    sleepMs?: number;
    maxRetries?: number;
    context?: Record<string, any>;
  },
) {
  const result: any[] = [];
  if (!cursor) return result;

  const sleepMs = options?.sleepMs ?? 2000;
  const maxRetries = options?.maxRetries ?? 1;

  let page = cursor;
  let retry = 0;

  try {
    for (const item of page) {
      result.push(item._data);
    }

    while (page.hasNext()) {
      try {
        page = await page.next();
        retry = 0;

        for (const item of page) {
          result.push(item._data);
        }

        await sleep(sleepMs);
      } catch (err) {
        const metaErr = normalizeMetaError(err);

        // ⛔ FATAL → STOP NGAY
        if ([190, 10, 200, 368, 102].includes(metaErr.code)) {
          throw new MetaFatalError(
            `META_FATAL(${metaErr.code}): ${metaErr.message}`,
            metaErr,
          );
        }

        // 🔁 RATE LIMIT
        if ([4, 17].includes(metaErr.code) && retry < maxRetries) {
          retry++;
          await sleep(sleepMs * retry);
          continue;
        }

        throw err;
      }
    }
  } catch (err) {
    if (err instanceof MetaFatalError) throw err;
    throw err;
  }

  return result;
}

export const CleanObjectOrArray = (value: any): any => {
  if (value === null || value === undefined || value === '') return undefined;

  if (Array.isArray(value)) {
    const arr = value.map(CleanObjectOrArray).filter(Boolean);
    return arr.length ? arr : undefined;
  }

  if (typeof value === 'object') {
    const obj: any = {};
    for (const k in value) {
      const v = CleanObjectOrArray(value[k]);
      if (v !== undefined) obj[k] = v;
    }
    return Object.keys(obj).length ? obj : undefined;
  }

  return value;
};

export const chunkArray = <T>(arr: T[], size: number): T[][] => {
  const result: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    result.push(arr.slice(i, i + size));
  }
  return result;
};

export function getDateRange(since: Date, until: Date): string[] {
  const dates: string[] = [];
  const cur = new Date(since);

  while (cur <= until) {
    dates.push(cur.toISOString().slice(0, 10)); // YYYY-MM-DD
    cur.setDate(cur.getDate() + 1);
  }

  return dates;
}

const INSIGHT_TTL_MS = 3 * 60 * 60 * 1000;
export function isFresh(date?: Date | null) {
  if (!date) return false;
  return Date.now() - new Date(date).getTime() < INSIGHT_TTL_MS;
}

export function extractCampaignMetrics(insight: any) {
  function toNumber(value: any, defaultValue = 0): number {
    if (value === null || value === undefined) return defaultValue;
    const num = Number(value);
    return Number.isFinite(num) ? num : defaultValue;
  }

  function getActionValue(actions: any[] | undefined, type: string) {
    if (!actions) return 0;
    const found = actions.find((a) => a.action_type === type);
    return toNumber(found?.value);
  }

  function getActionValueFromValues(values: any[] | undefined, type: string) {
    if (!values) return 0;
    const found = values.find((a) => a.action_type === type);
    return toNumber(found?.value);
  }

  function getVideoMetric(actions: any[] | undefined) {
    if (!actions || actions.length === 0) return 0;
    return toNumber(actions[0]?.value);
  }

  // ===== BASE =====
  const impressions = toNumber(insight?.impressions);
  const reach = toNumber(insight?.reach);
  const clicks = toNumber(insight?.clicks);
  const spend = toNumber(insight?.spend);

  // ===== PURCHASE (COUNT) =====
  const purchases = getActionValue(
    insight?.actions,
    'onsite_conversion.purchase',
  );

  // ===== PURCHASE VALUE (MONEY – đã quy đổi ở tầng fetch) =====
  const purchaseValue = getActionValueFromValues(
    insight?.action_values,
    'onsite_conversion.purchase',
  );

  // ===== ROAS (❗ CHUẨN CỦA CTY) =====
  const roasCalculated = spend > 0 ? purchaseValue / spend : 0;
  const roas = roasCalculated;

  // ===== DERIVED =====
  const cvr = clicks > 0 ? purchases / clicks : 0;
  const costPerResult = purchases > 0 ? spend / purchases : 0;
  const adsCostRatio = roas > 0 ? 1 / roas : 0;

  // ===== VIDEO RAW =====
  const videoPlay = getVideoMetric(insight?.video_play_actions);

  const video3s = getActionValue(insight?.actions, 'video_view');

  const videoThruplay = getVideoMetric(insight?.video_thruplay_watched_actions);

  const video100 = getVideoMetric(insight?.video_p100_watched_actions);

  const videoAvgWatchTime = getVideoMetric(
    insight?.video_avg_time_watched_actions,
  );

  // ===== VIDEO RATE =====
  const hookRate =
    videoPlay > 0 ? +((video3s / videoPlay) * 100).toFixed(2) : 0;

  const holdRate = video3s > 0 ? +((video100 / video3s) * 100).toFixed(2) : 0;

  // ===== EXTRA ACTIONS (MATCH GAS) =====
  const registrationComplete =
    getActionValue(insight?.actions, 'complete_registration') +
    getActionValue(
      insight?.actions,
      'offsite_conversion.complete_registration',
    );

  const registrationCompleteValue =
    getActionValueFromValues(insight?.action_values, 'complete_registration') +
    getActionValueFromValues(
      insight?.action_values,
      'offsite_conversion.complete_registration',
    );

  const messagingStarted = getActionValue(
    insight?.actions,
    'onsite_conversion.messaging_conversation_started_7d',
  );

  const messagingStartedValue = getActionValueFromValues(
    insight?.actions,
    'onsite_conversion.messaging_conversation_started_7d',
  );

  const outboundClicks = getActionValue(
    insight?.outbound_clicks,
    'outbound_click',
  );

  const outboundClicksValue = getActionValueFromValues(
    insight?.outbound_clicks,
    'outbound_click',
  );

  return {
    impressions,
    reach,
    frequency: toNumber(insight?.frequency),

    clicks,
    uniqueClicks: toNumber(insight?.unique_clicks),

    ctr: toNumber(insight?.ctr),
    uniqueCtr: toNumber(insight?.unique_ctr),

    cpc: toNumber(insight?.cpc),
    cpm: toNumber(insight?.cpm),

    spend,

    // ===== RESULT =====
    results: Number(purchases) + Number(registrationComplete),
    aov:
      Number(purchases) + Number(registrationComplete) > 0
        ? Number(purchaseValue) /
          (Number(purchases) + Number(registrationComplete))
        : null,

    costPerResult,

    purchases,
    purchaseValue,
    roas,

    cvr,
    adsCostRatio,

    registrationComplete,
    registrationCompleteValue,

    messagingStarted,
    messagingStartedValue,
    outboundClicks,
    outboundClicksValue,

    // ===== VIDEO =====
    videoPlay,
    video3s,
    videoThruplay,
    video100,
    videoAvgWatchTime,

    hookRate, // %
    holdRate, // %

    qualityRanking: insight?.quality_ranking ?? null,
    engagementRateRanking: insight?.engagement_rate_ranking ?? null,
    conversionRateRanking: insight?.conversion_rate_ranking ?? null,

    // DEBUG / RAW
    actions: insight?.actions ?? null,
    actionValues: insight?.action_values ?? null,
  };
}
