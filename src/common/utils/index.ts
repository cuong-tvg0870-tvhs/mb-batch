import { BadRequestException, HttpException, HttpStatus } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import Cursor from 'facebook-nodejs-business-sdk/src/cursor';
import { MetaFatalError, normalizeMetaError } from './meta-mapping.util';
export * from './meta-mapping.util';

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
