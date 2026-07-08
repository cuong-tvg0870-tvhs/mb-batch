import { BadRequestException, HttpException, HttpStatus } from '@nestjs/common';
import {
  CreativeAsset,
  CreativeFolder,
  FolderStatus,
  Prisma,
} from '@prisma/client';
import Cursor from 'facebook-nodejs-business-sdk/src/cursor';
import { normalizeMetaError } from './meta-mapping.util';
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
  return e?.response || e;
}

export function isRetryableError(e: any) {
  const err = metaError(e);
  // Rate limit codes: 4, 17, 32, 613, 80004
  // Transient/Server codes: 1 (API Unknown), 2 (API Service)
  const retryableCodes = [1, 2, 4, 17, 32, 613, 80004];

  const message = (err?.message || e?.message || '').toLowerCase();
  const isNetworkError =
    message.includes('no response was received') ||
    message.includes('timeout') ||
    message.includes('network error') ||
    message.includes('enotfound') ||
    message.includes('econnreset') ||
    message.includes('etimedout') ||
    message.includes('econnaborted') ||
    message.includes('socket hang up');

  return (
    retryableCodes.includes(err?.code) ||
    err?.is_transient === true ||
    err?.error_subcode === 2446079 ||
    message.includes('reduce the amount of data') ||
    message.includes('unexpected error') ||
    isNetworkError
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
  const e =
    err?.metaError ||
    err?.response?.data?.error ||
    err?.response?.data ||
    err?.response?.error ||
    err?.response ||
    err?.error ||
    err;
  return {
    message: e?.error_user_msg || e?.message || 'Meta API error',
    title: e?.error_user_title,
    code: e?.code,
    subcode: e?.error_subcode,
    type: e?.type,
    fbtrace_id: e?.fbtrace_id,
    is_transient: e?.is_transient,
    raw: e,
  };
};

// Meta subcode 2490408 cho MESSAGING_PURCHASE_CONVERSION = Trang chưa đủ điều kiện
// "mua hàng qua tin nhắn" (thiếu đo lường Purchase qua CAPI for Business Messaging),
// KHÔNG phải sai objective↔goal. Giữ song song với bản ở mb-ads (common/utils/index.ts).
export const MPC_NOT_ELIGIBLE_MESSAGE =
  'Trang chưa đủ điều kiện tối ưu "Mua hàng qua tin nhắn": Meta cần Trang bật đo lường mua hàng qua tin nhắn (CAPI for Business Messaging) và có đủ sự kiện Purchase gần đây. Hãy đổi "Mục tiêu hiệu quả" sang "Số tin nhắn" (Conversations), hoặc tạo nhóm quảng cáo này trực tiếp trên Trình quản lý quảng cáo của Meta.';

/**
 * Chuyển lỗi Meta (thường tiếng Anh) sang hướng dẫn ngắn gọn tiếng Việt + cách
 * khắc phục. Trả null nếu không khớp mẫu nào để caller fallback message gốc.
 * Giữ song song với bản ở mb-ads (common/utils/index.ts).
 */
export const metaErrorToFriendly = (metaError: any): string | null => {
  if (!metaError) return null;
  const msg = String(
    metaError.message || metaError.raw?.message || '',
  ).toLowerCase();
  const code = Number(metaError.code);
  const has = (...keys: string[]) => keys.some((k) => msg.includes(k));

  if (has('image hash', 'image_hash', 'invalid image', 'image is not'))
    return 'Ảnh không hợp lệ hoặc đã hết hạn trên Meta. Vui lòng chọn lại ảnh/tài nguyên cho quảng cáo bị lỗi rồi publish lại.';
  if (has('video', 'thumbnail') && has('not', 'invalid', 'missing'))
    return 'Video chưa sẵn sàng hoặc thiếu ảnh đại diện trên Meta. Hãy chọn lại video có thumbnail rồi thử lại.';
  if (
    has('minimum budget', 'budget is too low', 'below the minimum') ||
    (has('budget') && has('minimum', 'too low'))
  )
    return 'Ngân sách đang thấp hơn mức tối thiểu của Meta. Vui lòng tăng ngân sách ngày/trọn đời rồi publish lại.';
  if (has('special ad', 'special_ad_categor'))
    return 'Chiến dịch thuộc Danh mục quảng cáo đặc biệt — cần khai báo đúng danh mục và quốc gia điều chỉnh.';
  // "Đã cố tạo nội dung động mà không có ID nhóm sản phẩm" / "tried to create
  // dynamic content without a product set" — tái dùng "bài viết có sẵn" vốn là
  // quảng cáo động Shops/Catalog ở chiến dịch/tài khoản không có catalog.
  if (
    has('nội dung động', 'nhóm sản phẩm') ||
    has('dynamic content', 'product set', 'product_set')
  )
    return 'Quảng cáo cần gắn Nhóm sản phẩm (Catalog) nhưng chưa có. Thường gặp khi dùng lại "Bài viết có sẵn" vốn là quảng cáo động Shops/Catalog ở chiến dịch/tài khoản không có catalog — hãy chọn một Bài viết thường khác cho quảng cáo bị lỗi, hoặc thiết lập Catalog/Nhóm sản phẩm cho chiến dịch (hoặc đăng ở đúng tài khoản gốc có catalog).';
  // X3: Thực thể gắn theo Tài khoản quảng cáo bị dùng nhầm sang tài khoản khác —
  // hay gặp khi lên chiến dịch từ MẪU (template) của TKQC khác mà chưa chọn lại.
  // Tệp đối tượng (Custom/Lookalike): Meta báo not found / not available / invalid.
  if (
    has('custom audience', 'custom_audience', 'lookalike') &&
    has(
      'not found',
      'not available',
      'does not exist',
      'invalid',
      'not belong',
      'does not belong',
    )
  )
    return 'Tệp đối tượng (Custom/Lookalike) không thuộc Tài khoản quảng cáo đang chọn hoặc không còn tồn tại. Nếu bạn lên chiến dịch từ MẪU của tài khoản khác, hãy bỏ hoặc chọn lại Tệp đối tượng cho đúng tài khoản rồi publish lại.';
  // Bài viết có sẵn / Pixel / Catalog / Trang không thuộc đúng TKQC hoặc Trang.
  if (
    has(
      'does not belong',
      'not belong to',
      'is not associated',
      'not associated with',
      'not available for the ad account',
      'not available in this ad account',
    ) &&
    has(
      'post',
      'story',
      'object_story',
      'pixel',
      'dataset',
      'catalog',
      'product set',
      'product_set',
      'page',
      'fanpage',
    )
  )
    return 'Một thành phần (Bài viết có sẵn / Pixel / Catalog / Trang) không thuộc Tài khoản quảng cáo hoặc Trang đang chọn. Nếu bạn lên chiến dịch từ MẪU của tài khoản khác, hãy chọn lại đúng Bài viết/Pixel/Trang của tài khoản hiện tại rồi publish lại.';
  if (has('pixel', 'dataset', 'promoted object', 'promoted_object'))
    return 'Thiếu Pixel/Dataset hoặc đối tượng quảng bá (promoted object). Vui lòng chọn Pixel/Trang phù hợp với mục tiêu chiến dịch.';
  if (has('audience', 'targeting') && has('control', 'expand', 'invalid'))
    return 'Thiết lập đối tượng/nhắm mục tiêu chưa hợp lệ. Kiểm tra lại quốc gia, độ tuổi và vị trí quảng cáo.';
  if (
    (has('page', 'fanpage') && has('permission', 'access', 'not authorized')) ||
    code === 190
  )
    return 'Tài khoản/token Meta chưa đủ quyền hoặc đã hết hạn với Trang được chọn. Vui lòng kết nối lại hoặc liên hệ admin cấp quyền.';
  if (
    code === 200 ||
    code === 10 ||
    has('do not have permission', 'not authorized', 'permissions error')
  )
    return 'Tài khoản chưa đủ quyền thực hiện thao tác này trên Meta. Vui lòng liên hệ admin.';
  if (
    code === 17 ||
    code === 613 ||
    has('rate limit', 'too many', 'reduce the amount', 'request limit')
  )
    return 'Meta đang giới hạn tần suất (rate limit). Vui lòng đợi vài phút rồi publish lại.';
  return null;
};

/**
 * Phân loại lỗi Meta theo "AI SỬA ĐƯỢC" để nhân sự marketing tự phán đoán:
 *
 *  - DRAFT_CONFIG   🔧  Do bản nháp — sửa 1 thiết lập trong nháp là hết (ngân sách,
 *                        đối tượng, tuổi, ảnh/video, CID/Catalog, bid...). fixableInDraft=true.
 *  - META_LIMITATION 🚫  Meta KHÔNG hỗ trợ / ĐÃ DỪNG hỗ trợ (hoặc tài khoản/Trang chưa
 *                        đủ điều kiện) — KHÔNG sửa được bằng 1 field; phải đổi mục tiêu/
 *                        đích, hoặc bật tính năng trên Trang/Business (vd MPC chưa đủ điều
 *                        kiện, đích IG không khả dụng, targeting đã bị Meta gỡ).
 *                        fixableInDraft=false.
 *  - SYSTEM         ⚙️  Lỗi hệ thống — token/permission app, payload app dựng sai, hoặc
 *                        lỗi CHƯA phân loại. "Không phải lỗi của bạn", cần báo kỹ thuật.
 *  - TRANSIENT      ⏳  Tạm thời — rate limit / Meta outage. Thử lại sau.
 *
 * NGUYÊN TẮC: luôn ưu tiên error_user_msg/error_user_title của Meta (đã localize, an
 * toàn để hiển thị) làm nội dung; ta chỉ phủ thêm NHÓM + cách sửa. Không khớp mẫu nào
 * → mặc định SYSTEM (không đổ lỗi cho user) và nên log lại để bồi catalog.
 *
 * GIỮ SONG SONG với bản ở mb-ads (common/utils/index.ts).
 */
export type MetaErrorCategory =
  | 'DRAFT_CONFIG'
  | 'META_LIMITATION'
  | 'SYSTEM'
  | 'TRANSIENT';

export interface MetaErrorClassification {
  category: MetaErrorCategory;
  fixableInDraft: boolean;
  retryable: boolean;
  title: string; // tiêu đề ngắn cho badge/mục lỗi
  userMessage: string; // nội dung chính (ưu tiên error_user_msg của Meta)
  howToFix: string | null; // hướng dẫn khắc phục (nếu có)
  code?: number;
  subcode?: number;
  fbtrace_id?: string;
}

export const META_ERROR_CATEGORY_LABEL: Record<
  MetaErrorCategory,
  { label: string; hint: string }
> = {
  DRAFT_CONFIG: {
    label: 'Do bản nháp — bạn có thể tự sửa',
    hint: 'Chỉnh lại thiết lập bên dưới trong nháp rồi đăng lại.',
  },
  META_LIMITATION: {
    label: 'Meta không hỗ trợ / đã dừng hỗ trợ',
    hint: 'Cấu hình này Meta không hỗ trợ hoặc đã dừng hỗ trợ (hoặc tài khoản/Trang chưa đủ điều kiện) — không sửa được bằng 1 thiết lập; cần đổi mục tiêu/đích hoặc bật tính năng trên Trang/Business.',
  },
  SYSTEM: {
    label: 'Lỗi hệ thống — không phải lỗi của bạn',
    hint: 'Sự cố này đã được ghi nhận để đội kỹ thuật xử lý.',
  },
  TRANSIENT: {
    label: 'Meta đang bận — thử lại sau',
    hint: 'Đây là giới hạn tần suất/sự cố tạm thời của Meta, hãy thử lại sau ít phút.',
  },
};

export const classifyMetaError = (metaError: any): MetaErrorClassification => {
  const code = Number(metaError?.code);
  const subcode = Number(metaError?.subcode);
  const msg = String(
    metaError?.message || metaError?.raw?.message || '',
  ).toLowerCase();
  const has = (...keys: string[]) => keys.some((k) => msg.includes(k));
  const friendly = metaErrorToFriendly(metaError);
  // Nội dung ưu tiên: error_user_msg (Meta đã localize) > friendly của ta > message thô.
  const metaUserMsg = metaError?.title || metaError?.raw?.error_user_msg;
  const userMessage =
    metaUserMsg || friendly || metaError?.message || 'Lỗi không xác định từ Meta.';

  const build = (
    category: MetaErrorCategory,
    extra: Partial<MetaErrorClassification> = {},
  ): MetaErrorClassification => ({
    category,
    fixableInDraft: category === 'DRAFT_CONFIG',
    retryable: category === 'TRANSIENT',
    title: metaError?.title || META_ERROR_CATEGORY_LABEL[category].label,
    userMessage,
    howToFix: friendly,
    code: Number.isFinite(code) ? code : undefined,
    subcode: Number.isFinite(subcode) ? subcode : undefined,
    fbtrace_id: metaError?.fbtrace_id,
    ...extra,
  });

  // ⏳ TẠM THỜI — rate limit / outage / mạng. Ưu tiên xét trước.
  // isRetryableError đã bao trùm rate-limit (4/17/32/613/80004 + subcode 2446079).
  if (isRetryableError(metaError)) {
    return build('TRANSIENT');
  }

  // 🚫 META KHÔNG HỖ TRỢ / ĐÃ DỪNG HỖ TRỢ (hoặc chưa đủ điều kiện) — không sửa được
  // bằng 1 field trong nháp.
  // LƯU Ý subcode 2490408 ("không dùng được mục tiêu hiệu quả cho mục tiêu chiến dịch")
  // lưỡng nghĩa: ngoài MPC = user CHỌN SAI optimization_goal ↔ objective (tự sửa được →
  // DRAFT_CONFIG, để mặc định bên dưới bắt qua error_user_msg). CHỈ khi optimization_goal
  // = MESSAGING_PURCHASE_CONVERSION nó mới là "Trang chưa đủ điều kiện" (META_LIMITATION);
  // ca đó do CALL-SITE tự nâng category (nó biết optimization_goal, còn hàm này thì không).
  const isEligibility =
    has(
      'not eligible',
      'không đủ điều kiện',
      'is not supported',
      'not supported for',
      'not available for this',
      'cannot be used with',
      'incompatible',
      'không tương thích',
      'không hợp lệ với mục tiêu',
      'no longer available',
      'has been deprecated',
      'đã ngừng hỗ trợ',
      'is deprecated',
    );
  // Subcode targeting/đặc tính đã bị Meta gỡ hoặc không khả dụng cho tài khoản.
  const deprecatedSubcodes = [1487694, 1870088, 1870065, 2446394];
  if (isEligibility || deprecatedSubcodes.includes(subcode)) {
    return build('META_LIMITATION');
  }

  // ⚙️ LỖI HỆ THỐNG — token/permission app hoặc payload app dựng sai. User không tự sửa.
  const isAuthOrPermission =
    [190, 102, 10, 200, 294, 368].includes(code) ||
    subcode === 458 ||
    subcode === 459 ||
    subcode === 460 ||
    subcode === 463 ||
    subcode === 467 ||
    subcode === 492 ||
    has(
      'access token',
      'session has expired',
      'do not have permission',
      'not authorized',
      'permissions error',
      'ads_management',
    );
  if (isAuthOrPermission) {
    return build('SYSTEM');
  }

  // 🔧 DO BẢN NHÁP — lỗi validation (Meta thường kèm error_user_msg) hoặc khớp một
  // trong các mẫu friendly (ngân sách/ảnh/đối tượng/Catalog/special ad category...).
  const validationCodes = [100, 1487, 1885, 1870, 2446, 1359, 1443, 1815];
  const looksValidation =
    friendly !== null ||
    !!metaError?.raw?.error_user_msg ||
    validationCodes.some((c) => code === c || Math.floor(code / 1000) === c);
  if (looksValidation) {
    return build('DRAFT_CONFIG');
  }

  // Không khớp mẫu nào → coi là LỖI HỆ THỐNG (không đổ lỗi cho user) để log & bồi catalog.
  return build('SYSTEM');
};

/**
 * Chọn phân loại ĐẠI DIỆN từ danh sách ad/nhóm lỗi (mỗi phần tử có .classification).
 * Dùng cho badge TỔNG khi lỗi ném lên là wrapper tổng hợp không mang code Meta (vd
 * "Tất cả quảng cáo đều lỗi") — classify wrapper sẽ ra SYSTEM sai. Ưu tiên nhóm HÀNH
 * ĐỘNG ĐƯỢC trước để user thấy việc có thể làm: DRAFT_CONFIG > META_LIMITATION >
 * TRANSIENT > SYSTEM. GIỮ SONG SONG với bản ở mb-ads.
 */
export const pickDominantClassification = (
  list: any[],
): MetaErrorClassification | null => {
  const cls: MetaErrorClassification[] = (Array.isArray(list) ? list : [])
    .map((x) => x?.classification)
    .filter((c) => !!c?.category);
  if (!cls.length) return null;
  const priority: MetaErrorCategory[] = [
    'DRAFT_CONFIG',
    'META_LIMITATION',
    'TRANSIENT',
    'SYSTEM',
  ];
  for (const cat of priority) {
    const hit = cls.find((c) => c.category === cat);
    if (hit) return hit;
  }
  return cls[0];
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

export async function executeMetaApiWithRetry<T>(
  action: () => Promise<T> | T,
  options?: {
    maxRetries?: number;
    initialSleepMs?: number;
    networkSleepMs?: number;
    logger?: any;
    context?: Record<string, any>;
  },
): Promise<T> {
  const maxRetries = options?.maxRetries ?? 2;
  const initialSleepMs = options?.initialSleepMs ?? 60000;
  const networkSleepMs = options?.networkSleepMs ?? 10000;
  let retry = 0;

  while (true) {
    try {
      return await action();
    } catch (error: any) {
      if (isRetryableError(error) && retry < maxRetries) {
        retry++;
        const normalized = normalizeMetaError(error?.response || error);
        const message = (
          normalized.message ||
          error?.message ||
          ''
        ).toLowerCase();
        const isNetworkError =
          !normalized.code &&
          (message.includes('no response was received') ||
            message.includes('timeout') ||
            message.includes('network error') ||
            message.includes('enotfound') ||
            message.includes('econnreset') ||
            message.includes('etimedout') ||
            message.includes('econnaborted') ||
            message.includes('socket hang up'));
        const sleepTime =
          (isNetworkError ? networkSleepMs : initialSleepMs) * retry;
        const errMsg = error?.response?.code
          ? `Code: ${error.response.code}`
          : `Msg: ${error?.message || String(error)}`;
        const contextText = options?.context
          ? ` | Context: ${JSON.stringify(options.context)}`
          : '';
        options?.logger?.warn?.(
          `[Meta API Error] Error hit (${errMsg}). Retrying ${retry}/${maxRetries} after ${Math.round(sleepTime / 1000)}s...${contextText}`,
        );
        await sleep(sleepTime);
      } else {
        throw error;
      }
    }
  }
}

/**
 * Retry for Database operations (Prisma)
 */
export async function executeDbWithRetry<T>(
  action: () => Promise<T> | T,
  options?: {
    maxRetries?: number;
    initialSleepMs?: number;
    logger?: any;
  },
): Promise<T> {
  const maxRetries = options?.maxRetries ?? 3;
  const initialSleepMs = options?.initialSleepMs ?? 5000;
  let retry = 0;

  while (true) {
    try {
      return await action();
    } catch (error: any) {
      const isDbStarting =
        error?.message?.includes('not yet accepting connections') ||
        error?.cause?.message?.includes('not yet accepting connections') ||
        error?.code === '57P03';

      if (isDbStarting && retry < maxRetries) {
        retry++;
        const sleepTime = initialSleepMs * retry;
        options?.logger?.warn?.(
          `[DB Connection] Database is starting up or busy (57P03). Retrying ${retry}/${maxRetries} after ${Math.round(sleepTime / 1000)}s...`,
        );
        await sleep(sleepTime);
      } else {
        throw error;
      }
    }
  }
}
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

  const sleepMs = options?.sleepMs ?? 1000; // Nghỉ 1s giữa các page bình thường để tránh dồn dập
  const rateLimitSleepMs = 60000; // Khi dính Rate Limit sẽ nghỉ 60s
  const maxRetries = options?.maxRetries ?? 2;

  let page = cursor;
  let retry = 0;

  // 1. Lấy dữ liệu trang đầu tiên
  for (const item of page) {
    result.push(item._data);
  }

  try {
    // 2. Vòng lặp lấy các trang tiếp theo
    while (page.hasNext()) {
      // Nghỉ trước khi fetch trang tiếp theo theo yêu cầu
      await sleep(sleepMs);

      try {
        page = await page.next();
        retry = 0; // Reset retry khi fetch thành công trang mới

        for (const item of page) {
          result.push(item._data);
        }
      } catch (err) {
        const metaErr = normalizeMetaError(err);

        const isRateLimit = [4, 17].includes(metaErr.code);
        const isNetworkError =
          !metaErr.code &&
          (metaErr.message?.includes('no response was received') ||
            metaErr.message?.includes('timeout') ||
            metaErr.message?.includes('Network Error') ||
            metaErr.message?.includes('ENOTFOUND') ||
            metaErr.message?.includes('ECONNRESET') ||
            metaErr.message?.includes('socket hang up'));

        // Xử lý Rate Limit (Lỗi 4 hoặc 17) hoặc lỗi kết nối mạng tạm thời
        if ((isRateLimit || isNetworkError) && retry < maxRetries) {
          retry++;
          const waitTime = isRateLimit
            ? rateLimitSleepMs * retry
            : 10000 * retry; // Lỗi mạng thì chờ 10s, 20s
          const contextText = options?.context
            ? ` Context: ${JSON.stringify(options.context)}`
            : '';
          console.warn(
            `[Meta] Fetch error (RateLimit: ${isRateLimit}, NetworkError: ${isNetworkError}). Retrying in ${waitTime}ms... (Attempt ${retry}/${maxRetries}).${contextText}`,
          );
          await sleep(waitTime);
          // Quay lại đầu vòng lặp while để thử lại page.next()
          continue;
        }

        // Với các lỗi khác (bao gồm "reduce the amount of data", code 1/2, permission):
        // KHÔNG nuốt lỗi rồi trả về result từng phần (gây thiếu DAILY -> blank/sai trên màn hình).
        // Ném lỗi gốc ra ngoài để caller xử lý: adaptive splitter chia nhỏ request,
        // per-chunk handler bỏ qua chunk lỗi, hoặc đánh dấu needsReauth khi permission.
        console.error(
          `[Meta Fetch Error] Surfacing error after ${retry} retries: ${metaErr.message}`,
          metaErr,
        );
        throw err;
      }
    }
  } catch (globalErr) {
    // Không nuốt lỗi: propagate để BullMQ retry / caller xử lý thay vì trả về result thiếu.
    throw globalErr;
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

  // ===== PURCHASE (COUNT) =====
  const purchases =
    getActionValue(insight?.actions, 'purchase') +
    getActionValue(insight?.actions, 'onsite_conversion.purchase');

  // ===== PURCHASE VALUE (MONEY – đã quy đổi ở tầng fetch) =====
  const purchaseValue =
    getActionValueFromValues(insight?.action_values, 'purchase') +
    getActionValueFromValues(
      insight?.action_values,
      'onsite_conversion.purchase',
    );

  // ===== ROAS (❗ CHUẨN CỦA CTY) =====
  const roasCalculated = spend > 0 ? purchaseValue / spend : 0;
  const roas = roasCalculated;

  // ===== DERIVED =====
  const cvr = clicks > 0 ? registrationComplete / clicks : 0;

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

  const resultsFinal = Math.round(
    Number(purchases) + Number(registrationComplete),
  );
  const aov =
    resultsFinal > 0 ? Math.round(Number(purchaseValue) / resultsFinal) : null;

  const costPerResultFinal = resultsFinal > 0 ? spend / resultsFinal : 0;

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
    impressions: Math.round(impressions),
    reach: Math.round(reach),
    frequency: toNumber(insight?.frequency),

    clicks: Math.round(clicks),
    uniqueClicks: Math.round(toNumber(insight?.unique_clicks)),

    ctr: toNumber(insight?.ctr),
    uniqueCtr: toNumber(insight?.unique_ctr),

    cpc: toNumber(insight?.cpc),
    cpm: toNumber(insight?.cpm),

    spend,

    results: resultsFinal,
    aov,
    costPerResult: costPerResultFinal,

    purchases: Math.round(purchases),
    purchaseValue,
    roas,

    cvr,
    adsCostRatio,

    registrationComplete: Math.round(registrationComplete),
    registrationCompleteValue,

    messagingStarted: Math.round(messagingStarted),
    messagingStartedValue,
    outboundClicks: Math.round(outboundClicks),
    outboundClicksValue,

    // ===== VIDEO =====
    videoPlay,
    video3s,
    videoThruplay: Math.round(videoThruplay),
    video100,
    videoAvgWatchTime,

    hookRate, // %
    holdRate, // %

    qualityRanking: insight?.quality_ranking ?? null,
    engagementRateRanking: insight?.engagement_rate_ranking ?? null,
    conversionRateRanking: insight?.conversion_rate_ranking ?? null,

    // DEBUG / RAW
    actions: insight?.actions ? toPrismaJson(insight.actions) : null,
    actionValues: insight?.action_values
      ? toPrismaJson(insight.action_values)
      : null,
  };
}

export async function fetchAllWithAPIEndpoint(initialResponse: any) {
  let results: any[] = [];
  let response = initialResponse;

  while (true) {
    const data = response?.data || [];
    results = results.concat(data);

    const next = response?.paging?.next;
    if (!next) break;

    // gọi next page bằng URL
    response = await fetch(next).then((res) => res.json());
  }

  return results;
}

export type FolderNode = {
  id: string;
  name: string;
  description?: string;
  creation_time?: string;
  parent_folder?: { id: string; name: string };
  subfolders?: { data: FolderNode[] };
};

export type CreativeAssetResponse = {
  data?: CreativeAsset[];
  paging?: { next?: string };
};

export function buildSubfolderFields(depth: number): string {
  if (depth === 0) return '';
  const child = buildSubfolderFields(depth - 1);

  return `subfolders.limit(100){id,description,name,creation_time,parent_folder{ id, name }  ${child ? `, ${child}` : ''}}`;
}

export function flattenFolders(
  folders: FolderNode[],
  parentId: string | null = null,
  result: CreativeFolder[],
) {
  for (const folder of folders) {
    result.push({
      id: folder.id,
      name: folder.name,
      description: folder?.description!,
      parentId: folder.parent_folder?.id || parentId,
      creation_time: folder?.creation_time!,
      createdAtLocal: new Date(),
      updatedAt: new Date(),
      status: FolderStatus.ACTIVE,
    });

    if (folder.subfolders?.data?.length) {
      flattenFolders(folder.subfolders.data, folder.id, result);
    }
  }
  return result;
}

export function parseMetaUrlExpireTime(
  url?: string | (string | undefined)[],
): Date | null {
  if (!url) return null;

  const urls = Array.isArray(url) ? url : [url];
  const validUrls = urls.filter(Boolean) as string[];

  if (validUrls.length === 0) return null;

  let earliest: Date | null = null;

  for (const u of validUrls) {
    const match = u.match(/[?&]oe=([0-9A-Fa-f]+)/);
    if (!match) continue;
    try {
      const timestamp = parseInt(match[1], 16);
      const date = new Date(timestamp * 1000);
      if (!earliest || date < earliest) {
        earliest = date;
      }
    } catch (err) {
      // ignore
    }
  }

  if (!earliest) return new Date('2099-12-31');

  return earliest;
}
