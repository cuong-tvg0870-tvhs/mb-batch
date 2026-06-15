import { LarkRecord } from '@prisma/client';
import { toPrismaJson } from '../../common/utils';

const MAX_PERMISSION_RETRIES = 10;
const PUBLIC_ONLY_PERMISSION_ERROR =
  'File is public but not shared/added to service account Drive scope';
const RETRY_DELAYS_MINUTES = [
  30, 60, 120, 240, 480, 720, 1440, 1440, 1440, 1440,
];

// helpers
export function getText(field?: any) {
  return field?.value?.[0]?.text || null;
}

export function getArrayText(field?: any) {
  return field?.[0]?.text || null;
}

export function getSelect(field?: any) {
  return field?.value?.[0] || null;
}

export function getLink(field?: any) {
  return field?.[0]?.link || field?.[0]?.text || null;
}

export function extractDriveId(url?: string | null): string | null {
  if (!url) return null;

  const match1 = url.match(/\/d\/([a-zA-Z0-9_-]+)/);
  if (match1) return match1[1];

  const match2 = url.match(/[?&]id=([a-zA-Z0-9_-]+)/);
  if (match2) return match2[1];

  return null;
}

export function parseAllowedSharedDriveIds(value?: string | null): Set<string> {
  return new Set(
    (value || '')
      .split(',')
      .map((id) => id.trim())
      .filter(Boolean),
  );
}

export function hasExplicitDriveAccess(
  file: any,
  allowedSharedDriveIds: Set<string>,
): boolean {
  if (!file) return false;
  if (file.ownedByMe === true) return true;
  if (file.sharedWithMeTime) return true;
  if (file.driveId && allowedSharedDriveIds.has(file.driveId)) return true;

  return false;
}

export function isPermissionCheckDue(raw: any, now = new Date()): boolean {
  const retryCount = Number(raw?.retry_count || 0);
  if (retryCount >= MAX_PERMISSION_RETRIES) return false;
  if (raw?.permission_status !== 'FAILED') return true;
  if (!raw?.last_checked_at) return true;

  const lastCheckedAt = new Date(raw.last_checked_at).getTime();
  if (Number.isNaN(lastCheckedAt)) return true;

  const delayMinutes =
    RETRY_DELAYS_MINUTES[Math.max(0, retryCount - 1)] ||
    RETRY_DELAYS_MINUTES[RETRY_DELAYS_MINUTES.length - 1];
  const nextCheckAt = lastCheckedAt + delayMinutes * 60 * 1000;

  return now.getTime() >= nextCheckAt;
}

export function buildPermissionRawUpdate(
  raw: any,
  success: boolean,
  error: string | null,
  now = new Date(),
) {
  const previousRetryCount = Number(raw?.retry_count || 0);
  return {
    ...(raw || {}),
    permission_status: success ? 'SUCCESS' : 'FAILED',
    permission_error: error,
    permission_access_verified: success,
    retry_count: success ? 0 : previousRetryCount + 1,
    last_checked_at: now.toISOString(),
  };
}

export { PUBLIC_ONLY_PERMISSION_ERROR };

// mapper
export function mapRecord(item: any) {
  const f = item.fields || {};
  const driveUrl = getLink(f['Link Content']);
  return {
    id: item?.record_id,
    raw: toPrismaJson(item),
    cid: f['ID Content']!,

    project_name: f['Dự án'],
    project_code: f['Dự án'],

    brand_name: getSelect(f['Thương hiệu']),
    brand_code: getSelect(f['Thương hiệu']),

    product_code: getText(f['Mã sản phẩm']),
    product_name: getText(f['Sản phẩm (f)']),

    employee_id: getArrayText(f['ID MC']),
    employee_name: getText(f['Họ và tên']),

    drive_url: driveUrl,
    drive_id: extractDriveId(driveUrl),

    production_date: f['Ngày sản xuất'] ? new Date(f['Ngày sản xuất']) : null,
    creative_asset_id: null,
  } as LarkRecord;
}
