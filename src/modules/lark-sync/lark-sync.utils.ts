import { LarkRecord } from '@prisma/client';
import { toPrismaJson } from '../../common/utils';

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

// mapper
export function mapRecord(item: any) {
  const f = item.fields || {};
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

    drive_url: getLink(f['Link Content']),

    production_date: f['Ngày sản xuất'] ? new Date(f['Ngày sản xuất']) : null,
    creative_asset_id: null,
  } as LarkRecord;
}
