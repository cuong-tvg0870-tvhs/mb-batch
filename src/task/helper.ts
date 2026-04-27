import { LarkRecord } from '@prisma/client';
import { toPrismaJson } from 'src/common/utils';

// helpers
export function getText(field?: TextField) {
  return field?.value?.[0]?.text || null;
}

export function getArrayText(field?: ArrayTextField) {
  return field?.[0]?.text || null;
}

export function getSelect(field?: SelectField) {
  return field?.value?.[0] || null;
}

export function getLink(field?: LinkField) {
  return field?.[0]?.link || field?.[0]?.text || null;
}

// mapper
export function mapRecord(item: RecordItem) {
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
