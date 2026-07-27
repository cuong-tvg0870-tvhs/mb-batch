/**
 * Khớp tên asset với `nameRule` của Test content tự động. Rule nhận NHIỀU mã cách nhau
 * dấu phẩy — "SP00019, SP00020" khớp nếu tên chứa BẤT KỲ mã nào (OR), cùng ngữ nghĩa
 * với ô lọc mã của "Scale bài hiệu quả" (readFilters trong auto-launch-selection.service).
 *
 * Vì sao nhiều mã: một sản phẩm thường có nhiều mã POS (hàng local/trung, mã cũ hết
 * hàng thay bằng mã mới), marketer cần gom hết về một automation.
 *
 * Tương thích ngược: một mã đơn không có dấu phẩy chạy y hệt hành vi trước đây.
 * Rỗng / chỉ toàn dấu phẩy ⇒ KHÔNG lọc (true) — nếu trả false thì ô trống sẽ âm thầm
 * loại sạch content, đúng kiểu lỗi không ai nhìn ra.
 *
 * ⚠️ PARITY: bản sao byte-identical ở
 *   mb-ads/src/modules/draft-campaign/name-term-match.ts
 *   mb-batch/src/modules/draft-automation/name-term-match.ts
 * Lệch nhau thì "Chạy thử" (mb-ads) và lịch chạy thật (mb-batch) chọn ra hai tập
 * content khác nhau — loại bug rất khó truy.
 */
export function matchesAnyNameTerm(assetName: any, nameRule: any): boolean {
  const terms = String(nameRule ?? '')
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter((s) => s.length > 0);
  if (!terms.length) return true;
  const name = String(assetName ?? '').toLowerCase();
  return terms.some((t) => name.includes(t));
}

// =============================================================================
// Bộ lọc tên NÂNG CAO (Bao gồm + Loại trừ) + KHOẢNG thời gian tạo content cho
// "Test content tự động". Mở rộng matchesAnyNameTerm (giữ nguyên ở trên): thêm
// chiều Loại trừ và lọc theo khoảng thời gian tạo (mở 2 đầu tuỳ ý). Mọi helper
// THUẦN + back-compat 100% với field legacy (nameRule CSV, assetCreatedAfter 1 mốc).
//
// Ưu tiên: field mới non-null ⇒ dùng field mới, BỎ QUA legacy; absent/null ⇒ fold
// legacy. Triết lý match GIỮ NGUYÊN: lowercase + substring includes(), KHÔNG strip
// dấu tiếng Việt, KHÔNG regex. Include rỗng ⇒ pass; Exclude áp dụng độc lập và
// THẮNG include khi trùng term.
//
// ⚠️ PARITY: cùng ràng buộc byte-identical như matchesAnyNameTerm (2 repo). Lệch
// nhau thì "Chạy thử" (mb-ads) và lịch chạy thật (mb-batch) chọn ra hai tập content
// khác nhau.
// =============================================================================

export type NameFilterConfig =
  | { include?: string[] | null; exclude?: string[] | null }
  | null
  | undefined;
export type CreatedRangeConfig =
  | { from?: string | null; to?: string | null }
  | null
  | undefined;
// Đã trim + lowercase + bỏ rỗng + khử trùng — sẵn sàng cho matchesNameFilter.
export type NormalizedNameFilter = { include: string[]; exclude: string[] };

// Chuẩn hoá 1 mảng term: trim + lowercase + bỏ rỗng + khử trùng, giữ thứ tự.
function normalizeTerms(arr: unknown): string[] {
  if (!Array.isArray(arr)) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const item of arr) {
    const t = String(item ?? '')
      .trim()
      .toLowerCase();
    if (!t || seen.has(t)) continue;
    seen.add(t);
    out.push(t);
  }
  return out;
}

// nameFilter non-null ⇒ dùng include/exclude của nó, BỎ QUA nameRule legacy.
// Absent/null ⇒ fold legacy: nameRule CSV → include (split dấu phẩy), exclude = [].
export function normalizeNameFilter(
  nameFilter: NameFilterConfig,
  legacyNameRule?: string | null,
): NormalizedNameFilter {
  if (nameFilter != null) {
    return {
      include: normalizeTerms(nameFilter.include),
      exclude: normalizeTerms(nameFilter.exclude),
    };
  }
  return {
    include: normalizeTerms(String(legacyNameRule ?? '').split(',')),
    exclude: [],
  };
}

// Tên KHÔNG đạt điều kiện Bao gồm: include rỗng ⇒ luôn đạt (false). Có include mà
// tên không chứa BẤT KỲ term nào ⇒ true (fail). Tách riêng để đếm bucket độc lập.
export function failsNameInclude(
  assetName: unknown,
  f: NormalizedNameFilter,
): boolean {
  if (!f.include.length) return false;
  const name = String(assetName ?? '').toLowerCase();
  return !f.include.some((t) => name.includes(t));
}

// Tên DÍNH từ khoá Loại trừ: chứa BẤT KỲ term exclude nào ⇒ true. Exclude rỗng ⇒
// false. Áp dụng độc lập kể cả khi include rỗng (exclude thắng include).
export function hitsNameExclude(
  assetName: unknown,
  f: NormalizedNameFilter,
): boolean {
  if (!f.exclude.length) return false;
  const name = String(assetName ?? '').toLowerCase();
  return f.exclude.some((t) => name.includes(t));
}

// Đạt bộ lọc tên = qua Bao gồm VÀ không dính Loại trừ.
export function matchesNameFilter(
  assetName: unknown,
  f: NormalizedNameFilter,
): boolean {
  return !failsNameInclude(assetName, f) && !hitsNameExclude(assetName, f);
}

// Parse ngày tạo asset: chấp nhận Date, chuỗi ISO, HOẶC chuỗi/số epoch-SECONDS
// thuần số (×1000 — khớp cách mb-ads xử lý creation_time hiện tại). Rỗng / không
// parse được ⇒ null.
export function parseAssetDate(value: unknown): Date | null {
  if (value == null) return null;
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }
  const raw = typeof value === 'number' ? value : String(value);
  if (typeof raw === 'string' && raw.trim() === '') return null;
  const asNumber = Number(raw);
  const date = new Date(
    Number.isNaN(asNumber) ? (raw as string) : asNumber * 1000,
  );
  return Number.isNaN(date.getTime()) ? null : date;
}

// Ngày tạo hiệu lực của 1 asset: creation_time (Meta) ưu tiên, fallback
// createdAtLocal rồi createdAt. Giữ đúng thứ tự ưu tiên như engine hiện tại.
export function getAssetCreatedDate(
  asset:
    | { creation_time?: unknown; createdAtLocal?: unknown; createdAt?: unknown }
    | null
    | undefined,
): Date | null {
  if (!asset) return null;
  return (
    parseAssetDate(asset.creation_time) ||
    parseAssetDate(asset.createdAtLocal) ||
    parseAssetDate(asset.createdAt)
  );
}

// Chuẩn hoá khoảng thời gian tạo. range có ÍT NHẤT 1 bound (from/to non-null) ⇒
// dùng range, BỎ QUA legacy. Absent/null/rỗng ⇒ fold legacy: mốc "sau ngày" đầu
// tiên tìm được (assetCreatedAfter || assetCreationTimeFrom || creationTimeFrom)
// → from, to = null. Trả về Date đã parse (bound không parse được → null).
export function normalizeCreatedRange(
  range: CreatedRangeConfig,
  ...legacyFrom: Array<string | null | undefined>
): { from: Date | null; to: Date | null } {
  const hasBound = range != null && (range.from != null || range.to != null);
  if (hasBound) {
    return {
      from: range!.from != null ? parseAssetDate(range!.from) : null,
      to: range!.to != null ? parseAssetDate(range!.to) : null,
    };
  }
  for (const legacy of legacyFrom) {
    const d = legacy != null ? parseAssetDate(legacy) : null;
    if (d) return { from: d, to: null };
  }
  return { from: null, to: null };
}

// assetDate có nằm trong khoảng? Không bound nào ⇒ true (không lọc). from > to (cả
// 2 set) ⇒ range vô nghĩa, coi như KHÔNG lọc (an toàn, không loại sạch pool). Có
// bound mà asset thiếu ngày ⇒ false (defensive). So sánh INCLUSIVE 2 đầu.
export function inCreatedRange(
  assetDate: Date | null,
  range: { from: Date | null; to: Date | null },
): boolean {
  const { from, to } = range;
  if (!from && !to) return true;
  if (from && to && from.getTime() > to.getTime()) return true;
  if (!assetDate) return false;
  const t = assetDate.getTime();
  if (from && t < from.getTime()) return false;
  if (to && t > to.getTime()) return false;
  return true;
}
