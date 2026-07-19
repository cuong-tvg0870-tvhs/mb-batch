/**
 * Tiện ích kiểm quyền quảng bá Trang theo TKQC dựa trên `Account.promotePages`
 * (Meta edge `promote_pages`, shape `[{ id, name }]`). Dùng để chặn auto-publish
 * từ bài cũ khi TKQC đích KHÔNG được cấp quyền trên Trang của bài → tránh 1815017.
 *
 * NGUYÊN TẮC (user chốt): CHỈ chặn khi promotePages ĐÃ sync (có dữ liệu) và pageId
 * không thuộc. Account chưa sync (null/rỗng) ⇒ "chưa biết" ⇒ KHÔNG chặn (fallback).
 *
 * Bản sao 1:1 của mb-ads `common/utils/promote-pages.util.ts` (parity 2 writer).
 */

/** Đọc `Account.promotePages` (JSON) → Set<pageId>. null/rỗng/không hợp lệ → null. */
export function toPromotePageIdSet(promotePages: unknown): Set<string> | null {
  if (!Array.isArray(promotePages) || promotePages.length === 0) return null;
  const ids = promotePages
    .map((p: any) =>
      p && typeof p === 'object' ? String(p.id ?? '') : String(p ?? ''),
    )
    .filter((id) => id.length > 0);
  return ids.length ? new Set(ids) : null;
}

/**
 * TKQC có quyền quảng bá Trang `pageId` không?
 * - `pageId` rỗng ⇒ true. promotePages chưa sync (null) ⇒ true (fallback).
 * - Ngược lại ⇒ true CHỈ khi pageId ∈ promotePages.
 */
export function accountCanPromotePage(
  promotePages: unknown,
  pageId: string | null | undefined,
): boolean {
  if (!pageId) return true;
  const set = toPromotePageIdSet(promotePages);
  if (!set) return true;
  return set.has(pageId);
}

/** pageId từ object_story_id `{pageId}_{postId}` (null nếu không đúng dạng). */
export function pageIdFromStory(story: string | null | undefined): string | null {
  if (!story) return null;
  const idx = story.indexOf('_');
  return idx > 0 ? story.slice(0, idx) : null;
}
