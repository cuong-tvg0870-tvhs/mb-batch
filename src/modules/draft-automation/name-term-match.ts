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
