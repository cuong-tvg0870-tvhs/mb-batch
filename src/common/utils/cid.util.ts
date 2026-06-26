// CID là cấu trúc đặt tên nội bộ của công ty, vd: CID00046478.
// Một nguồn chân lý duy nhất cho việc trích / cắt / so khớp CID, dùng chung cho
// luồng upload (meta-media-upload) và luồng tự động hóa tạo nháp (draft-automation).
export const CID_PATTERN = /CID\d+/i;

// Trích token CID đầy đủ (kèm tiền tố "CID") từ một tên bất kỳ, in hoa.
export function extractCidFromName(name?: string | null): string | null {
  const match = (name || '').match(CID_PATTERN);
  return match ? match[0].toUpperCase() : null;
}

// Cắt CID ra khỏi tên, trả về cả CID và phần tên còn lại đã dọn dấu phân tách thừa.
export function stripCidFromName(name?: string | null): {
  cid: string | null;
  stripped: string;
} {
  const raw = name || '';
  const cid = extractCidFromName(raw);
  if (!cid) return { cid: null, stripped: raw.trim() };
  const stripped = raw
    .replace(CID_PATTERN, '')
    .replace(/[\s_\-|]{2,}/g, ' ')
    .replace(/^[\s_\-|]+|[\s_\-|]+$/g, '')
    .trim();
  return { cid, stripped };
}

// Tên file chỉ hợp lệ khi chứa đúng CID của Lark record (CID00046478).
export function fileMatchesRecordCid(
  fileName?: string | null,
  recordCid?: string | null,
): boolean {
  const expected = (recordCid || '').trim().toUpperCase();
  if (!expected) return false;
  const found = extractCidFromName(fileName);
  return found !== null && found === expected;
}

// Ghép CID vào đúng vị trí CID của tên quảng cáo theo cấu trúc đặt tên:
//   MÃ_DỰ_ÁN | MSNV | MÃ_SẢN_PHẨM | LOẠI_HÌNH | CID |
// Ưu tiên thay token CID/placeholder sẵn có; nếu không có thì dùng vị trí thứ 5.
export function applyCidToAdName(name: string, cid: string): string {
  if (!name || !cid) return name;
  const parts = name.split('|');
  let idx = parts.findIndex(
    (p) => CID_PATTERN.test(p.trim()) || p.trim() === '[MÃ_CID_CONTENT]',
  );
  if (idx === -1) {
    // Chỉ dùng vị trí mặc định (thứ 5) cho tên quảng cáo dạng pipe; tránh đụng
    // tên dạng campaign/adset có sentinel AUTO_ADS.
    if (parts.length >= 5 && parts[5]?.trim() !== 'AUTO_ADS') idx = 4;
    else return name;
  }
  parts[idx] = cid;
  return parts.join('|');
}
