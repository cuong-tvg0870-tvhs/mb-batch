export const CREATIVE_MEDIA_QUEUE = 'creative-media-queue';

export const CREATIVE_MEDIA_JOBS = {
  // Re-host thumbnail VIDEO (URL trong DB do meta-media-sync giữ tươi) → ghi ownedImageUrl.
  REHOST_CREATIVE_IMAGES: 'rehost-creative-images',
  // Re-host creative ORPHAN (SHARE/STATUS/PHOTO… không có videoId/imageId): URL DB đã chết →
  // gọi Meta lấy thumbnail_url tươi 1 lần → re-host vĩnh viễn (đã host là không lấy lại nữa).
  REHOST_ORPHAN_IMAGES: 'rehost-orphan-images',
};
