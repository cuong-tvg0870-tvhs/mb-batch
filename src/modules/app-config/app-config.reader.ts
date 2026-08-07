import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

interface CacheItem {
  value: any; // raw JSON từ SystemConfig (undefined = chưa có row)
  at: number;
}

/**
 * Reader NHẸ cho các "knob sản phẩm" trong SystemConfig (PARITY với mb-ads
 * AppConfigService — nhưng mb-batch chỉ ĐỌC, không có registry/admin API/UI; ngoại lệ
 * DUY NHẤT là `setString` cho knob TỰ HỌC, xem chú thích tại chỗ). Precedence:
 * DB override → env fallback → default. Cache 30s để không đọc DB mỗi lượt cron.
 * ⚠️ Key + default phải khớp registry mb-ads (app-config.registry.ts) — sửa 1 bên soi bên kia.
 */
@Injectable()
export class AppConfigReader {
  private readonly ttlMs = 30_000;
  private readonly cache = new Map<string, CacheItem>();

  constructor(private readonly prisma: PrismaService) {}

  private async readRaw(key: string): Promise<any> {
    const now = Date.now();
    const hit = this.cache.get(key);
    if (hit && now - hit.at < this.ttlMs) return hit.value;
    let value: any;
    try {
      const row = await this.prisma.systemConfig.findUnique({
        where: { key },
        select: { value: true },
      });
      value = row ? row.value : undefined;
    } catch {
      value = undefined; // fail-open → rơi về env/default
    }
    this.cache.set(key, { value, at: now });
    return value;
  }

  private unwrap(raw: any): any {
    if (raw === undefined || raw === null) return undefined;
    return typeof raw === 'object' && 'value' in raw ? raw.value : raw;
  }

  async getNumber(key: string, def: number, envVar?: string): Promise<number> {
    const v = this.unwrap(await this.readRaw(key));
    if (v !== undefined && v !== null) {
      const n = Number(v);
      if (Number.isFinite(n)) return n;
    }
    if (envVar) {
      const e = Number(process.env[envVar]);
      if (Number.isFinite(e)) return e;
    }
    return def;
  }

  async getBoolean(
    key: string,
    def: boolean,
    envVar?: string,
  ): Promise<boolean> {
    const v = this.unwrap(await this.readRaw(key));
    if (v !== undefined && v !== null) {
      return typeof v === 'boolean' ? v : String(v).toLowerCase() === 'true';
    }
    if (envVar) {
      const e = process.env[envVar];
      if (e !== undefined && e !== '') return e === 'true';
    }
    return def;
  }

  /**
   * Knob kiểu CHUỖI, dùng cho các knob NHIỀU TRẠNG THÁI (vd
   * `quick_contact_capability`: 'UNKNOWN' | 'AVAILABLE' | 'UNAVAILABLE').
   * CỐ Ý không nhét mấy knob này vào getBoolean: getBoolean biến 'UNAVAILABLE' thành
   * false (vì chuỗi !== 'true') nên "chưa biết" và "không có quyền" thành một → lần
   * deploy đầu tiên sẽ khoá oan hoặc mở oan. Precedence giữ y hệt hai getter kia:
   * DB override → env → default; chuỗi rỗng coi như CHƯA SET để rơi tiếp về default
   * (row còn sót value='' không được biến thành trạng thái thứ tư).
   */
  async getString(key: string, def: string, envVar?: string): Promise<string> {
    const v = this.unwrap(await this.readRaw(key));
    if (v !== undefined && v !== null && String(v) !== '') return String(v);
    if (envVar) {
      const e = process.env[envVar];
      if (e !== undefined && e !== '') return e;
    }
    return def;
  }

  /**
   * NGOẠI LỆ DUY NHẤT của nguyên tắc "mb-batch chỉ đọc": knob TỰ HỌC — cron publish
   * gặp lỗi Meta chứng minh ỨNG DỤNG thiếu năng lực thì ghi lại kết luận đó để CẢ HAI
   * backend ngừng gửi field gây lỗi (xem draft-automation-meta-publisher.service.ts
   * #learnQuickContactUnavailable). Đây KHÔNG phải cửa cấu hình chung: admin vẫn chỉnh
   * knob bên mb-ads (registry + UI), mb-batch chỉ ghi thứ máy tự rút ra từ lỗi thật.
   *
   * Lưu CHUỖI THÔ (không bọc `{ value }`) cho khớp AppConfigService.set() của mb-ads —
   * decode hai bên đọc được cả hai dạng, nhưng thống nhất MỘT dạng thì cùng một khoá
   * không sinh ra hai kiểu dữ liệu khi admin sửa tay xen kẽ với ghi tự động.
   * `description` chỉ để người mở DB đọc hiểu (mb-ads.set() cũng ghi nhãn registry vào
   * đây) và CHỈ ghi lúc tạo mới, không đè nhãn admin đã sửa.
   *
   * Bust cache LOCAL ngay: những quảng cáo còn lại của chính lượt cron này thấy giá trị
   * mới tức thì (đó là cách 1 lỗi đầu tiên cứu được các ad sau trong cùng chiến dịch).
   * ⚠️ Cache của mb-ads nằm ở TIẾN TRÌNH KHÁC nên vẫn trễ ≤30s — đừng thiết kế luồng
   * cần hiệu lực tức thì cross-process.
   */
  async setString(key: string, value: string, description?: string) {
    await this.prisma.systemConfig.upsert({
      where: { key },
      create: { key, value, description },
      update: { value },
    });
    this.cache.delete(key);
  }
}
