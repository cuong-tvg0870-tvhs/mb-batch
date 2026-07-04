import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

interface CacheItem {
  value: any; // raw JSON từ SystemConfig (undefined = chưa có row)
  at: number;
}

/**
 * Reader NHẸ cho các "knob sản phẩm" trong SystemConfig (PARITY với mb-ads
 * AppConfigService — nhưng mb-batch chỉ ĐỌC, không có registry/admin API/UI). Precedence:
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
}
