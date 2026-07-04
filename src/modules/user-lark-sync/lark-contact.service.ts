import { Injectable, Logger } from '@nestjs/common';
import axios from 'axios';

/**
 * ⚠️ PARITY: bản sao của mb-ads/src/modules/larkbase/lark-contact.service.ts.
 * Hai writer (mb-ads tay + mb-batch cron) dùng CHUNG 1 DB → mapping/logic phải
 * giống hệt. Sửa bên nào thì đồng bộ bên kia.
 */
export interface LarkContactUser {
  name: string;
  employeeNo: string | null;
  avatar: string | null;
  openId: string | null;
  userId: string | null;
  unionId: string | null;
  mobile: string | null;
  jobTitle: string | null;
  departmentIds: string[];
  raw: Record<string, any>;
}

/** Lark contact → field ghi vào bảng User. Giữ parity với mb-ads. */
export function larkContactToUserData(
  lark: LarkContactUser,
  syncedAt: Date,
): Record<string, any> {
  const data: Record<string, any> = {
    avatar: lark.avatar ?? null,
    larkOpenId: lark.openId ?? null,
    larkUserId: lark.userId ?? null,
    larkUnionId: lark.unionId ?? null,
    larkMobile: lark.mobile ?? null,
    larkJobTitle: lark.jobTitle ?? null,
    larkDepartmentIds: lark.departmentIds ?? [],
    larkRaw: lark.raw ?? {},
    larkSyncedAt: syncedAt,
  };
  if (lark.name) data.name = lark.name;
  if (lark.employeeNo) data.employee_id = lark.employeeNo;
  return data;
}

/**
 * Email hệ thống MB = enterprise_email của Lark → batch_get_id KHÔNG hỗ trợ.
 * Tải danh bạ trong phạm vi app (scopes → users/batch), map theo
 * email + enterprise_email (chữ thường), cache TTL. Giữ parity với mb-ads.
 */
@Injectable()
export class LarkContactService {
  private readonly logger = new Logger(LarkContactService.name);
  private readonly host = 'https://open.larksuite.com';

  // Domain email công ty coi là ALIAS của nhau (cùng local-part = cùng người).
  // Override qua env LARK_COMPANY_EMAIL_DOMAINS. Giữ parity với mb-ads.
  private readonly companyDomains = (
    process.env.LARK_COMPANY_EMAIL_DOMAINS || 'tvhs.asia,thanhvinhgroup.com'
  )
    .split(',')
    .map((d) => d.trim().toLowerCase())
    .filter(Boolean);

  private accessToken: string | null = null;
  private expireAt = 0;

  private directory: Map<string, LarkContactUser> | null = null;
  private directoryAt = 0;
  private readonly DIR_TTL_MS = 10 * 60 * 1000;

  private async getAccessToken(): Promise<string> {
    const now = Date.now();
    if (this.accessToken && now < this.expireAt - 60000) {
      return this.accessToken;
    }

    const res = await axios.post(
      `${this.host}/open-apis/auth/v3/tenant_access_token/internal`,
      {
        app_id: process.env.LARK_APP_ID,
        app_secret: process.env.LARK_APP_SECRET,
      },
    );

    if (res.data.code !== 0) {
      this.logger.error('Lark auth failed', res.data);
      throw new Error('Cannot get Lark access token');
    }

    this.accessToken = res.data.tenant_access_token;
    this.expireAt = now + res.data.expire * 1000;
    return this.accessToken!;
  }

  private async request(
    method: 'GET' | 'POST',
    path: string,
    opts: { params?: Record<string, any>; data?: any } = {},
  ): Promise<any> {
    const token = await this.getAccessToken();
    const res = await axios({
      method,
      url: `${this.host}${path}`,
      params: opts.params,
      paramsSerializer: { indexes: null },
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json; charset=utf-8',
      },
      validateStatus: () => true,
    });
    return res.data;
  }

  private pickAvatar(avatar: any): string | null {
    if (!avatar || typeof avatar !== 'object') return null;
    return (
      avatar.avatar_240 ||
      avatar.avatar_640 ||
      avatar.avatar_72 ||
      avatar.avatar_origin ||
      null
    );
  }

  private mapUser(u: any): LarkContactUser {
    return {
      name: u.name || u.en_name || '',
      employeeNo: u.employee_no || null,
      avatar: this.pickAvatar(u.avatar),
      openId: u.open_id || null,
      userId: u.user_id || null,
      unionId: u.union_id || null,
      mobile: u.mobile || null,
      jobTitle: u.job_title || null,
      departmentIds: Array.isArray(u.department_ids) ? u.department_ids : [],
      raw: u,
    };
  }

  private async collectScopeUserIds(): Promise<string[]> {
    const ids: string[] = [];
    let pageToken: string | undefined;
    const seen = new Set<string>();

    do {
      const data = await this.request('GET', '/open-apis/contact/v3/scopes', {
        params: {
          user_id_type: 'open_id',
          page_size: 100,
          page_token: pageToken,
        },
      });
      if (data.code !== 0) {
        this.logger.error(`Lark scopes lỗi: ${data.code} ${data.msg}`);
        throw new Error(`LARK_SCOPES_FAILED:${data.code}:${data.msg}`);
      }
      ids.push(...(data.data?.user_ids || []));
      pageToken = data.data?.has_more ? data.data?.page_token : undefined;
      if (pageToken && seen.has(pageToken)) break;
      if (pageToken) seen.add(pageToken);
    } while (pageToken);

    return ids;
  }

  /** Biến thể domain công ty của 1 email (cùng local-part). Parity với mb-ads. */
  private aliasEmails(email: string): string[] {
    const e = (email || '').toLowerCase();
    const at = e.indexOf('@');
    if (at <= 0) return [];
    const lp = e.slice(0, at);
    const domain = e.slice(at + 1);
    if (!this.companyDomains.includes(domain)) return [];
    return this.companyDomains
      .filter((d) => d !== domain)
      .map((d) => `${lp}@${d}`);
  }

  private async buildDirectory(): Promise<Map<string, LarkContactUser>> {
    const ids = await this.collectScopeUserIds();
    const map = new Map<string, LarkContactUser>();
    const entries: { user: LarkContactUser; emails: string[] }[] = [];

    for (let i = 0; i < ids.length; i += 50) {
      const chunk = ids.slice(i, i + 50);
      const data = await this.request(
        'GET',
        '/open-apis/contact/v3/users/batch',
        { params: { user_ids: chunk, user_id_type: 'open_id' } },
      );
      if (data.code !== 0) {
        this.logger.error(`Lark users/batch lỗi: ${data.code} ${data.msg}`);
        throw new Error(`LARK_USERS_BATCH_FAILED:${data.code}:${data.msg}`);
      }
      for (const u of data.data?.items || []) {
        const emails: string[] = [];
        if (u.enterprise_email)
          emails.push(String(u.enterprise_email).toLowerCase());
        if (u.email) emails.push(String(u.email).toLowerCase());
        entries.push({ user: this.mapUser(u), emails });
      }
    }

    // Pass 1: khoá EXACT (luôn thắng). Pass 2: khoá ALIAS domain công ty (lấp trống).
    for (const { user, emails } of entries) {
      for (const e of emails) map.set(e, user);
    }
    for (const { user, emails } of entries) {
      for (const e of emails) {
        for (const a of this.aliasEmails(e)) {
          if (!map.has(a)) map.set(a, user);
        }
      }
    }

    this.logger.log(`Danh bạ Lark: ${entries.length} user, ${map.size} khoá email`);
    return map;
  }

  async getDirectoryMap(force = false): Promise<Map<string, LarkContactUser>> {
    const now = Date.now();
    if (!force && this.directory && now - this.directoryAt < this.DIR_TTL_MS) {
      return this.directory;
    }
    const map = await this.buildDirectory();
    this.directory = map;
    this.directoryAt = now;
    return map;
  }

  async lookupUserByEmail(
    email: string,
    force = false,
  ): Promise<LarkContactUser | null> {
    const key = (email || '').trim().toLowerCase();
    if (!key) return null;
    const dir = await this.getDirectoryMap(force);
    return dir.get(key) || null;
  }
}
