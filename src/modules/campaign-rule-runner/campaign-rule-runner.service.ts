import { Injectable, Logger } from '@nestjs/common';
import { AdSet, Campaign, FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import {
  classifyMetaError,
  executeMetaApiWithRetry,
  isRetryableError,
  parseMetaError,
} from '../../common/utils';
import { AppConfigReader } from '../app-config/app-config.reader';
import { DistributedLockService } from '../distributed-lock/distributed-lock.service';
import { PrismaService } from '../prisma/prisma.service';
import {
  DEFAULT_TIMEZONE,
  INSIGHT_FIELDS,
  INSIGHT_NETWORK_SLEEP_MS,
  INSIGHT_RATELIMIT_SLEEP_MS,
  MAX_GROUP_DEPTH,
  META_CALL_TIMEOUT_MS,
  RULE_LOCK_TTL_SECONDS,
  RULE_RUN_MAX_WALL_MS,
} from './campaign-rule-runner.constants';
import {
  EvalContext,
  explainGroup,
  summarizeEvaluation,
} from './campaign-rule-evaluator';
import type { CustomMetricEvalDef } from './campaign-rule-custom-metric';
import {
  buildRollingSpec,
  buildSpecs,
  executeBudgetSchedule,
  fetchBudgetSchedules,
  fetchLiveBudget,
  type RollingConfig,
} from './campaign-rule-executor';
import { resolveMetric } from './campaign-rule-metric.resolver';
import {
  alignedNow,
  dedupeKey,
  isRuleDue,
  retryDedupeKey,
  SLOT_RETRY_MAX_ATTEMPTS,
  SLOT_RETRY_WINDOW_MS,
} from './campaign-rule-schedule.util';
import {
  normalizeTimeframe,
  timeframeToMetaParams,
  type MetaInsightTimeParams,
} from './campaign-rule-timeframe';

/** Include đệ quy cây group điều kiện tới độ sâu cố định (Prisma cần depth hữu hạn). */
function groupInclude(depth: number): any {
  if (depth <= 0) return { conditions: true };
  return {
    conditions: true,
    childGroups: { include: groupInclude(depth - 1) },
  };
}

/**
 * Lý do KHÔNG nối được khung cuốn chiếu → câu tiếng Việt cho nhân viên marketing đọc
 * trong nhật ký (item vẫn là SKIPPED, không phải lỗi). Lý do lạ giữ nguyên mã để debug.
 */
function describeRollingSkip(skipReason?: string, availableSec?: number): string {
  if (skipReason === 'below_min_duration_3h') {
    const minutes = Math.max(0, Math.round((availableSec ?? 0) / 60));
    return `khoảng trống còn lại chỉ ${minutes} phút (Meta yêu cầu khung tăng ngân sách tối thiểu 3 giờ) → chưa nối khung, sẽ thử lại lượt sau.`;
  }
  return `chưa tạo khung (${skipReason ?? 'không rõ'}).`;
}

/**
 * Khóa trong `CampaignRuleRun.ruleSnapshot` (Json sẵn có — KHÔNG thêm cột) đánh dấu
 * lượt chạy CHẾT VÌ LỖI TẠM THỜI Ở KHÂU ĐỌC INSIGHT, tức chưa hề gọi Meta ghi gì →
 * slot này còn được phép thử lại. Chỉ runner ghi khóa này (mb-ads khi "áp lịch tay"
 * cũng tạo run nhưng KHÔNG bao giờ có khóa này → không bị hiểu nhầm là cần thử lại).
 */
const RETRY_META_KEY = 'insightRetry';

/** Chi tiết kỹ thuật của một lỗi gọi Meta — LƯU để debug, KHÔNG hiện lên message chính. */
interface MetaFailureDetail {
  /** Message thô của Meta/axios (thường tiếng Anh). */
  message: string;
  kind: 'TRANSIENT' | 'PERMANENT';
  code?: number | string;
  subcode?: number;
  /** Mã lỗi mạng gốc: ECONNRESET/ETIMEDOUT/EAI_AGAIN... (xem ghi chú ở describeMetaFailure). */
  causeCode?: string;
  status?: number;
  /** URL đã CẮT query-string (tránh lọt access_token vào DB). */
  url?: string;
  fbtraceId?: string;
}

/** Mã lỗi mạng gốc (nếu còn móc lại được) — xem ghi chú ở describeMetaFailure. */
function rawErrorCode(error: any): string | undefined {
  const code =
    error?.cause?.code ??
    error?.code ??
    error?.cause?.errno ??
    error?.errno ??
    error?.cause?.syscall;
  if (code == null || code === '') return undefined;
  return String(code);
}

/** Bỏ query-string khỏi url Meta (chứa access_token) trước khi lưu vào DB. */
function sanitizeUrl(url: any): string | undefined {
  if (typeof url !== 'string' || !url) return undefined;
  const i = url.indexOf('?');
  return i >= 0 ? url.slice(0, i) : url;
}

/**
 * Phân loại + bóc nguyên nhân gốc của một lỗi gọi Meta.
 *
 * VÌ SAO PHẢI TỰ MÓC `cause`/`code`: SDK Meta (facebook-nodejs-business-sdk/src/
 * exceptions.js — `constructErrorResponse`) khi axios KHÔNG nhận được response chỉ
 * giữ đúng câu "The request was made but no response was received" rồi VỨT SẠCH
 * `err.code` của axios (ECONNRESET/ETIMEDOUT/EAI_AGAIN) → nhìn log xong không biết
 * mạng hỏng ở đâu. Ta cố móc lại từ `error.cause`/`error` (còn khi lỗi ném ra TRƯỚC
 * lúc SDK bọc lại), và luôn giữ `url`/`status`/`fbtrace_id` mà SDK có gắn.
 */
function describeMetaFailure(error: any): {
  transient: boolean;
  detail: MetaFailureDetail;
  /** Nội dung Meta/ta diễn giải được (ưu tiên error_user_msg → friendly tiếng Việt). */
  metaMessage: string;
} {
  const parsed = parseMetaError(error);
  const transient = isRetryableError(error);
  const cls = classifyMetaError(parsed);
  const detail: MetaFailureDetail = {
    message: parsed.message,
    kind: transient ? 'TRANSIENT' : 'PERMANENT',
  };
  if (parsed.code != null) detail.code = parsed.code;
  if (parsed.subcode != null) detail.subcode = parsed.subcode;
  const causeCode = rawErrorCode(error);
  if (causeCode) detail.causeCode = causeCode;
  const status = Number((error as any)?.status);
  if (Number.isFinite(status)) detail.status = status;
  const url = sanitizeUrl((error as any)?.url);
  if (url) detail.url = url;
  if (parsed.fbtrace_id) detail.fbtraceId = parsed.fbtrace_id;
  return { transient, detail, metaMessage: cls.userMessage || parsed.message };
}

/**
 * Lỗi khi ĐẨY khung ngân sách lên Meta → message tiếng Việt + phân loại tạm thời/vĩnh viễn.
 *
 * 🔴 Dù phân loại là TRANSIENT, runner TUYỆT ĐỐI KHÔNG tự chạy lại lượt này: lỗi xảy ra
 * SAU khi đã gọi Meta nên không biết Meta đã nhận khung hay chưa — tự thử lại có thể bơm
 * ngân sách HAI LẦN. Phân loại chỉ để hiển thị/điều tra; việc chạy lại do người dùng quyết.
 */
function describeExecFailure(err: any): {
  message: string;
  kind: 'TRANSIENT' | 'PERMANENT';
} {
  const transient = isRetryableError(err);
  if (transient) {
    return {
      kind: 'TRANSIENT',
      message:
        'Meta không phản hồi khi đẩy khung tăng ngân sách (lỗi mạng tạm thời). Hệ thống KHÔNG tự thử lại vì chưa rõ Meta đã nhận khung hay chưa — hãy kiểm tra lịch tăng ngân sách trên Meta, cần thì chạy lại thủ công.',
    };
  }
  const cls = classifyMetaError(err ?? {});
  return {
    kind: 'PERMANENT',
    message: cls.userMessage || err?.message || 'Đẩy budget schedule thất bại.',
  };
}

/** Gộp chi tiết kỹ thuật thành 1 dòng cho log (message + mã lỗi mạng + trace). */
function failureLogText(detail: MetaFailureDetail): string {
  const bits = [detail.message];
  if (detail.causeCode) bits.push(`code=${detail.causeCode}`);
  if (detail.code != null) bits.push(`metaCode=${detail.code}`);
  if (detail.status != null) bits.push(`status=${detail.status}`);
  if (detail.url) bits.push(`url=${detail.url}`);
  if (detail.fbtraceId) bits.push(`trace=${detail.fbtraceId}`);
  return bits.join(' · ');
}

/** Entity tối giản mà runner cần để đánh giá + thực thi. */
interface RunnerEntity {
  id: string;
  name?: string | null;
  dailyBudget?: number | null;
  lifetimeBudget?: number | null;
  // Cho hours_since_creation (rawPayload.created_time, fallback startTime) và results
  // (rawPayload.optimization_goal / promoted_object.custom_event_type ở level ad set).
  startTime?: Date | null;
  rawPayload?: any;
}

/**
 * Runner "campaign rule": cron quét rule ACTIVE, chạy nhánh "Theo điều kiện".
 *
 * Ràng buộc:
 * - Chỉ đọc/ghi bảng campaign_rule* + đọc Campaign/AdSet/Account. KHÔNG đụng Automation*.
 * - Chỉ action BUDGET_SCHEDULE_BUMP; chỉ level CAMPAIGN + ADSET. Các trường hợp khác → SKIPPED/log.
 * - Idempotent qua dedupeKey (unique trên CampaignRuleRun). Lượt THỬ LẠI slot dùng khóa
 *   `<dedupeKey>#retryN` (N suy ra tất định từ DB) nên vẫn chống chồng cross-replica.
 * - Metric LIVE fetch trực tiếp từ Meta THEO timeframe của điều kiện (mặc định today;
 *   xem campaign-rule-timeframe.ts), KHÔNG đọc insight cache DB.
 */
@Injectable()
export class CampaignRuleRunnerService {
  private readonly logger = new Logger(CampaignRuleRunnerService.name);
  private metaInitialized = false;
  /**
   * Mốc thời gian (epoch ms) mà lượt chạy rule hiện tại phải kết thúc trước, để không
   * vượt TTL của khóa `crr:<ruleId>`. An toàn khi để ở cấp instance vì `runDueRules`
   * xử lý các rule TUẦN TỰ (vòng `for` có await) — không có 2 lượt chạy chồng nhau
   * trong cùng một process.
   */
  private runDeadlineAt = Number.POSITIVE_INFINITY;

  constructor(
    private readonly prisma: PrismaService,
    private readonly lock: DistributedLockService,
    private readonly appConfig: AppConfigReader,
  ) {}

  /**
   * SÀN HIỆU QUẢ LIVE: chặn bơm ngân sách khi ROAS HÔM NAY (insight date_preset=today)
   * của đối tượng thấp hơn sàn cấu hình — đỡ đổ thêm tiền vào camp đang lỗ (phân tích
   * cho thấy ~40% lượt bơm rơi vào camp ROAS sập). Chỉ chặn khi:
   *   - sàn > 0 (bật), VÀ
   *   - đã tiêu ≥ ngưỡng tối thiểu hôm nay (ROAS đầu ngày ít dữ liệu → không chặn nhầm), VÀ
   *   - đo ĐƯỢC ROAS (null = camp tin nhắn/không mua → KHÔNG chặn).
   * Trả { block, reason?, roas, spend, floor }.
   */
  private async evalBumpGuard(insight: any): Promise<{
    block: boolean;
    reason?: string;
    roas: number | null;
    spend: number | null;
    floor: number;
    minSpend: number;
  }> {
    const floor = await this.appConfig.getNumber(
      'campaign_rule_bump_roas_floor',
      1,
      'CAMPAIGN_RULE_BUMP_ROAS_FLOOR',
    );
    const minSpend = await this.appConfig.getNumber(
      'campaign_rule_bump_guard_min_spend',
      50000,
      'CAMPAIGN_RULE_BUMP_GUARD_MIN_SPEND',
    );
    const spend = resolveMetric('spend', insight, null);
    const roas = resolveMetric('purchase_roas', insight, null);
    if (!(floor > 0)) return { block: false, roas, spend, floor, minSpend };
    if (spend == null || spend < minSpend)
      return { block: false, roas, spend, floor, minSpend };
    if (roas == null) return { block: false, roas, spend, floor, minSpend };
    if (roas < floor) {
      return {
        block: true,
        reason: `Hoãn bơm: ROAS hôm nay ${roas.toFixed(2)} < sàn ${floor} (đã chi ${Math.round(
          spend,
        )} ≥ ${minSpend}). Giữ nguyên khung đang chạy, không đổ thêm ngân sách vào camp hiệu quả thấp.`,
        roas,
        spend,
        floor,
        minSpend,
      };
    }
    return { block: false, roas, spend, floor, minSpend };
  }

  private initMetaApi() {
    if (this.metaInitialized) return;
    const token = process.env.SDK_FACEBOOK_ACCESS_TOKEN;
    if (!token) {
      throw new Error('SDK_FACEBOOK_ACCESS_TOKEN is missing in environment');
    }
    FacebookAdsApi.init(token);
    this.metaInitialized = true;
  }

  /** Điểm vào từ scheduler: quét mọi rule ACTIVE, xử lý rule nào đến hạn. */
  async runDueRules(): Promise<void> {
    this.initMetaApi();
    const now = new Date();

    const rules = await this.prisma.campaignRule.findMany({
      where: {
        status: 'ACTIVE',
        deletedAt: null,
        schedule: { isNot: null },
      },
      include: {
        schedule: true,
        tasks: {
          orderBy: { position: 'asc' },
          include: { rootGroup: { include: groupInclude(MAX_GROUP_DEPTH) } },
        },
      },
    });

    if (rules.length === 0) return;
    this.logger.log(`🔎 Quét ${rules.length} campaign rule ACTIVE...`);

    for (const rule of rules) {
      try {
        await this.processRule(rule, now);
      } catch (error) {
        this.logger.error(
          `Rule ${rule.id} (${rule.name}) lỗi: ${error?.message || error}`,
        );
      }
    }
  }

  /** Kiểm tra dueness rồi chạy dưới khóa phân tán chống chồng cross-replica. */
  private async processRule(rule: any, now: Date): Promise<void> {
    const schedule = rule.schedule;
    if (!schedule) return;

    const timezone = await this.resolveTimezone(rule);
    const lastRunAt = await this.getLastRunAt(rule.id);
    const dueness = isRuleDue(schedule, lastRunAt, now, timezone);

    let aligned: Date;
    let key: string;
    let attempt = 0; // 0 = lượt đầu của slot; >0 = lần thử lại thứ N

    if (dueness.due) {
      aligned = dueness.aligned || alignedNow(now);
      key = dedupeKey(rule.id, rule.accountId, aligned);
    } else {
      if (dueness.outOfDateRange) {
        this.logger.debug(
          `Rule ${rule.id} ngoài khoảng ngày hiệu lực → bỏ qua.`,
        );
        return; // ngoài khoảng hiệu lực thì cũng KHÔNG thử lại slot cũ
      }
      // Không đến hạn: còn 1 cửa nữa — slot trước vừa chết vì lỗi TẠM THỜI ở khâu đọc
      // insight (chưa gọi Meta ghi gì) thì thử lại chính slot đó.
      const retry = await this.findRetryableRun(rule, now);
      if (!retry) return;
      aligned = retry.aligned;
      attempt = retry.attempt;
      key = retryDedupeKey(
        dedupeKey(rule.id, rule.accountId, aligned),
        attempt,
      );
      this.logger.log(
        `↻ Rule ${rule.id} thử lại slot ${aligned.toISOString()} (lần ${attempt}/${SLOT_RETRY_MAX_ATTEMPTS}) sau lỗi tạm thời khi đọc insight.`,
      );
    }

    await this.lock.runExclusive(
      `crr:${rule.id}`,
      RULE_LOCK_TTL_SECONDS,
      async () => {
        await this.executeRun(rule, timezone, aligned, key, now, attempt);
      },
    );
  }

  /**
   * Slot cần THỬ LẠI (nếu có). AN TOÀN TIỀN BẠC — chỉ trả về khi lượt gần nhất:
   *  - do CHÍNH runner tạo theo lịch (`dedupeKey != null`; run "áp lịch tay" của mb-ads
   *    có dedupeKey null → loại), VÀ
   *  - status FAILED kèm dấu `ruleSnapshot.insightRetry.retryable` — dấu này CHỈ được
   *    đặt khi TOÀN BỘ entity chết ngay ở khâu ĐỌC insight, tức lượt đó chưa gọi Meta
   *    tạo/sửa khung ngân sách lần nào ⇒ chạy lại không thể bơm ngân sách hai lần, VÀ
   *  - chưa vượt trần số lần thử lại, VÀ còn trong cửa sổ SLOT_RETRY_WINDOW_MS.
   *
   * Lỗi XẢY RA SAU KHI ĐÃ GỌI META (executionError của item) KHÔNG bao giờ đặt dấu này
   * → không tự thử lại, vì không biết Meta đã nhận khung hay chưa.
   */
  private async findRetryableRun(
    rule: any,
    now: Date,
  ): Promise<{ aligned: Date; attempt: number } | null> {
    const last = await this.prisma.campaignRuleRun.findFirst({
      where: { ruleId: rule.id, dedupeKey: { not: null } },
      orderBy: [{ scheduledFor: 'desc' }, { createdAt: 'desc' }],
      select: { status: true, scheduledFor: true, ruleSnapshot: true },
    });
    if (!last || last.status !== 'FAILED') return null;

    const meta = (last.ruleSnapshot as any)?.[RETRY_META_KEY];
    if (!meta?.retryable) return null;

    // `attempt` của run gần nhất = số lần ĐÃ thử lại slot này (0 = lượt đầu).
    const attempted = Number(meta.attempt) || 0;
    if (attempted >= SLOT_RETRY_MAX_ATTEMPTS) return null;

    const scheduledFor = new Date(last.scheduledFor);
    if (now.getTime() - scheduledFor.getTime() > SLOT_RETRY_WINDOW_MS) return null;

    return { aligned: scheduledFor, attempt: attempted + 1 };
  }

  /**
   * Nạp TRƯỚC (1 lần/run) định nghĩa custom metric mà rule tham chiếu, ĐÃ LỌC
   * context='BUDGET_SCHEDULE' (không lấy metric của ngữ cảnh khác → không sai số).
   * Trả undefined nếu rule không dùng custom metric (đường rule thường không đổi).
   * Metric bị xoá / sai context → không có trong map → resolver trả null+warn (điều
   * kiện KHÔNG khớp, không âm thầm pass).
   */
  private async loadCustomMetricRegistry(
    rule: any,
  ): Promise<Map<string, CustomMetricEvalDef> | undefined> {
    const refs = new Set<string>();
    const walk = (group: any) => {
      if (!group) return;
      for (const c of group.conditions || []) {
        const p = c?.params || {};
        for (const k of [p.metric, p.leftMetric, p.rightMetric]) {
          if (
            typeof k === 'string' &&
            k.toLowerCase().startsWith('custom_metric:')
          ) {
            refs.add(k.toLowerCase());
          }
        }
      }
      for (const g of group.childGroups || []) walk(g);
    };
    for (const task of rule.tasks || []) walk(task.rootGroup);
    if (refs.size === 0) return undefined;

    const ids = Array.from(refs).map((r) =>
      r.slice('custom_metric:'.length),
    );
    const rows = await this.prisma.customMetric.findMany({
      where: { id: { in: ids }, context: 'BUDGET_SCHEDULE' },
      select: { id: true, formula: true, format: true },
    });
    const map = new Map<string, CustomMetricEvalDef>();
    for (const row of rows) {
      map.set(`custom_metric:${row.id}`.toLowerCase(), {
        id: row.id,
        formula: (row.formula as any) || [],
        format: (row.format as any) || 'numeric',
      });
    }
    return map;
  }

  /**
   * Tạo run (idempotent) rồi đánh giá + thực thi từng entity.
   * `attempt` = 0 cho lượt đầu của slot, >0 khi đang THỬ LẠI slot đó (xem findRetryableRun).
   */
  private async executeRun(
    rule: any,
    timezone: string,
    aligned: Date,
    key: string,
    now: Date,
    attempt = 0,
  ): Promise<void> {
    const ruleSnapshot: Record<string, any> = {
      name: rule.name,
      level: rule.level,
      autoExecute: rule.autoExecute,
      timezone,
      ...(attempt > 0 ? { retryAttempt: attempt } : {}),
    };
    let run;
    try {
      run = await this.prisma.campaignRuleRun.create({
        data: {
          ruleId: rule.id,
          accountId: rule.accountId,
          scheduledFor: aligned,
          startedAt: new Date(),
          dedupeKey: key,
          status: 'RUNNING',
          ruleSnapshot,
        },
      });
    } catch (error) {
      // Unique dedupeKey trùng → một tick/replica khác đã tạo run cho slot này.
      if (error?.code === 'P2002') {
        this.logger.log(
          `Rule ${rule.id} slot ${key} đã có run → SKIPPED_OVERLAP.`,
        );
        return;
      }
      throw error;
    }

    let entitiesScanned = 0;
    let matchedCount = 0;
    let errorsCount = 0;
    let fatalError: string | null = null;
    // Đếm entity chết NGAY ở khâu đọc insight (trước MỌI lời gọi ghi lên Meta) — cơ sở
    // để quyết định có được thử lại slot hay không.
    let insightFailures = 0;
    let insightPermanentFailures = 0;
    // Số entity bị bỏ dở vì lượt chạy chạm hạn chót (xem runDeadlineAt bên dưới).
    let deadlineSkipped = 0;

    // Hạn chót của lượt chạy: khóa `crr:<ruleId>` KHÔNG tự gia hạn, nên mọi thứ có thể
    // ngủ/chờ phải nằm gọn trong RULE_RUN_MAX_WALL_MS. Quá hạn mà vẫn chạy tiếp = chạy
    // KHÔNG có khóa → replica khác có thể vào cùng rule → bơm ngân sách hai lần.
    this.runDeadlineAt = Date.now() + RULE_RUN_MAX_WALL_MS;

    try {
      // Chỉ hỗ trợ level CAMPAIGN + ADSET, và cần campaignId để scope.
      if (rule.level !== 'CAMPAIGN' && rule.level !== 'ADSET') {
        this.logger.warn(
          `Rule ${rule.id} level ${rule.level} chưa hỗ trợ (chỉ CAMPAIGN/ADSET) → bỏ qua.`,
        );
      } else if (!rule.campaignId) {
        this.logger.warn(
          `Rule ${rule.id} level ${rule.level} thiếu campaignId (phủ cả account) chưa hỗ trợ → bỏ qua.`,
        );
      } else {
        // Nạp custom metric 1 lần/run (đã lọc context) rồi luồn xuống mọi entity/task.
        const customMetrics = await this.loadCustomMetricRegistry(rule);
        const entities = await this.loadEntities(rule);
        for (const entity of entities) {
          // Chạm hạn chót → DỪNG SẠCH, không bắt đầu entity mới. Thà bỏ sót vài entity
          // (lượt quét sau xử lý) còn hơn chạy tiếp khi khóa đã có thể hết hạn.
          if (Date.now() >= this.runDeadlineAt) {
            deadlineSkipped = entities.length - entitiesScanned;
            this.logger.warn(
              `Rule ${rule.id} run ${run.id} chạm hạn chót ${RULE_RUN_MAX_WALL_MS}ms → ` +
                `bỏ dở ${deadlineSkipped} entity, sẽ quét ở lượt sau.`,
            );
            break;
          }
          entitiesScanned += 1;
          const res = await this.processEntity(
            rule,
            run.id,
            entity,
            timezone,
            now,
            customMetrics,
          );
          matchedCount += res.matched;
          errorsCount += res.errors;
          if (res.insightFetchError) {
            insightFailures += 1;
            if (res.insightFetchError !== 'TRANSIENT') {
              insightPermanentFailures += 1;
            }
          }
        }
      }
    } catch (error) {
      fatalError = parseMetaError(error).message || String(error);
      this.logger.error(`Rule ${rule.id} run ${run.id} lỗi: ${fatalError}`);
    }

    // Cả lượt chỉ toàn entity chết ở khâu đọc insight → lượt này KHÔNG làm được gì:
    // đánh FAILED (trước đây vẫn COMPLETED nên nhìn nhật ký tưởng đã chạy xong).
    const allInsightFailed = entitiesScanned > 0 && insightFailures === entitiesScanned;

    // ĐƯỢC PHÉP THỬ LẠI khi và chỉ khi:
    //  - không có lỗi fatal (lỗi fatal có thể xảy ra SAU khi vài entity đã đẩy Meta), VÀ
    //  - MỌI entity đều chết ở khâu ĐỌC insight — khâu này nằm TRƯỚC mọi lời gọi ghi
    //    (executeBudgetSchedule) ở cả nhánh FIXED lẫn ROLLING ⇒ lượt này chắc chắn CHƯA
    //    tạo khung nào trên Meta ⇒ chạy lại không thể bơm ngân sách hai lần, VÀ
    //  - mọi lỗi đều TẠM THỜI (mạng/timeout/rate-limit); lỗi vĩnh viễn (sai cấu hình,
    //    Meta từ chối) thì thử lại chỉ tốn quota, VÀ
    //  - chưa chạm trần số lần thử lại (matchedCount===0 là bất biến, kiểm tra cho chắc).
    const retryable =
      !fatalError &&
      allInsightFailed &&
      insightPermanentFailures === 0 &&
      matchedCount === 0 &&
      attempt < SLOT_RETRY_MAX_ATTEMPTS;

    const insightFailMessage = retryable
      ? 'Meta không phản hồi khi lấy số liệu (lỗi mạng tạm thời) — sẽ tự thử lại slot này ở lượt quét sau.'
      : allInsightFailed
        ? 'Không lấy được số liệu từ Meta cho toàn bộ đối tượng của lượt chạy này (xem chi tiết ở từng dòng).'
        : null;

    await this.prisma.campaignRuleRun.update({
      where: { id: run.id },
      data: {
        status: fatalError || allInsightFailed ? 'FAILED' : 'COMPLETED',
        finishedAt: new Date(),
        entitiesScanned,
        matchedCount,
        errorsCount,
        errorMessage: fatalError ?? insightFailMessage,
        // Dấu cho tick sau biết slot này còn được thử lại (Json sẵn có, không thêm cột).
        ...(retryable
          ? {
              ruleSnapshot: {
                ...ruleSnapshot,
                [RETRY_META_KEY]: {
                  retryable: true,
                  attempt,
                  stage: 'FETCH_INSIGHT',
                  entities: insightFailures,
                },
              },
            }
          : {}),
      },
    });

    this.logger.log(
      `✅ Rule ${rule.id} run ${run.id}: quét ${entitiesScanned}, khớp ${matchedCount}, lỗi ${errorsCount}.` +
        (retryable ? ' ↻ sẽ thử lại slot ở lượt quét sau.' : ''),
    );
  }

  /** Nạp entity cần đánh giá theo level. */
  private async loadEntities(rule: any): Promise<RunnerEntity[]> {
    if (rule.level === 'CAMPAIGN') {
      const campaign = await this.prisma.campaign.findUnique({
        where: { id: rule.campaignId },
        select: {
          id: true,
          name: true,
          dailyBudget: true,
          lifetimeBudget: true,
          startTime: true,
          rawPayload: true,
        },
      });
      return campaign ? [campaign] : [];
    }
    // ADSET: nếu rule pin adSetId (ABO — mỗi nhóm 1 lịch riêng) thì CHỈ nhóm đó;
    // không thì mọi ad set thuộc campaign (bỏ ad set đã xoá mềm).
    return this.prisma.adSet.findMany({
      where: {
        campaignId: rule.campaignId,
        deletedAt: null,
        ...(rule.adSetId ? { id: rule.adSetId } : {}),
      },
      select: {
        id: true,
        name: true,
        dailyBudget: true,
        lifetimeBudget: true,
        startTime: true,
        rawPayload: true,
      },
    });
  }

  /**
   * Fetch insight live cho 1 entity, đánh giá MỌI task, ghi item tương ứng.
   * Lỗi fetch insight → tất cả task của entity đó thành FAILED (không làm hỏng entity khác)
   * và trả `insightFetchError` (TRANSIENT/PERMANENT) để executeRun quyết định có thử lại slot.
   */
  private async processEntity(
    rule: any,
    runId: string,
    entity: RunnerEntity,
    timezone: string,
    now: Date,
    customMetrics?: Map<string, CustomMetricEvalDef>,
  ): Promise<{
    matched: number;
    errors: number;
    insightFetchError?: 'TRANSIENT' | 'PERMANENT';
  }> {
    const level: 'CAMPAIGN' | 'ADSET' = rule.level;

    // ---- Cuốn chiếu: bỏ qua đọc insight khi đang GIỮA khung ----
    // Rule chỉ toàn task ROLLING: nếu khung "của mình" còn phủ xa hơn lead thì
    // rule đang "nghỉ" — chỉ cần đánh giá điều kiện ở đuôi khung (T'−lead). Kiểm
    // tra coveredUntil TRƯỚC (chỉ tốn 1 call budget schedules) để KHÔNG gọi
    // getInsights suốt thời gian khung chạy (đỡ token). Tail-tick sẽ rơi xuống
    // luồng dưới, fetch insight bình thường.
    const allRolling =
      rule.tasks.length > 0 &&
      rule.tasks.every(
        (t: any) =>
          t.kind === 'BUDGET_SCHEDULE_BUMP' && t.params?.mode === 'ROLLING',
      );
    if (allRolling) {
      const nowUnix = Math.floor(now.getTime() / 1000);
      const ownedIds = await this.gatherOwnedScheduleIds(rule.id, entity.id);
      const live = await fetchBudgetSchedules(level, entity.id);
      const coveredUntil = live.reduce(
        (mx, w) =>
          ownedIds.has(w.id) && w.time_end > nowUnix
            ? Math.max(mx, w.time_end)
            : mx,
        nowUnix,
      );
      const maxLead = rule.tasks.reduce(
        (mx: number, t: any) =>
          Math.max(
            mx,
            Math.max(0, Math.round(t.params?.rolling?.leadMinutes ?? 15)) * 60,
          ),
        0,
      );
      if (coveredUntil - nowUnix > maxLead) {
        for (const task of rule.tasks) {
          await this.createItem({
            runId,
            task,
            level,
            entity,
            status: 'SKIPPED',
            snapshot: { budgets: this.budgetSnapshot(entity) },
            changePreview: { rolling: { mode: 'ROLLING', coveredUntil } },
            evaluation: { matched: true },
            matchedConditionSummary:
              'Đang còn khung phủ → chưa cần nối (bỏ qua đọc insight, đỡ token).',
          });
        }
        return { matched: rule.tasks.length, errors: 0 };
      }
    }

    // Khung HÔM NAY là "chủ đạo": guard (sàn ROAS), snapshot và path ROLLING đều đọc
    // trên khung này. Fetch today TRƯỚC — lỗi today → GIỮ hành vi cũ: mọi task FAILED.
    let insight: any;
    try {
      insight = await this.fetchLiveInsight(level, entity.id);
    } catch (error) {
      // Giữ NGUYÊN NHÂN GỐC (mã lỗi mạng/url/trace) ở phần lưu trữ để còn debug; phần
      // hiển thị (errorMessage) là câu tiếng Việt cho nhân viên marketing đọc.
      const { transient, detail, metaMessage } = describeMetaFailure(error);
      this.logger.warn(
        `Lấy insight ${level} ${entity.id} lỗi: ${failureLogText(detail)} → item FAILED` +
          (transient ? ' (tạm thời — slot sẽ được thử lại).' : ' (lỗi vĩnh viễn — KHÔNG thử lại).'),
      );
      const userMessage = transient
        ? 'Meta không phản hồi (lỗi mạng tạm thời) — sẽ thử lại lượt sau.'
        : `Không lấy được số liệu từ Meta: ${metaMessage}`;
      let errors = 0;
      for (const task of rule.tasks) {
        errors += 1;
        await this.createItem({
          runId,
          task,
          level,
          entity,
          status: 'FAILED',
          snapshot: { budgets: this.budgetSnapshot(entity) },
          changePreview: {},
          evaluation: {
            matched: false,
            // Giữ kiểu string như cũ (FE `RunEvaluation.insightError` đang đọc chuỗi).
            insightError: detail.message,
            insightErrorKind: detail.kind,
            insightErrorDetail: detail,
          },
          errorMessage: userMessage,
        });
      }
      return {
        matched: 0,
        errors,
        insightFetchError: transient ? 'TRANSIENT' : 'PERMANENT',
      };
    }

    // Fetch MỖI timeframe DISTINCT còn lại (ngoài today) 1 lần → map để điều kiện đọc
    // đúng khung của nó. QUYẾT ĐỊNH (mục 6 của spec): CÔ LẬP LỖI theo khung — khung nào
    // fetch lỗi thì CHỈ các điều kiện dùng khung đó coi như KHÔNG khớp (ghi lý do vào
    // explain), KHÔNG đánh FAILED toàn entity (chọn phương án cô lập vì rẻ & chính xác
    // hơn: today vẫn chạy bình thường). Riêng lỗi today ở trên vẫn FAILED toàn entity.
    const { byTimeframe, errorsByTimeframe } =
      await this.fetchInsightsByTimeframe(
        rule,
        level,
        entity.id,
        insight,
        now,
        timezone,
      );

    const ctx: EvalContext = {
      insight,
      insightByTimeframe: byTimeframe,
      insightErrors: errorsByTimeframe,
      entity,
      now,
      timezone,
      customMetrics,
    };
    let matched = 0;
    let errors = 0;

    for (const task of rule.tasks) {
      // Chỉ hỗ trợ BUDGET_SCHEDULE_BUMP.
      if (task.kind !== 'BUDGET_SCHEDULE_BUMP') {
        this.logger.log(
          `Task ${task.id} kind ${task.kind} chưa hỗ trợ → SKIPPED.`,
        );
        await this.createItem({
          runId,
          task,
          level,
          entity,
          status: 'SKIPPED',
          snapshot: this.buildSnapshot(insight, entity),
          changePreview: {},
          evaluation: null,
          errorMessage: `Task kind ${task.kind} chưa hỗ trợ (chỉ BUDGET_SCHEDULE_BUMP).`,
        });
        continue;
      }

      // Chế độ "cuốn chiếu": khung ĐỘNG nối đuôi theo thời điểm rule nổ (xử lý riêng).
      if (task.params?.mode === 'ROLLING') {
        const res = await this.processRollingTask({
          runId,
          task,
          rule,
          level,
          entity,
          insight,
          insightByTimeframe: byTimeframe,
          insightErrors: errorsByTimeframe,
          timezone,
          now,
          customMetrics,
        });
        matched += res.matched;
        errors += res.errors;
        continue;
      }

      // Đánh giá + GIẢI THÍCH điều kiện (ghi vào evaluation để nhật ký hiện vì sao).
      const evalTree = explainGroup(task.rootGroup, ctx);
      const isMatched = evalTree.matched;
      const evalSummary = summarizeEvaluation(evalTree);
      const evaluation = { matched: isMatched, summary: evalSummary, tree: evalTree };
      const snapshot = this.buildSnapshot(insight, entity);
      // % (MULTIPLIER) quy đổi theo ngân sách THẬT của chính đối tượng đang xét.
      const targetBudget = entity?.dailyBudget ?? entity?.lifetimeBudget ?? null;
      // Mốc giờ khung lịch diễn giải theo múi giờ TKQC (timezone đã resolve = tz
      // account khi rule.timezone="account").
      const specs = buildSpecs(task.params?.periods, targetBudget, timezone);
      const changePreview = { budget_schedule_specs: specs };

      if (!isMatched) {
        await this.createItem({
          runId,
          task,
          level,
          entity,
          status: 'NOT_MATCHED',
          snapshot,
          changePreview,
          evaluation,
          matchedConditionSummary: evalSummary,
        });
        continue;
      }

      matched += 1;

      if (rule.autoExecute) {
        // Sàn hiệu quả live: ROAS hôm nay dưới sàn → HOÃN bơm (không đổ thêm tiền vào
        // camp đang lỗ). Item SKIPPED, giữ nguyên khung đang chạy.
        const guard = await this.evalBumpGuard(insight);
        if (guard.block) {
          await this.createItem({
            runId,
            task,
            level,
            entity,
            status: 'SKIPPED',
            snapshot,
            changePreview: { ...changePreview, guard },
            evaluation: { ...evaluation, guardBlocked: true },
            matchedConditionSummary: guard.reason,
          });
          continue;
        }
        const result = await executeBudgetSchedule(level, entity.id, specs);
        if (result.ok) {
          await this.createItem({
            runId,
            task,
            level,
            entity,
            status: 'EXECUTED',
            snapshot,
            changePreview,
            evaluation,
            matchedConditionSummary: `${evalSummary} → đã đẩy budget schedule.`,
            executedAt: new Date(),
            executionAttempts: 1,
            metaTraceId: result.metaTraceId,
            metaBudgetScheduleIds: result.scheduleIds,
          });
        } else {
          errors += 1;
          // Đã CHẠM Meta rồi mới lỗi → chỉ phân loại + diễn giải, KHÔNG tự thử lại.
          const execFail = describeExecFailure(result.error);
          await this.createItem({
            runId,
            task,
            level,
            entity,
            status: 'FAILED',
            snapshot,
            changePreview,
            evaluation,
            matchedConditionSummary: `${evalSummary} nhưng đẩy Meta thất bại.`,
            errorMessage: execFail.message,
            executionAttempts: 1,
            executionError: {
              ...(result.error ?? {}),
              kind: execFail.kind,
              autoRetried: false,
            },
            metaTraceId: result.metaTraceId,
            metaBudgetScheduleIds: result.scheduleIds,
          });
        }
      } else {
        await this.createItem({
          runId,
          task,
          level,
          entity,
          status: 'PENDING',
          snapshot,
          changePreview,
          evaluation,
          matchedConditionSummary: `${evalSummary} → chờ xác nhận.`,
        });
      }
    }

    return { matched, errors };
  }

  /**
   * Xử lý 1 task BUDGET_SCHEDULE_BUMP ở chế độ CUỐN CHIẾU (mode=ROLLING).
   *
   * Mỗi tick tới hạn:
   *  - Đọc ngân sách LIVE từ Meta (để tính % + cho điều kiện tham chiếu budget).
   *  - Đánh giá điều kiện trên insight LIVE.
   *  - Đọc khung budget schedule THẬT trên Meta, tách "của mình" (owned) vs người khác.
   *  - ĐẠT + sắp hết phủ (coveredUntil − now ≤ lead) → tạo 1 khung KẾ nối đuôi; còn phủ
   *    xa thì NO-OP ("rule nghỉ"). KHÔNG đạt → dừng nối, GIỮ NGUYÊN khung đã đặt
   *    (không huỷ; Meta tự revert ngân sách khi khung hết hạn, care-ads lo tắt ads xấu).
   */
  private async processRollingTask(args: {
    runId: string;
    task: any;
    rule: any;
    level: 'CAMPAIGN' | 'ADSET';
    entity: RunnerEntity;
    insight: any;
    // Map insight theo timeframe (đọc điều kiện theo đúng khung); insight = today.
    insightByTimeframe?: Map<string, any>;
    insightErrors?: Map<string, string>;
    timezone: string;
    now: Date;
    customMetrics?: Map<string, CustomMetricEvalDef>;
  }): Promise<{ matched: number; errors: number }> {
    const {
      runId,
      task,
      rule,
      level,
      entity,
      insight,
      insightByTimeframe,
      insightErrors,
      timezone,
      now,
      customMetrics,
    } = args;
    const rolling = (task.params?.rolling ?? {}) as RollingConfig;
    const nowUnix = Math.floor(now.getTime() / 1000);

    // Ngân sách LIVE từ Meta (fallback DB nếu đọc lỗi) — dùng cho cả % tăng lẫn điều kiện.
    const liveBudget = await fetchLiveBudget(level, entity.id);
    const liveEntity: RunnerEntity = {
      ...entity,
      dailyBudget: liveBudget.dailyBudget ?? entity.dailyBudget ?? null,
      lifetimeBudget: liveBudget.lifetimeBudget ?? entity.lifetimeBudget ?? null,
    };
    const targetBudget = liveEntity.dailyBudget ?? liveEntity.lifetimeBudget ?? null;

    const ctx: EvalContext = {
      insight,
      insightByTimeframe,
      insightErrors,
      entity: liveEntity,
      now,
      timezone,
      customMetrics,
    };
    // Đánh giá + GIẢI THÍCH điều kiện (ghi evaluation để nhật ký hiện vì sao đạt/không).
    const evalTree = explainGroup(task.rootGroup, ctx);
    const isMatched = evalTree.matched;
    const evalSummary = summarizeEvaluation(evalTree);
    const evaluation = { matched: isMatched, summary: evalSummary, tree: evalTree };
    const snapshot = this.buildSnapshot(insight, liveEntity);

    // Khung "của mình" = HDP do các lần chạy trước của rule này tạo (theo entity).
    const ownedIds = await this.gatherOwnedScheduleIds(rule.id, entity.id);
    const live = await fetchBudgetSchedules(level, entity.id);
    const ownedWindows = live.filter((w) => ownedIds.has(w.id));
    const foreignWindows = live.filter((w) => !ownedIds.has(w.id));
    const coveredUntil = ownedWindows.reduce(
      (mx, w) => (w.time_end > nowUnix ? Math.max(mx, w.time_end) : mx),
      nowUnix,
    );

    // ---- KHÔNG đạt điều kiện: DỪNG nối, GIỮ NGUYÊN mọi khung đã đặt ----
    // KHÔNG xoá/huỷ khung nào (kể cả khung chưa bắt đầu): (1) ads xấu đã có rule
    // care-ads tự tắt, khung đã tăng cũng không tiêu; (2) huỷ/xoá lịch nhiều lần
    // phá hành vi máy học của camp; (3) Meta tự đưa ngân sách về gốc khi khung hết
    // hạn. Chỉ số giảm giữa chừng → không cần can thiệp.
    if (!isMatched) {
      await this.createItem({
        runId,
        task,
        level,
        entity,
        status: 'NOT_MATCHED',
        snapshot,
        changePreview: { rolling: { mode: 'ROLLING' } },
        evaluation,
        matchedConditionSummary: `${evalSummary} → dừng nối khung mới (giữ nguyên khung đang chạy).`,
      });
      return { matched: 0, errors: 0 };
    }

    // ---- Đạt điều kiện ----
    const lead = Math.max(0, Math.round(rolling.leadMinutes ?? 15)) * 60;
    // Còn phủ xa hơn lead → chưa cần nối (rule "nghỉ" trong khoảng T→T').
    if (coveredUntil - nowUnix > lead) {
      await this.createItem({
        runId,
        task,
        level,
        entity,
        status: 'SKIPPED',
        snapshot,
        changePreview: { rolling: { mode: 'ROLLING', coveredUntil } },
        evaluation,
        matchedConditionSummary: `${evalSummary} · đang còn khung phủ → chưa cần nối khung mới.`,
      });
      return { matched: 1, errors: 0 };
    }

    const { spec, skipReason, availableSec } = buildRollingSpec(rolling, {
      nowUnix,
      tz: timezone,
      targetBudget,
      coveredUntil,
      ownedWindows,
      foreignWindows,
    });
    if (!spec) {
      await this.createItem({
        runId,
        task,
        level,
        entity,
        status: 'SKIPPED',
        snapshot,
        changePreview: { rolling: { mode: 'ROLLING', skipReason, availableSec } },
        evaluation,
        matchedConditionSummary: `${evalSummary} nhưng ${describeRollingSkip(skipReason, availableSec)}`,
      });
      return { matched: 1, errors: 0 };
    }

    const changePreview = {
      budget_schedule_specs: [spec],
      rolling: { mode: 'ROLLING', windowMode: rolling.windowMode ?? 'DURATION' },
    };

    // ROLLING nên tự chạy (đêm không ai duyệt). Nếu rule không autoExecute → chờ duyệt.
    if (!rule.autoExecute) {
      await this.createItem({
        runId,
        task,
        level,
        entity,
        status: 'PENDING',
        snapshot,
        changePreview,
        evaluation,
        matchedConditionSummary: `${evalSummary} → chờ xác nhận (khung cuốn chiếu).`,
      });
      return { matched: 1, errors: 0 };
    }

    // Sàn hiệu quả live (giống path FIXED): ROAS hôm nay dưới sàn → HOÃN nối khung mới.
    // Không huỷ khung đang chạy (Meta tự revert khi hết hạn) — chỉ ngừng ĐỔ THÊM tiền.
    const guard = await this.evalBumpGuard(insight);
    if (guard.block) {
      await this.createItem({
        runId,
        task,
        level,
        entity,
        status: 'SKIPPED',
        snapshot,
        changePreview: { ...changePreview, guard },
        evaluation: { ...evaluation, guardBlocked: true },
        matchedConditionSummary: guard.reason,
      });
      return { matched: 1, errors: 0 };
    }

    const result = await executeBudgetSchedule(level, entity.id, [spec]);
    if (result.ok) {
      await this.createItem({
        runId,
        task,
        level,
        entity,
        status: 'EXECUTED',
        snapshot,
        changePreview,
        evaluation,
        matchedConditionSummary: `${evalSummary} → đã nối khung tăng ngân sách.`,
        executedAt: new Date(),
        executionAttempts: 1,
        metaTraceId: result.metaTraceId,
        metaBudgetScheduleIds: result.scheduleIds,
      });
      return { matched: 1, errors: 0 };
    }
    // Đã CHẠM Meta rồi mới lỗi → chỉ phân loại + diễn giải, KHÔNG tự thử lại.
    const execFail = describeExecFailure(result.error);
    await this.createItem({
      runId,
      task,
      level,
      entity,
      status: 'FAILED',
      snapshot,
      changePreview,
      evaluation,
      matchedConditionSummary: `${evalSummary} nhưng đẩy Meta thất bại.`,
      errorMessage: execFail.message,
      executionAttempts: 1,
      executionError: {
        ...(result.error ?? {}),
        kind: execFail.kind,
        autoRetried: false,
      },
      metaTraceId: result.metaTraceId,
      metaBudgetScheduleIds: result.scheduleIds,
    });
    return { matched: 1, errors: 1 };
  }

  /** Tập id HDP "của mình" (do các lần chạy trước của rule tạo cho entity này). */
  private async gatherOwnedScheduleIds(
    ruleId: string,
    entityId: string,
  ): Promise<Set<string>> {
    const items = await this.prisma.campaignRuleRunItem.findMany({
      where: {
        run: { ruleId },
        entityId,
        NOT: { metaBudgetScheduleIds: { isEmpty: true } },
      },
      select: { metaBudgetScheduleIds: true },
    });
    return new Set(items.flatMap((i) => i.metaBudgetScheduleIds).map(String));
  }

  /** Fetch insight LIVE khung HÔM NAY cho campaign/adset. Trả object phẳng (rỗng nếu không có). */
  private async fetchLiveInsight(
    level: 'CAMPAIGN' | 'ADSET',
    entityId: string,
  ): Promise<any> {
    return this.fetchInsightWithParams(
      level,
      entityId,
      { date_preset: 'today' },
      'today',
    );
  }

  /**
   * Gom mọi timeframe DISTINCT xuất hiện trong điều kiện của mọi task (đã normalize):
   *   - VALUE : params.timeframe
   *   - METRIC: params.leftTimeframe + params.rightTimeframe
   * LUÔN gồm 'today' (guard/snapshot/rolling đọc trên khung hôm nay). TIME/RANKING không
   * có metric-timeframe → bỏ qua.
   */
  private collectTimeframes(rule: any): Set<string> {
    const set = new Set<string>(['today']);
    const walk = (group: any) => {
      if (!group) return;
      for (const c of group.conditions || []) {
        const p = c?.params || {};
        if (c?.compareType === 'METRIC') {
          set.add(normalizeTimeframe(p.leftTimeframe));
          set.add(normalizeTimeframe(p.rightTimeframe));
        } else if (c?.compareType === 'VALUE') {
          set.add(normalizeTimeframe(p.timeframe));
        }
      }
      for (const g of group.childGroups || []) walk(g);
    };
    for (const task of rule.tasks || []) walk(task.rootGroup);
    return set;
  }

  /**
   * Fetch insight cho mỗi timeframe distinct (ngoài today — đã fetch sẵn ở caller). Trả
   * map { timeframe → insight | null } (+ map lỗi cho EXPLAIN). CÔ LẬP LỖI theo khung:
   * khung nào lỗi → value=null (điều kiện dùng khung đó tự KHÔNG khớp), KHÔNG ném lên
   * để giữ các khung/điều kiện khác chạy bình thường.
   */
  private async fetchInsightsByTimeframe(
    rule: any,
    level: 'CAMPAIGN' | 'ADSET',
    entityId: string,
    todayInsight: any,
    now: Date,
    timezone: string,
  ): Promise<{
    byTimeframe: Map<string, any>;
    errorsByTimeframe: Map<string, string>;
  }> {
    const byTimeframe = new Map<string, any>([['today', todayInsight]]);
    const errorsByTimeframe = new Map<string, string>();

    for (const tf of this.collectTimeframes(rule)) {
      if (tf === 'today') continue; // đã có sẵn
      try {
        const params = timeframeToMetaParams(tf, now, timezone);
        byTimeframe.set(
          tf,
          await this.fetchInsightWithParams(level, entityId, params, tf),
        );
      } catch (error) {
        const msg = parseMetaError(error).message;
        this.logger.warn(
          `Lấy insight ${level} ${entityId} khung ${tf} lỗi: ${msg} → điều kiện dùng khung này KHÔNG khớp (cô lập, các khung khác vẫn chạy).`,
        );
        byTimeframe.set(tf, null);
        errorsByTimeframe.set(tf, msg);
      }
    }
    return { byTimeframe, errorsByTimeframe };
  }

  /** Lõi fetch insight LIVE với tham số thời gian bất kỳ. Trả object phẳng (rỗng nếu không có). */
  /**
   * Số lần retry tối đa còn "mua" được bằng thời gian còn lại tới hạn chót của lượt chạy.
   *
   * PHÉP TÍNH worst-case cho MỘT lần gọi insight với k lần retry (lấy nhánh RATE-LIMIT
   * vì nó chậm nhất; công thức sleep của executeMetaApiWithRetry là `sleep × retry`):
   *   tổng ngủ  = 15s×1 + 15s×2 + 15s×3           (k=3) = 90s
   *   tổng gọi  = (k+1) lần × 30s timeout axios   (k=3) = 120s
   *   ⇒ k=3 → 210s | k=2 → 135s | k=1 → 75s | k=0 → 30s
   *
   * Chọn k lớn nhất mà worst-case vẫn ≤ thời gian còn lại ⇒ một lần gọi insight KHÔNG
   * BAO GIỜ kéo lượt chạy vượt hạn chót (= TTL khóa 300s trừ biên an toàn 30s = 270s).
   * Không còn thời gian → k=0: gọi đúng 1 lần, thà báo lỗi tạm thời (slot sẽ được thử
   * lại ở lượt sau) còn hơn chạy tiếp khi khóa đã có thể chết.
   */
  private retriesWithinDeadline(): number {
    const remainingMs = this.runDeadlineAt - Date.now();
    for (let k = 3; k >= 1; k -= 1) {
      // Tổng ngủ = INSIGHT_RATELIMIT_SLEEP_MS × (1+2+...+k) = ×k(k+1)/2
      const sleepMs = (INSIGHT_RATELIMIT_SLEEP_MS * k * (k + 1)) / 2;
      const callMs = (k + 1) * META_CALL_TIMEOUT_MS;
      if (sleepMs + callMs <= remainingMs) return k;
    }
    return 0;
  }

  private async fetchInsightWithParams(
    level: 'CAMPAIGN' | 'ADSET',
    entityId: string,
    timeParams: MetaInsightTimeParams,
    tfLabel: string,
  ): Promise<any> {
    // Meta hay chập chờn "no response was received" (timeout mạng) → retry với backoff.
    // getInsights là đọc-only nên retry an toàn.
    //
    // 🔴 BẮT BUỘC bó theo hạn chót của lượt chạy: mặc định `initialSleepMs` của
    // executeMetaApiWithRetry là 60s, mà công thức là `sleep × retry` → lỗi RATE-LIMIT
    // với maxRetries=3 sẽ ngủ 60+120+180 = 360s, DÀI HƠN TTL khóa (300s). Khóa hết hạn
    // giữa lúc còn đang `await sleep` → replica khác chiếm khóa, chạy cùng rule → BƠM
    // NGÂN SÁCH HAI LẦN. Vì vậy: backoff rút xuống 15s/30s/45s và số lần retry được
    // tính lại theo thời gian CÒN LẠI tới hạn chót.
    const maxRetries = this.retriesWithinDeadline();
    const rows = await executeMetaApiWithRetry(
      () =>
        level === 'CAMPAIGN'
          ? new Campaign(entityId).getInsights(INSIGHT_FIELDS, timeParams)
          : new AdSet(entityId).getInsights(INSIGHT_FIELDS, timeParams),
      {
        maxRetries,
        initialSleepMs: INSIGHT_RATELIMIT_SLEEP_MS,
        networkSleepMs: INSIGHT_NETWORK_SLEEP_MS,
        logger: this.logger,
        context: { scope: 'campaign-rule insight', level, entityId, timeframe: tfLabel },
      },
    );
    const first = Array.isArray(rows) ? rows[0] : rows?.[0];
    if (!first) return {};
    return first._data || first;
  }

  /** Snapshot metric đọc được + ngân sách, để UI log-detail hiển thị. */
  private buildSnapshot(insight: any, entity: RunnerEntity) {
    const metrics: Record<string, number | null> = {};
    for (const key of [
      'spend',
      'impressions',
      'reach',
      'frequency',
      'clicks',
      'ctr',
      'cpc',
      'cpm',
      'purchase_roas',
      'purchases',
    ]) {
      metrics[key] = resolveMetric(key, insight, entity);
    }
    return { metrics, budgets: this.budgetSnapshot(entity) };
  }

  private budgetSnapshot(entity: RunnerEntity) {
    return {
      dailyBudget: entity?.dailyBudget ?? null,
      lifetimeBudget: entity?.lifetimeBudget ?? null,
    };
  }

  /** Ghi một CampaignRuleRunItem. Gom mọi field optional để giữ call-site gọn. */
  private async createItem(args: {
    runId: string;
    task: any;
    level: 'CAMPAIGN' | 'ADSET';
    entity: RunnerEntity;
    status: string;
    snapshot: any;
    changePreview: any;
    evaluation: any;
    matchedConditionSummary?: string;
    errorMessage?: string;
    executedAt?: Date;
    executionAttempts?: number;
    executionError?: any;
    metaTraceId?: string;
    metaBudgetScheduleIds?: string[];
  }): Promise<void> {
    await this.prisma.campaignRuleRunItem.create({
      data: {
        runId: args.runId,
        taskId: args.task?.id ?? null,
        taskKind: args.task?.kind ?? null,
        level: args.level as any,
        entityId: args.entity.id,
        entityName: args.entity.name || args.entity.id,
        status: args.status as any,
        snapshot: args.snapshot ?? {},
        changePreview: args.changePreview ?? {},
        evaluation: args.evaluation ?? null,
        matchedConditionSummary: args.matchedConditionSummary ?? null,
        errorMessage: args.errorMessage ?? null,
        executedAt: args.executedAt ?? null,
        executionAttempts: args.executionAttempts ?? 0,
        executionError: args.executionError ?? null,
        metaTraceId: args.metaTraceId ?? null,
        metaBudgetScheduleIds: args.metaBudgetScheduleIds ?? [],
      },
    });
  }

  /** lastRunAt = max(scheduledFor) của các run trước đó của rule. */
  private async getLastRunAt(ruleId: string): Promise<Date | null> {
    const last = await this.prisma.campaignRuleRun.findFirst({
      where: { ruleId },
      orderBy: { scheduledFor: 'desc' },
      select: { scheduledFor: true },
    });
    return last?.scheduledFor ?? null;
  }

  /** rule.timezone hoặc, nếu "account", tz của ad account (fallback default). */
  private async resolveTimezone(rule: any): Promise<string> {
    if (rule.timezone && rule.timezone !== 'account') return rule.timezone;
    try {
      const account = await this.prisma.account.findUnique({
        where: { id: rule.accountId },
        select: { timezone: true },
      });
      return account?.timezone || DEFAULT_TIMEZONE;
    } catch {
      return DEFAULT_TIMEZONE;
    }
  }
}
