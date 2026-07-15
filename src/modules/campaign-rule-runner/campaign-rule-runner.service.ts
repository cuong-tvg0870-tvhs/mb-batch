import { Injectable, Logger } from '@nestjs/common';
import { AdSet, Campaign, FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import { executeMetaApiWithRetry, parseMetaError } from '../../common/utils';
import { AppConfigReader } from '../app-config/app-config.reader';
import { DistributedLockService } from '../distributed-lock/distributed-lock.service';
import { PrismaService } from '../prisma/prisma.service';
import {
  DEFAULT_TIMEZONE,
  INSIGHT_FIELDS,
  MAX_GROUP_DEPTH,
  RULE_LOCK_TTL_SECONDS,
} from './campaign-rule-runner.constants';
import {
  EvalContext,
  explainGroup,
  summarizeEvaluation,
} from './campaign-rule-evaluator';
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
} from './campaign-rule-schedule.util';

/** Include đệ quy cây group điều kiện tới độ sâu cố định (Prisma cần depth hữu hạn). */
function groupInclude(depth: number): any {
  if (depth <= 0) return { conditions: true };
  return {
    conditions: true,
    childGroups: { include: groupInclude(depth - 1) },
  };
}

/** Entity tối giản mà runner cần để đánh giá + thực thi. */
interface RunnerEntity {
  id: string;
  name?: string | null;
  dailyBudget?: number | null;
  lifetimeBudget?: number | null;
}

/**
 * Runner "campaign rule": cron quét rule ACTIVE, chạy nhánh "Theo điều kiện".
 *
 * Ràng buộc:
 * - Chỉ đọc/ghi bảng campaign_rule* + đọc Campaign/AdSet/Account. KHÔNG đụng Automation*.
 * - Chỉ action BUDGET_SCHEDULE_BUMP; chỉ level CAMPAIGN + ADSET. Các trường hợp khác → SKIPPED/log.
 * - Idempotent qua dedupeKey (unique trên CampaignRuleRun).
 * - Metric LIVE fetch trực tiếp từ Meta (date_preset=today), KHÔNG đọc insight cache DB.
 */
@Injectable()
export class CampaignRuleRunnerService {
  private readonly logger = new Logger(CampaignRuleRunnerService.name);
  private metaInitialized = false;

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

    if (!dueness.due) {
      if (dueness.outOfDateRange) {
        this.logger.debug(
          `Rule ${rule.id} ngoài khoảng ngày hiệu lực → bỏ qua.`,
        );
      }
      return;
    }

    const aligned = dueness.aligned || alignedNow(now);
    const key = dedupeKey(rule.id, rule.accountId, aligned);

    await this.lock.runExclusive(
      `crr:${rule.id}`,
      RULE_LOCK_TTL_SECONDS,
      async () => {
        await this.executeRun(rule, timezone, aligned, key, now);
      },
    );
  }

  /** Tạo run (idempotent) rồi đánh giá + thực thi từng entity. */
  private async executeRun(
    rule: any,
    timezone: string,
    aligned: Date,
    key: string,
    now: Date,
  ): Promise<void> {
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
          ruleSnapshot: {
            name: rule.name,
            level: rule.level,
            autoExecute: rule.autoExecute,
            timezone,
          },
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
        const entities = await this.loadEntities(rule);
        for (const entity of entities) {
          entitiesScanned += 1;
          const res = await this.processEntity(rule, run.id, entity, timezone, now);
          matchedCount += res.matched;
          errorsCount += res.errors;
        }
      }
    } catch (error) {
      fatalError = parseMetaError(error).message || String(error);
      this.logger.error(`Rule ${rule.id} run ${run.id} lỗi: ${fatalError}`);
    }

    await this.prisma.campaignRuleRun.update({
      where: { id: run.id },
      data: {
        status: fatalError ? 'FAILED' : 'COMPLETED',
        finishedAt: new Date(),
        entitiesScanned,
        matchedCount,
        errorsCount,
        errorMessage: fatalError,
      },
    });

    this.logger.log(
      `✅ Rule ${rule.id} run ${run.id}: quét ${entitiesScanned}, khớp ${matchedCount}, lỗi ${errorsCount}.`,
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
      },
    });
  }

  /**
   * Fetch insight live cho 1 entity, đánh giá MỌI task, ghi item tương ứng.
   * Lỗi fetch insight → tất cả task của entity đó thành FAILED (không làm hỏng entity khác).
   */
  private async processEntity(
    rule: any,
    runId: string,
    entity: RunnerEntity,
    timezone: string,
    now: Date,
  ): Promise<{ matched: number; errors: number }> {
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

    let insight: any;
    try {
      insight = await this.fetchLiveInsight(level, entity.id);
    } catch (error) {
      const msg = parseMetaError(error).message;
      this.logger.warn(
        `Lấy insight ${level} ${entity.id} lỗi: ${msg} → item FAILED.`,
      );
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
          evaluation: { matched: false, insightError: msg },
          errorMessage: `Lỗi lấy insight: ${msg}`,
        });
      }
      return { matched: 0, errors };
    }

    const ctx: EvalContext = { insight, entity, now, timezone };
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
          timezone,
          now,
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
            errorMessage:
              result.error?.message || 'Đẩy budget schedule thất bại.',
            executionAttempts: 1,
            executionError: result.error,
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
    timezone: string;
    now: Date;
  }): Promise<{ matched: number; errors: number }> {
    const { runId, task, rule, level, entity, insight, timezone, now } = args;
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

    const ctx: EvalContext = { insight, entity: liveEntity, now, timezone };
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

    const { spec, skipReason } = buildRollingSpec(rolling, {
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
        changePreview: { rolling: { mode: 'ROLLING', skipReason } },
        evaluation,
        matchedConditionSummary: `${evalSummary} nhưng chưa tạo khung (${skipReason ?? 'không rõ'}).`,
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
      errorMessage: result.error?.message || 'Đẩy budget schedule thất bại.',
      executionAttempts: 1,
      executionError: result.error,
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

  /** Fetch insight LIVE (date_preset=today) cho campaign/adset. Trả object phẳng (rỗng nếu không có). */
  private async fetchLiveInsight(
    level: 'CAMPAIGN' | 'ADSET',
    entityId: string,
  ): Promise<any> {
    const params = { date_preset: 'today' };
    // Meta hay chập chờn "no response was received" (timeout mạng) → retry NGẮN 2 lần
    // (3s, 6s) cho lỗi transient. getInsights là đọc-only nên retry an toàn; backoff
    // ngắn để không kéo dài tick runner (mỗi entity 1 lần/tick).
    const rows = await executeMetaApiWithRetry(
      () =>
        level === 'CAMPAIGN'
          ? new Campaign(entityId).getInsights(INSIGHT_FIELDS, params)
          : new AdSet(entityId).getInsights(INSIGHT_FIELDS, params),
      {
        maxRetries: 2,
        networkSleepMs: 3000,
        initialSleepMs: 3000,
        logger: this.logger,
        context: { scope: 'campaign-rule insight', level, entityId },
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
