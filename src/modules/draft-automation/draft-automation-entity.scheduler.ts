import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { DraftAutomation } from '@prisma/client';
import { CronExpressionParser } from 'cron-parser';
import { PrismaService } from '../prisma/prisma.service';
import {
  normalizeRuleSchedule,
  nextRunFromSchedule,
} from './draft-automation-schedule';
import {
  AutomationRunResult,
  DraftAutomationScheduler,
} from './draft-automation.scheduler';

// Reconciler quét mỗi 30 phút (khớp cadence engine legacy). Không dùng CronJob động
// per-row: một vòng quét đơn giản các row TỚI HẠN rồi chạy engine dùng lại.
const RECONCILE_CRON = '*/30 * * * *';
const DEFAULT_TIMEZONE = 'Asia/Ho_Chi_Minh';
const MIN_INTERVAL_MINUTES = 30;
const MIN_INTERVAL_MS = MIN_INTERVAL_MINUTES * 60 * 1000;
// Coi khóa chạy là "kẹt" và cho chiếm lại sau 30 phút (một lượt build+publish bình
// thường ngắn hơn nhiều; ngưỡng này chỉ để tự phục hồi khi tiến trình chết giữa chừng).
const RUN_LOCK_STUCK_MS = 30 * 60 * 1000;
// Công tắc tắt khẩn cấp TOÀN CỤC mọi tự động hóa (SystemConfig). value = { active: true }
// → bỏ qua toàn bộ lượt (cả cron lẫn run-now). Marketer/admin bật khi cần "phanh gấp".
const DRAFT_AUTOMATION_KILL_SWITCH_KEY = 'draft_automation_kill_switch';

/**
 * Scheduler cho THỰC THỂ MỚI `DraftAutomation` ("1 template → nhiều lượt tự động").
 * Đây là bản CHẠY-THEO-LỊCH của lệnh chạy on-demand bên mb-ads.
 *
 * Mỗi 30 phút: tìm các row ACTIVE tới hạn (sourceType=TEMPLATE, có templateId), với
 * mỗi row gọi DÙNG LẠI engine dựng-nháp qua `runner.runDraftAutomationOnce(row)` rồi
 * cập nhật run-tracking + lịch kế tiếp trên row. Đây là scheduler DUY NHẤT cho tự động
 * hóa dựng-nháp — hệ "automation cấu hình trong template" (cũ) đã được gỡ bỏ hoàn toàn.
 */
@Injectable()
export class DraftAutomationEntityScheduler {
  private readonly logger = new Logger(DraftAutomationEntityScheduler.name);
  // Chống chồng lượt: một vòng quét chưa xong thì không mở vòng mới (build có thể
  // lâu hơn 30 phút khi nhiều row).
  private running = false;

  constructor(
    private readonly prisma: PrismaService,
    private readonly runner: DraftAutomationScheduler,
  ) {}

  @Cron(RECONCILE_CRON, { timeZone: DEFAULT_TIMEZONE })
  async reconcileDueAutomations() {
    if (this.running) {
      this.logger.warn(
        'DraftAutomation reconcile bỏ qua: lượt quét trước chưa hoàn tất.',
      );
      return;
    }
    this.running = true;
    try {
      if (await this.isKillSwitchActive()) {
        this.logger.warn(
          'DraftAutomation: công tắc tắt khẩn cấp đang BẬT — bỏ qua toàn bộ lượt tự động hóa.',
        );
        return;
      }
      const now = new Date();
      // TỚI HẠN: ACTIVE, chưa xoá, nguồn TEMPLATE có templateId, và (chưa từng chạy)
      // hoặc (nextRunAt đã tới). Dùng chung nextRunAt cho cả INTERVAL lẫn CRON —
      // sau mỗi lượt ta luôn ghi nextRunAt (mốc kế tiếp) nên điều kiện lte là đủ.
      const dueRows = await this.prisma.draftAutomation.findMany({
        where: {
          status: 'ACTIVE',
          deletedAt: null,
          sourceType: 'TEMPLATE',
          templateId: { not: null },
          OR: [{ nextRunAt: null }, { nextRunAt: { lte: now } }],
        },
        orderBy: { nextRunAt: 'asc' },
      });

      if (dueRows.length === 0) return;

      this.logger.log(
        `DraftAutomation: ${dueRows.length} tự động hóa tới hạn chạy.`,
      );

      // Chạy tuần tự: engine đọc/ghi nhiều SystemCampaign dùng chung asset; chạy
      // song song dễ tranh asset. Một row lỗi không được chặn các row còn lại.
      for (const row of dueRows) {
        // CRON vừa tạo/sửa/resume có nextRunAt=null (mb-ads chưa tính mốc cron). Query
        // coi null là "tới hạn" → nếu chạy ngay sẽ SAI GIỜ HẸN (đăng sớm). Thay vì chạy,
        // SEED mốc cron kế tiếp để nó chỉ chạy đúng khung giờ đã đặt (finding A6).
        // Lịch KHUNG GIỜ (cron cũ, hoặc lịch JSON SPECIFIC mới) vừa tạo/sửa/resume có
        // nextRunAt=null. Query coi null là "tới hạn" → chạy ngay sẽ SAI GIỜ HẸN.
        const isSlotSchedule =
          row.scheduleType === 'CRON' ||
          normalizeRuleSchedule(row.schedule)?.type === 'SPECIFIC';
        if (isSlotSchedule && row.nextRunAt === null) {
          const seeded = this.computeNextRunAt(row, now);
          await this.prisma.draftAutomation
            .updateMany({
              where: { id: row.id, status: 'ACTIVE', nextRunAt: null },
              data: { nextRunAt: seeded },
            })
            .catch(() => undefined);
          this.logger.log(
            `DraftAutomation "${row.name}" (${row.id}): lịch khung giờ — hẹn mốc chạy đầu tiên ${seeded.toISOString()}.`,
          );
          continue;
        }
        await this.runOne(row);
      }
    } catch (err: any) {
      this.logger.error(
        'DraftAutomation reconcile lỗi:',
        this.formatError(err),
      );
    } finally {
      this.running = false;
    }
  }

  private async runOne(row: DraftAutomation) {
    const runAt = new Date();

    // Trần an toàn (P1): đạt maxRuns → COMPLETED, không chạy thêm (chống LOOP vô hạn).
    if (row.maxRuns != null && row.runCount >= row.maxRuns) {
      await this.prisma.draftAutomation
        .updateMany({
          where: { id: row.id, status: 'ACTIVE', deletedAt: null },
          data: { status: 'COMPLETED', nextRunAt: null, runLockedAt: null },
        })
        .catch(() => undefined);
      this.logger.log(
        `DraftAutomation "${row.name}" (${row.id}) đạt trần ${row.maxRuns} lượt → COMPLETED.`,
      );
      return;
    }

    // Claim NGUYÊN TỬ: chỉ 1 tiến trình lấy được khóa cho row này. Chống cron-tick trùng
    // "Chạy ngay" (mb-ads) và mb-batch multi-replica → không dựng 2 nháp / đăng đôi.
    // Điều kiện status=ACTIVE đồng thời chặn row vừa bị PAUSED giữa vòng quét (A5).
    const stuckBefore = new Date(runAt.getTime() - RUN_LOCK_STUCK_MS);
    const claim = await this.prisma.draftAutomation.updateMany({
      where: {
        id: row.id,
        status: 'ACTIVE',
        deletedAt: null,
        OR: [{ runLockedAt: null }, { runLockedAt: { lt: stuckBefore } }],
      },
      data: { runLockedAt: runAt },
    });
    if (claim.count === 0) {
      this.logger.warn(
        `DraftAutomation "${row.name}" (${row.id}) bỏ qua lượt: đang chạy ở tiến trình khác hoặc vừa tạm dừng.`,
      );
      return;
    }

    let result: AutomationRunResult;
    try {
      result = await this.runner.runDraftAutomationOnce(row);
    } catch (err: any) {
      // runDraftAutomationOnce đã bắt lỗi nội bộ; đây là lưới an toàn cuối.
      this.logger.error(
        `DraftAutomation "${row.name}" (${row.id}) lỗi khi chạy:`,
        this.formatError(err),
      );
      result = {
        status: 'FAILED',
        reason: err?.message || String(err),
        isComplete: false,
        published: false,
      };
    }

    // COMPLETED khi runMode=ONCE và lượt chạy đã hoàn tất (đủ asset). Với
    // publishMode=PUBLISH_IMMEDIATELY, isComplete=true nghĩa là đã gọi publish;
    // published phản ánh có thực đăng hay bị khóa bỏ qua. Ta chốt COMPLETED theo
    // isComplete (khớp legacy runMode ONCE && isComplete) và chỉ khi SUCCESS.
    const completed =
      row.runMode === 'ONCE' &&
      result.status === 'SUCCESS' &&
      result.isComplete;

    const nextRunAt = completed ? null : this.computeNextRunAt(row, runAt);

    try {
      // where status=ACTIVE: nếu user PAUSED/DELETE trong lúc chạy thì KHÔNG cập
      // nhật (0 row) → tôn trọng tạm dừng, không hồi sinh row. lastRunStatus dùng
      // lại enum DraftAutomationRunStatus (SUCCESS/SKIPPED/FAILED) trùng result.status.
      await this.prisma.draftAutomation.updateMany({
        where: { id: row.id, status: 'ACTIVE', deletedAt: null },
        data: {
          lastRunAt: runAt,
          runCount: { increment: 1 },
          lastRunStatus: result.status,
          lastRunReason: result.reason ?? null,
          // Ghi nháp "đang gom dở" của row (A7): xong → null, còn dở → generatedCampaignId.
          // Chỉ đụng khi chạy thành công (SKIPPED/FAILED giữ nguyên giá trị cũ).
          ...(result.status === 'SUCCESS'
            ? {
                inProgressDraftId: result.isComplete
                  ? null
                  : (result.generatedCampaignId ?? null),
              }
            : {}),
          ...(completed
            ? { status: 'COMPLETED', nextRunAt: null }
            : { nextRunAt }),
        },
      });
    } catch (err: any) {
      this.logger.error(
        `DraftAutomation "${row.name}" (${row.id}) cập nhật trạng thái lỗi:`,
        this.formatError(err),
      );
    }

    // Giải phóng khóa của CHÍNH lượt này (khớp runLockedAt=runAt). Tách khỏi update
    // trên vì update đó guard status=ACTIVE — nếu row bị PAUSED giữa chừng, update
    // không khớp nhưng khóa vẫn phải được nhả để lượt sau chạy được.
    await this.prisma.draftAutomation
      .updateMany({
        where: { id: row.id, runLockedAt: runAt },
        data: { runLockedAt: null },
      })
      .catch(() => undefined);

    this.logger.log(
      `DraftAutomation "${row.name}" (${row.id}): ${result.status}` +
        (result.reason ? ` — ${result.reason}` : '') +
        (completed
          ? ' → COMPLETED'
          : nextRunAt
            ? ` → kế tiếp ${nextRunAt.toISOString()}`
            : ''),
    );
  }

  /**
   * nextRunAt cho lượt kế tiếp (từ thời điểm vừa chạy `from`):
   * - INTERVAL: from + max(30, intervalMinutes) phút (ép tối thiểu 30 phút).
   * - CRON: lần khớp cron kế tiếp theo timezone của row; chặn cron dày < 30 phút.
   *   Cron không hợp lệ / quá dày → fallback from + 30 phút để row không bị treo.
   */
  private computeNextRunAt(row: DraftAutomation, from: Date): Date {
    // Lịch JSON (rule-builder, dùng chung với "Scale bài hiệu quả") là nguồn ƯU TIÊN.
    // Vẫn ép sàn 30 phút như đường cũ: cron quét của scheduler này chạy mỗi 30 phút nên
    // hẹn dày hơn thế là hẹn suông. Xem draft-automation-schedule.ts.
    const ruleSchedule = normalizeRuleSchedule(row.schedule);
    if (ruleSchedule) {
      const next = nextRunFromSchedule(
        ruleSchedule,
        from,
        row.timezone ?? DEFAULT_TIMEZONE,
      );
      if (next) {
        const floor = new Date(from.getTime() + MIN_INTERVAL_MS);
        return next.getTime() < floor.getTime() ? floor : next;
      }
      // null = lịch đã hết hạn (dateTo đã qua) hoặc không còn khung giờ nào. Hẹn xa để
      // row không bị quét lại mỗi tick; status sẽ do người dùng đổi.
      this.logger.log(
        `DraftAutomation "${row.name}" (${row.id}): lịch JSON không còn mốc kế tiếp — hoãn 24h.`,
      );
      return new Date(from.getTime() + 24 * 60 * 60 * 1000);
    }

    if (row.scheduleType === 'CRON' && row.cronExpression) {
      const next = this.computeCronNextRunAt(
        row.cronExpression,
        row.timezone ?? DEFAULT_TIMEZONE,
        from,
      );
      if (next) return next;
      this.logger.warn(
        `DraftAutomation "${row.name}" (${row.id}) cron không dùng được — fallback +30 phút.`,
      );
      return new Date(from.getTime() + MIN_INTERVAL_MS);
    }
    const minutes = Math.max(
      MIN_INTERVAL_MINUTES,
      Number(row.intervalMinutes) || MIN_INTERVAL_MINUTES,
    );
    return new Date(from.getTime() + minutes * 60 * 1000);
  }

  private computeCronNextRunAt(
    cronExpression: string,
    timezone: string,
    from: Date,
  ): Date | undefined {
    try {
      // Chặn cron dày < 30 phút (mirror assertCronIntervalIsSafe của cron scheduler
      // legacy): xét 12 mốc liên tiếp, khoảng cách phải >= 30 phút.
      const guard = CronExpressionParser.parse(cronExpression, {
        currentDate: from,
        tz: timezone,
      });
      let previous = guard.next().toDate();
      for (let i = 0; i < 12; i += 1) {
        const next = guard.next().toDate();
        if (next.getTime() - previous.getTime() < MIN_INTERVAL_MS) {
          throw new Error('Cron interval phải tối thiểu 30 phút.');
        }
        previous = next;
      }

      // Mốc cron kế tiếp sau `from` (chỉ cần > from một chút; interval-safety ở trên
      // đã đảm bảo khoảng cách giữa các mốc >= 30 phút, khớp semantics legacy).
      const interval = CronExpressionParser.parse(cronExpression, {
        currentDate: from,
        tz: timezone,
      });
      const minimumDate = new Date(from.getTime() + 1000);
      for (let i = 0; i < 100; i += 1) {
        const next = interval.next().toDate();
        if (next.getTime() >= minimumDate.getTime()) {
          return next;
        }
      }
    } catch (err: any) {
      this.logger.warn(
        `Cron "${cronExpression}" không dùng được: ${err?.message || err}`,
      );
    }
    return undefined;
  }

  private formatError(err: any) {
    return err?.stack || err?.message || String(err);
  }

  // Công tắc tắt khẩn cấp toàn cục: SystemConfig[draft_automation_kill_switch].active===true
  // → bỏ qua mọi lượt. Lỗi đọc config KHÔNG được chặn engine (fail-open: coi như tắt switch).
  private async isKillSwitchActive(): Promise<boolean> {
    try {
      const cfg = await this.prisma.systemConfig.findUnique({
        where: { key: DRAFT_AUTOMATION_KILL_SWITCH_KEY },
        select: { value: true },
      });
      return (cfg?.value as any)?.active === true;
    } catch {
      return false;
    }
  }
}
