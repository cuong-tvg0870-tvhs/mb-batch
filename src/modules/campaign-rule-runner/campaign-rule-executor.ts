import { Logger } from '@nestjs/common';
import { AdSet, Campaign } from 'facebook-nodejs-business-sdk';
import { parseMetaError } from '../../common/utils';
import { wallClockToUnix } from './campaign-rule-tz.util';

/**
 * Thực thi action BUDGET_SCHEDULE_BUMP: đẩy `budget_schedule_specs` lên Meta cho
 * campaign (CBO) hoặc ad set (ABO). Meta tự bơm ngân sách theo khung giờ rồi revert.
 *
 * Giả định FacebookAdsApi.init(...) ĐÃ được gọi (service làm 1 lần trước khi chạy).
 */

const logger = new Logger('CampaignRuleExecutor');

export interface BudgetPeriod {
  timeStart?: string; // "YYYY-MM-DDTHH:mm" (local)
  timeEnd?: string;
  budgetValueType?: 'ABSOLUTE' | 'MULTIPLIER' | string;
  budgetValue?: number;
}

export interface BudgetScheduleSpec {
  time_start: number;
  time_end: number;
  budget_value: number;
  budget_value_type: string;
}

export interface ExecResult {
  ok: boolean;
  error?: any;
  metaTraceId?: string;
  // Id các HighDemandPeriod Meta trả về (để sau tắt/xoá).
  scheduleIds: string[];
}

/**
 * periods (từ task.params) → budget_schedule_specs của Meta.
 * QUAN TRỌNG: Meta yêu cầu `budget_value` là SỐ NGUYÊN → luôn gửi ABSOLUTE.
 *   %  (MULTIPLIER 1.5)  → ABSOLUTE = round(ngân_sách_hằng_ngày × 1.5) theo ngân
 *                          sách THẬT của đối tượng (`targetBudget`, minor units).
 *   Số tiền (ABSOLUTE)   → round(budget_value).
 * Mốc giờ "YYYY-MM-DDTHH:mm" được diễn giải theo MÚI GIỜ TKQC (`tz`, IANA) — Meta
 * chạy budget schedule theo timezone tài khoản quảng cáo, không theo tz server.
 * Bỏ qua period thiếu thời gian hợp lệ, hoặc MULTIPLIER mà không biết targetBudget.
 */
export function buildSpecs(
  periods: any,
  targetBudget?: number | null,
  tz?: string | null,
): BudgetScheduleSpec[] {
  if (!Array.isArray(periods)) return [];
  const specs: BudgetScheduleSpec[] = [];
  for (const period of periods) {
    if (!period) continue;
    const timeStart = wallClockToUnix(String(period.timeStart), tz);
    const timeEnd = wallClockToUnix(String(period.timeEnd), tz);
    if (!Number.isFinite(timeStart) || !Number.isFinite(timeEnd)) continue;

    const type = period.budgetValueType || 'ABSOLUTE';
    const rawValue = Number(period.budgetValue);
    if (!Number.isFinite(rawValue)) continue;

    let budgetValue: number;
    if (type === 'MULTIPLIER') {
      if (!targetBudget || targetBudget <= 0) continue; // không quy đổi được → bỏ qua
      budgetValue = Math.round(targetBudget * rawValue);
    } else {
      budgetValue = Math.round(rawValue);
    }
    if (!(budgetValue > 0)) continue;

    specs.push({
      time_start: timeStart,
      time_end: timeEnd,
      budget_value: budgetValue,
      budget_value_type: 'ABSOLUTE',
    });
  }
  return specs;
}

/**
 * Đẩy specs lên Meta theo 2 pha: validate_only rồi commit. Trả {ok,error,metaTraceId}.
 * Bắt mọi lỗi (rate-limit/permission/validate) → ok=false + error đã parse.
 */
export async function executeBudgetSchedule(
  level: 'CAMPAIGN' | 'ADSET' | string,
  entityId: string,
  specs: BudgetScheduleSpec[],
): Promise<ExecResult> {
  if (!specs || specs.length === 0) {
    return { ok: false, error: { message: 'Không có budget_schedule_specs để đẩy' }, scheduleIds: [] };
  }

  if (level !== 'CAMPAIGN' && level !== 'ADSET') {
    return {
      ok: false,
      error: { message: `Level ${level} không hỗ trợ BUDGET_SCHEDULE_BUMP` },
      scheduleIds: [],
    };
  }

  // Với entity ĐÃ TỒN TẠI phải TẠO từng budget schedule qua edge
  // /{id}/budget_schedules (SDK createBudgetSchedule). Set field budget_schedule_specs
  // qua update chỉ hợp lệ lúc CREATE campaign → nếu không sẽ "Invalid parameter".
  const scheduleIds: string[] = [];
  try {
    for (const spec of specs) {
      const params = {
        time_start: spec.time_start,
        time_end: spec.time_end,
        budget_value: spec.budget_value,
        budget_value_type: spec.budget_value_type,
      };
      const hdp =
        level === 'CAMPAIGN'
          ? await new Campaign(entityId).createBudgetSchedule([], params)
          : await new AdSet(entityId).createBudgetSchedule([], params);
      const sid = (hdp as { id?: string; _data?: { id?: string } })?.id ?? (hdp as { _data?: { id?: string } })?._data?.id;
      if (sid) scheduleIds.push(String(sid));
    }

    // Bật cờ tổng để Meta tick "Schedule budget increases" + thực sự áp dụng
    // (tạo khung qua edge không tự bật). Best-effort — lỗi không làm hỏng kết quả.
    if (scheduleIds.length > 0) {
      try {
        if (level === 'CAMPAIGN')
          await new Campaign(entityId).update([], { is_budget_schedule_enabled: true });
        else await new AdSet(entityId).update([], { is_budget_schedule_enabled: true });
      } catch (e) {
        logger.warn(
          `Bật is_budget_schedule_enabled ${level} ${entityId} lỗi: ${parseMetaError(e).message}`,
        );
      }
    }

    return { ok: true, scheduleIds };
  } catch (error) {
    const parsed = parseMetaError(error);
    const userMsg =
      (error as { response?: { error_user_msg?: string; error_user_title?: string } })?.response
        ?.error_user_msg ||
      (error as { response?: { error_user_title?: string } })?.response?.error_user_title ||
      parsed.message;
    logger.warn(
      `Đẩy budget_schedule ${level} ${entityId} thất bại: ${userMsg}` +
        (parsed.fbtrace_id ? ` (trace ${parsed.fbtrace_id})` : ''),
    );
    return {
      ok: false,
      error: {
        message: userMsg,
        code: parsed.code,
        subcode: parsed.subcode,
        type: parsed.type,
        blameFields: parsed.blameFields,
      },
      metaTraceId: parsed.fbtrace_id,
      // Có thể vài khung đã tạo được trước khi lỗi — vẫn lưu để tắt được.
      scheduleIds,
    };
  }
}
