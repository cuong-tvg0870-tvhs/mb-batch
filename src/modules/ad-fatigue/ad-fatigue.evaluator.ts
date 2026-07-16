import { AdFatigueStatus } from '@prisma/client';
import {
  AD_FATIGUE_THRESHOLDS,
  AdFatigueThresholds,
} from './ad-fatigue.constants';

export interface AdFatigueMetricWindow {
  impressions: number;
  clicks: number;
  spend?: number;
}

export interface AdFatigueEvaluationInput {
  /** Frequency từ insight DAY_7; null khi chưa có rollup. */
  frequency: number | null;
  current: AdFatigueMetricWindow;
  previous: AdFatigueMetricWindow;
}

export interface AdFatigueEvaluationResult {
  status: AdFatigueStatus;
  score: number;
  reasons: string[];
  frequency: number | null;
  currentCtr: number | null;
  previousCtr: number | null;
  /** Tỷ lệ suy giảm tương đối 0..1, ví dụ 0.25 = giảm 25%. */
  ctrDropRate: number | null;
  currentImpressions: number;
  previousImpressions: number;
  currentSpend: number;
}

export interface AdFatigueLifecycleState {
  status: AdFatigueStatus;
  detectedAt: Date | null;
  resolvedAt: Date | null;
}

const safeNonNegative = (value: unknown) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
};

const round = (value: number, precision = 6) => {
  const factor = 10 ** precision;
  return Math.round(value * factor) / factor;
};

const formatNumber = (value: number, maximumFractionDigits = 2) =>
  new Intl.NumberFormat('vi-VN', { maximumFractionDigits }).format(value);

const formatPercent = (value: number) =>
  new Intl.NumberFormat('vi-VN', {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  }).format(value * 100);

const calculateCtr = (clicks: number, impressions: number) =>
  impressions > 0 ? (clicks / impressions) * 100 : null;

const calculateScore = (
  frequency: number,
  ctrDropRate: number,
  thresholds: AdFatigueThresholds,
) => {
  if (thresholds.watchFrequency <= 0 || thresholds.watchCtrDropRate <= 0) {
    return 0;
  }

  // Cả tần suất lẫn CTR drop đều phải xấu nên lấy nhánh rủi ro thấp hơn.
  // Điểm 60 tương ứng vừa chạm WATCH; ngưỡng FATIGUED mặc định đạt khoảng 90.
  const jointRisk = Math.min(
    frequency / thresholds.watchFrequency,
    ctrDropRate / thresholds.watchCtrDropRate,
  );
  return Math.max(0, Math.min(100, Math.round(jointRisk * 60)));
};

export function evaluateAdFatigue(
  input: AdFatigueEvaluationInput,
  thresholds: AdFatigueThresholds = AD_FATIGUE_THRESHOLDS,
): AdFatigueEvaluationResult {
  const currentImpressions = Math.round(
    safeNonNegative(input.current.impressions),
  );
  const previousImpressions = Math.round(
    safeNonNegative(input.previous.impressions),
  );
  const currentClicks = safeNonNegative(input.current.clicks);
  const previousClicks = safeNonNegative(input.previous.clicks);
  const currentSpend = round(safeNonNegative(input.current.spend));
  const frequencyValue = Number(input.frequency);
  const frequency =
    input.frequency !== null &&
    Number.isFinite(frequencyValue) &&
    frequencyValue > 0
      ? round(frequencyValue)
      : null;

  const currentCtr = calculateCtr(currentClicks, currentImpressions);
  const previousCtr = calculateCtr(previousClicks, previousImpressions);
  const insufficientReasons: string[] = [];

  if (frequency === null) {
    insufficientReasons.push(
      'Chưa có tần suất 7 ngày để đánh giá mức độ lặp lại quảng cáo.',
    );
  }
  if (currentImpressions < thresholds.minImpressionsPerWindow) {
    insufficientReasons.push(
      `Ba ngày gần nhất chỉ có ${formatNumber(currentImpressions, 0)} lượt hiển thị, thấp hơn ngưỡng tối thiểu ${formatNumber(thresholds.minImpressionsPerWindow, 0)}.`,
    );
  }
  if (previousImpressions < thresholds.minImpressionsPerWindow) {
    insufficientReasons.push(
      `Ba ngày đối chứng chỉ có ${formatNumber(previousImpressions, 0)} lượt hiển thị, thấp hơn ngưỡng tối thiểu ${formatNumber(thresholds.minImpressionsPerWindow, 0)}.`,
    );
  }
  if (previousCtr === null || previousCtr <= 0) {
    insufficientReasons.push(
      'CTR của ba ngày đối chứng bằng 0 nên chưa thể tính tỷ lệ suy giảm.',
    );
  }

  if (insufficientReasons.length > 0) {
    return {
      status: AdFatigueStatus.INSUFFICIENT_DATA,
      score: 0,
      reasons: insufficientReasons,
      frequency,
      currentCtr: currentCtr === null ? null : round(currentCtr),
      previousCtr: previousCtr === null ? null : round(previousCtr),
      ctrDropRate: null,
      currentImpressions,
      previousImpressions,
      currentSpend,
    };
  }

  const ctrDropRate = round(
    Math.max(0, (previousCtr! - (currentCtr || 0)) / previousCtr!),
  );
  const isFatigued =
    frequency! >= thresholds.fatiguedFrequency &&
    ctrDropRate >= thresholds.fatiguedCtrDropRate;
  const isWatch =
    frequency! >= thresholds.watchFrequency &&
    ctrDropRate >= thresholds.watchCtrDropRate;
  const status = isFatigued
    ? AdFatigueStatus.FATIGUED
    : isWatch
      ? AdFatigueStatus.WATCH
      : AdFatigueStatus.HEALTHY;

  const reasons: string[] = [];
  if (status === AdFatigueStatus.HEALTHY) {
    reasons.push(
      'Quảng cáo chưa đồng thời vượt ngưỡng tần suất và mức giảm CTR.',
    );
  } else {
    reasons.push(
      `Tần suất 7 ngày xấp xỉ ${formatNumber(frequency!)} (tính từ dữ liệu DAY_7 đã rollup).`,
    );
    reasons.push(
      `CTR ba ngày gần nhất giảm ${formatPercent(ctrDropRate)}% so với ba ngày trước đó (${formatNumber(previousCtr!, 2)}% → ${formatNumber(currentCtr || 0, 2)}%).`,
    );
    reasons.push(
      status === AdFatigueStatus.FATIGUED
        ? 'Quảng cáo có dấu hiệu mệt mỏi rõ rệt; nên cân nhắc thay nội dung hoặc giảm phân phối.'
        : 'Quảng cáo bắt đầu có dấu hiệu mệt mỏi; nên theo dõi và chuẩn bị nội dung thay thế.',
    );
  }

  return {
    status,
    score: calculateScore(frequency!, ctrDropRate, thresholds),
    reasons,
    frequency,
    currentCtr: round(currentCtr || 0),
    previousCtr: round(previousCtr!),
    ctrDropRate,
    currentImpressions,
    previousImpressions,
    currentSpend,
  };
}

/**
 * Giữ một cảnh báo đang mở xuyên qua trạng thái thiếu dữ liệu. Chỉ HEALTHY mới
 * là bằng chứng để đóng cảnh báo; việc Ad ngừng phân phối không đồng nghĩa đã hết
 * fatigue.
 */
export function resolveAdFatigueLifecycle(
  existing: AdFatigueLifecycleState | undefined,
  nextStatus: AdFatigueStatus,
  evaluatedAt: Date,
) {
  const wasAlert =
    existing?.status === AdFatigueStatus.WATCH ||
    existing?.status === AdFatigueStatus.FATIGUED;
  const hasOpenAlert = Boolean(
    wasAlert || (existing?.detectedAt && !existing.resolvedAt),
  );
  const isAlert =
    nextStatus === AdFatigueStatus.WATCH ||
    nextStatus === AdFatigueStatus.FATIGUED;

  return {
    detectedAt: isAlert
      ? hasOpenAlert
        ? existing?.detectedAt || evaluatedAt
        : evaluatedAt
      : existing?.detectedAt || null,
    resolvedAt: isAlert
      ? null
      : nextStatus === AdFatigueStatus.HEALTHY && hasOpenAlert
        ? evaluatedAt
        : existing?.resolvedAt || null,
  };
}
