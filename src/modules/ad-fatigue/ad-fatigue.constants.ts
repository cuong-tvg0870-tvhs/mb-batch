const readFiniteNumber = (
  envName: string,
  fallback: number,
  options: { min?: number; max?: number } = {},
) => {
  const rawValue = process.env[envName]?.trim();
  if (!rawValue) return fallback;
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed)) return fallback;
  if (options.min !== undefined && parsed < options.min) return fallback;
  if (options.max !== undefined && parsed > options.max) return fallback;
  return parsed;
};

const watchFrequency = readFiniteNumber('AD_FATIGUE_WATCH_FREQUENCY', 2, {
  min: 0.01,
});
const fatiguedFrequency = Math.max(
  watchFrequency,
  readFiniteNumber('AD_FATIGUE_FATIGUED_FREQUENCY', 3, { min: 0.01 }),
);
const watchCtrDropRate =
  readFiniteNumber('AD_FATIGUE_WATCH_CTR_DROP_PCT', 15, {
    min: 0.01,
    max: 100,
  }) / 100;
const fatiguedCtrDropRate = Math.max(
  watchCtrDropRate,
  readFiniteNumber('AD_FATIGUE_FATIGUED_CTR_DROP_PCT', 25, {
    min: 0.01,
    max: 100,
  }) / 100,
);

export interface AdFatigueThresholds {
  watchFrequency: number;
  fatiguedFrequency: number;
  watchCtrDropRate: number;
  fatiguedCtrDropRate: number;
  minImpressionsPerWindow: number;
}

/**
 * Ngưỡng mặc định của bộ phát hiện fatigue v1.
 *
 * Override bằng env:
 * - AD_FATIGUE_WATCH_FREQUENCY (mặc định 2)
 * - AD_FATIGUE_FATIGUED_FREQUENCY (mặc định 3)
 * - AD_FATIGUE_WATCH_CTR_DROP_PCT (mặc định 15)
 * - AD_FATIGUE_FATIGUED_CTR_DROP_PCT (mặc định 25)
 * - AD_FATIGUE_MIN_IMPRESSIONS_PER_WINDOW (mặc định 1000)
 */
export const AD_FATIGUE_THRESHOLDS: Readonly<AdFatigueThresholds> =
  Object.freeze({
    watchFrequency,
    fatiguedFrequency,
    watchCtrDropRate,
    fatiguedCtrDropRate,
    minImpressionsPerWindow: Math.round(
      readFiniteNumber('AD_FATIGUE_MIN_IMPRESSIONS_PER_WINDOW', 1000, {
        min: 1,
      }),
    ),
  });

export const AD_FATIGUE_RULE_VERSION =
  process.env.AD_FATIGUE_RULE_VERSION?.trim() || 'v1';

export const AD_FATIGUE_WRITE_CHUNK_SIZE = Math.round(
  readFiniteNumber('AD_FATIGUE_WRITE_CHUNK_SIZE', 100, {
    min: 1,
    max: 500,
  }),
);

export const AD_FATIGUE_READ_CHUNK_SIZE = Math.round(
  readFiniteNumber('AD_FATIGUE_READ_CHUNK_SIZE', 500, {
    min: 1,
    max: 1000,
  }),
);

export const AD_FATIGUE_DEFAULT_TIMEZONE = 'Asia/Ho_Chi_Minh';
