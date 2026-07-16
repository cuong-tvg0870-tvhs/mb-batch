import { Injectable, Logger } from '@nestjs/common';
import { AdFatigueStatus, InsightRange, Prisma } from '@prisma/client';
import dayjs from 'dayjs';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';
import { chunk, executeDbWithRetry } from '../../common/utils';
import { PrismaService } from '../prisma/prisma.service';
import {
  AD_FATIGUE_DEFAULT_TIMEZONE,
  AD_FATIGUE_READ_CHUNK_SIZE,
  AD_FATIGUE_RULE_VERSION,
  AD_FATIGUE_WRITE_CHUNK_SIZE,
} from './ad-fatigue.constants';
import {
  AdFatigueMetricWindow,
  evaluateAdFatigue,
  resolveAdFatigueLifecycle,
} from './ad-fatigue.evaluator';

dayjs.extend(utc);
dayjs.extend(timezone);

interface AdFatigueAccountSummary {
  accountId: string;
  evaluated: number;
  healthy: number;
  watch: number;
  fatigued: number;
  insufficientData: number;
}

interface MutableAdMetrics {
  current: AdFatigueMetricWindow;
  previous: AdFatigueMetricWindow;
}

@Injectable()
export class AdFatigueService {
  private readonly logger = new Logger(AdFatigueService.name);

  constructor(private readonly prisma: PrismaService) {}

  async evaluateAccount(
    accountId: string,
    accountTimezone?: string | null,
    evaluatedAt = new Date(),
  ): Promise<AdFatigueAccountSummary> {
    const timezoneName = this.normalizeTimezone(accountTimezone);
    const windows = this.buildComparisonWindows(timezoneName, evaluatedAt);

    const ads = await executeDbWithRetry(
      () =>
        this.prisma.ad.findMany({
          where: { accountId, deletedAt: null },
          select: {
            id: true,
            insight7d: { select: { frequency: true } },
          },
        }),
      { logger: this.logger },
    );

    const summary: AdFatigueAccountSummary = {
      accountId,
      evaluated: ads.length,
      healthy: 0,
      watch: 0,
      fatigued: 0,
      insufficientData: 0,
    };

    if (ads.length === 0) {
      this.logger.log(
        `[${accountId}] Không có quảng cáo để đánh giá Ad Fatigue.`,
      );
      return summary;
    }

    for (const adChunk of chunk(ads, AD_FATIGUE_READ_CHUNK_SIZE)) {
      const adIds = adChunk.map((ad) => ad.id);
      const [dailyInsights, existingStates] = await executeDbWithRetry(
        () =>
          Promise.all([
            this.prisma.adInsight.findMany({
              where: {
                adId: { in: adIds },
                range: InsightRange.DAILY,
                dateStart: {
                  gte: windows.previousStart,
                  lte: windows.currentEnd,
                },
              },
              select: {
                adId: true,
                dateStart: true,
                impressions: true,
                clicks: true,
                spend: true,
              },
            }),
            this.prisma.adFatigueState.findMany({
              where: { adId: { in: adIds } },
              select: {
                adId: true,
                status: true,
                detectedAt: true,
                resolvedAt: true,
              },
            }),
          ]),
        { logger: this.logger },
      );

      const metricMap = new Map<string, MutableAdMetrics>();
      for (const ad of adChunk) {
        metricMap.set(ad.id, {
          current: { impressions: 0, clicks: 0, spend: 0 },
          previous: { impressions: 0, clicks: 0, spend: 0 },
        });
      }

      for (const insight of dailyInsights) {
        const metrics = metricMap.get(insight.adId);
        if (!metrics) continue;
        const target =
          insight.dateStart >= windows.currentStart
            ? metrics.current
            : metrics.previous;
        target.impressions += Number(insight.impressions || 0);
        target.clicks += Number(insight.clicks || 0);
        target.spend = Number(target.spend || 0) + Number(insight.spend || 0);
      }

      const existingStateMap = new Map(
        existingStates.map((state) => [state.adId, state]),
      );
      const snapshots = adChunk.map((ad) => {
        const metrics = metricMap.get(ad.id)!;
        const result = evaluateAdFatigue({
          // Con trỏ insight7d vừa được rollup cập nhật; frequency vẫn là số
          // xấp xỉ vì reach cộng từ các DAILY có thể trùng người.
          frequency: ad.insight7d?.frequency ?? null,
          current: metrics.current,
          previous: metrics.previous,
        });
        const existing = existingStateMap.get(ad.id);
        const { detectedAt, resolvedAt } = resolveAdFatigueLifecycle(
          existing,
          result.status,
          evaluatedAt,
        );
        const snapshot = {
          status: result.status,
          score: result.score,
          reasons: result.reasons as Prisma.InputJsonValue,
          frequency: result.frequency,
          currentCtr: result.currentCtr,
          previousCtr: result.previousCtr,
          ctrDropRate: result.ctrDropRate,
          currentImpressions: result.currentImpressions,
          previousImpressions: result.previousImpressions,
          currentSpend: result.currentSpend,
          ruleVersion: AD_FATIGUE_RULE_VERSION,
          evaluatedAt,
          detectedAt,
          resolvedAt,
        };

        if (result.status === AdFatigueStatus.FATIGUED) summary.fatigued++;
        else if (result.status === AdFatigueStatus.WATCH) summary.watch++;
        else if (result.status === AdFatigueStatus.HEALTHY) summary.healthy++;
        else summary.insufficientData++;

        return { adId: ad.id, snapshot };
      });

      for (const snapshotChunk of chunk(
        snapshots,
        AD_FATIGUE_WRITE_CHUNK_SIZE,
      )) {
        await executeDbWithRetry(
          () =>
            this.prisma.$transaction(
              snapshotChunk.map(({ adId, snapshot }) =>
                this.prisma.adFatigueState.upsert({
                  where: { adId },
                  update: snapshot,
                  create: { adId, ...snapshot },
                }),
              ),
            ),
          { logger: this.logger },
        );
      }
    }

    this.logger.log(
      `[${accountId}] Đã đánh giá Ad Fatigue cho ${summary.evaluated} quảng cáo (${windows.currentStart}→${windows.currentEnd} so với ${windows.previousStart}→${windows.previousEnd}, múi giờ ${timezoneName}): ${summary.fatigued} mệt mỏi, ${summary.watch} cần theo dõi, ${summary.healthy} khỏe, ${summary.insufficientData} thiếu dữ liệu. Không có quảng cáo nào bị tự động tắt.`,
    );

    return summary;
  }

  private buildComparisonWindows(timezoneName: string, evaluatedAt: Date) {
    const now = dayjs(evaluatedAt).tz(timezoneName);
    const currentEnd = now.subtract(1, 'day');
    const currentStart = currentEnd.subtract(2, 'day');
    const previousEnd = currentStart.subtract(1, 'day');
    const previousStart = previousEnd.subtract(2, 'day');

    return {
      currentStart: currentStart.format('YYYY-MM-DD'),
      currentEnd: currentEnd.format('YYYY-MM-DD'),
      previousStart: previousStart.format('YYYY-MM-DD'),
      previousEnd: previousEnd.format('YYYY-MM-DD'),
    };
  }

  private normalizeTimezone(value?: string | null) {
    if (!value) return AD_FATIGUE_DEFAULT_TIMEZONE;
    try {
      dayjs().tz(value);
      return value;
    } catch {
      this.logger.warn(
        `Múi giờ tài khoản "${value}" không hợp lệ; dùng ${AD_FATIGUE_DEFAULT_TIMEZONE} để đánh giá Ad Fatigue.`,
      );
      return AD_FATIGUE_DEFAULT_TIMEZONE;
    }
  }
}
