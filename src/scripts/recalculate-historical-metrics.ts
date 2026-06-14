import * as dotenv from 'dotenv';
dotenv.config();

import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient, CreativeStatus, InsightRange, Prisma } from '@prisma/client';
import { Pool } from 'pg';
import dayjs from 'dayjs';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL as string,
  max: 20, // Safe pool size
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});
const adapter = new PrismaPg(pool);
const prisma = new PrismaClient({ adapter });

function toNumber(value: any, defaultValue = 0): number {
  if (value === null || value === undefined) return defaultValue;
  const num = Number(value);
  return Number.isFinite(num) ? num : defaultValue;
}

function getActionValue(actions: any, type: string): number {
  if (!actions || !Array.isArray(actions)) return 0;
  const found = actions.find((a: any) => a.action_type === type);
  return toNumber(found?.value);
}

function getActionValueFromValues(values: any, type: string): number {
  if (!values || !Array.isArray(values)) return 0;
  const found = values.find((a: any) => a.action_type === type);
  return toNumber(found?.value);
}

function extractCampaignMetricsFromRaw(insight: any) {
  function getVal(actions: any[] | undefined, type: string) {
    if (!actions) return 0;
    const found = actions.find((a) => a.action_type === type);
    return toNumber(found?.value);
  }

  function getValFromValues(values: any[] | undefined, type: string) {
    if (!values) return 0;
    const found = values.find((a) => a.action_type === type);
    return toNumber(found?.value);
  }

  const spend = toNumber(insight?.spend);
  const clicks = toNumber(insight?.clicks);

  const registrationComplete =
    getVal(insight?.actions, 'complete_registration') +
    getVal(insight?.actions, 'offsite_conversion.complete_registration');

  const registrationCompleteValue =
    getValFromValues(insight?.action_values, 'complete_registration') +
    getValFromValues(
      insight?.action_values,
      'offsite_conversion.complete_registration',
    );

  const purchases =
    getVal(insight?.actions, 'purchase') +
    getVal(insight?.actions, 'onsite_conversion.purchase');

  const purchaseValue =
    getValFromValues(insight?.action_values, 'purchase') +
    getValFromValues(
      insight?.action_values,
      'onsite_conversion.purchase',
    );

  const roas = spend > 0 ? purchaseValue / spend : 0;
  const cvr = clicks > 0 ? registrationComplete / clicks : 0;
  const adsCostRatio = roas > 0 ? 1 / roas : 0;
  
  const results = Math.round(Number(purchases) + Number(registrationComplete));
  const aov = results > 0 ? Math.round(Number(purchaseValue) / results) : null;
  const costPerResult = results > 0 ? spend / results : 0;

  return {
    purchases: Math.round(purchases),
    purchaseValue,
    roas,
    cvr,
    adsCostRatio,
    results,
    aov,
    costPerResult,
    registrationComplete: Math.round(registrationComplete),
    registrationCompleteValue,
  };
}

// -------------------------------------------------------------
// STANDALONE CREATIVE AGGREGATION LOGIC
// -------------------------------------------------------------
async function createManySafe(prismaModel: any, data: any[], batchSize = 100) {
  for (let i = 0; i < data.length; i += batchSize) {
    const batch = data.slice(i, i + batchSize);
    await prismaModel.createMany({ data: batch });
  }
}

async function aggregateCreativeInsights(accountId: string) {
  console.log(`   [${accountId}] Aggregating CreativeInsight...`);
  const today = dayjs().format('YYYY-MM-DD');
  const sevenDaysAgo = dayjs().subtract(6, 'day').format('YYYY-MM-DD');
  const threeDaysAgo = dayjs().subtract(2, 'day').format('YYYY-MM-DD');

  // 1. Load all creatives for this account
  const creatives = await prisma.creative.findMany({
    where: { accountId },
    select: {
      id: true,
      ads: { select: { id: true } },
    },
  });

  if (creatives.length === 0) {
    console.log(`   [${accountId}] No creatives found to aggregate.`);
    return;
  }

  const batchSize = 100;
  for (let i = 0; i < creatives.length; i += batchSize) {
    const batch = creatives.slice(i, i + batchSize);
    const adIds = batch.flatMap((c) => c.ads.map((a) => a.id));
    if (!adIds.length) continue;

    // 2. Load AdInsights for these ads
    const insights = await prisma.adInsight.findMany({
      where: {
        adId: { in: adIds },
        range: {
          in: [
            InsightRange.MAX,
            InsightRange.DAY_7,
            InsightRange.DAY_3,
            InsightRange.TODAY,
            InsightRange.DAILY,
          ],
        },
      },
    });

    // 3. Group insights by AdId
    const insightMap = new Map<string, any[]>();
    for (const ins of insights) {
      if (!insightMap.has(ins.adId)) insightMap.set(ins.adId, []);
      insightMap.get(ins.adId)!.push(ins);
    }

    const creativeInsightUpserts: any[] = [];
    const creativeUpdates: any[] = [];

    const sumMetrics = (target: Record<string, number>, source: any) => {
      for (const key in source) {
        if (typeof source[key] === 'number') {
          target[key] = (target[key] || 0) + source[key];
        }
      }
    };

    const recalculateDerivedMetrics = (target: Record<string, number>) => {
      const impressions = target.impressions || 0;
      const clicks = target.clicks || 0;
      const spend = target.spend || 0;
      const purchases = target.purchases || 0;
      const purchaseValue = target.purchaseValue || 0;
      const registrationComplete = target.registrationComplete || 0;
      const results = purchases + registrationComplete;
      const videoPlay = target.videoPlay || 0;
      const video3s = target.video3s || 0;
      const video100 = target.video100 || 0;
      const uniqueClicks = target.uniqueClicks || 0;
      const reach = target.reach || 0;

      target.ctr = impressions > 0 ? (clicks / impressions) * 100 : 0;
      target.cpc = clicks > 0 ? spend / clicks : 0;
      target.cpm = impressions > 0 ? (spend / impressions) * 1000 : 0;
      target.roas = spend > 0 ? purchaseValue / spend : 0;
      target.cvr = clicks > 0 ? registrationComplete / clicks : 0;
      target.costPerResult = results > 0 ? spend / results : 0;
      target.aov = results > 0 ? purchaseValue / results : 0;
      target.adsCostRatio = target.roas > 0 ? 1 / target.roas : 0;
      target.hookRate = videoPlay > 0 ? +((video3s / videoPlay) * 100).toFixed(2) : 0;
      target.holdRate = video3s > 0 ? +((video100 / video3s) * 100).toFixed(2) : 0;
      target.uniqueCtr = reach > 0 ? (uniqueClicks / reach) * 100 : 0;
      target.results = results;
    };

    // 4. Calculate for each Creative in batch
    for (const creative of batch) {
      const ads = creative.ads.map((a) => a.id);
      const bucket = {
        max: {} as Record<string, number>,
        last7d: {} as Record<string, number>,
        last3d: {} as Record<string, number>,
        today: {} as Record<string, number>,
        daily: {} as Record<string, Record<string, number>>,
      };

      const dateBounds = {
        max: { minStart: null as string | null, maxStop: null as string | null },
        last7d: { minStart: null as string | null, maxStop: null as string | null },
        last3d: { minStart: null as string | null, maxStop: null as string | null },
        today: { minStart: null as string | null, maxStop: null as string | null },
      };

      const updateBounds = (rangeKey: 'max' | 'last7d' | 'last3d' | 'today', ins: any) => {
        const start = ins.dateStart;
        const stop = ins.dateStop;
        if (start) {
          if (!dateBounds[rangeKey].minStart || start < dateBounds[rangeKey].minStart) {
            dateBounds[rangeKey].minStart = start;
          }
        }
        if (stop) {
          if (!dateBounds[rangeKey].maxStop || stop > dateBounds[rangeKey].maxStop) {
            dateBounds[rangeKey].maxStop = stop;
          }
        }
      };

      for (const adId of ads) {
        const adInsights = insightMap.get(adId) || [];
        for (const ins of adInsights) {
          if (ins.range === InsightRange.MAX) {
            sumMetrics(bucket.max, ins);
            updateBounds('max', ins);
          }
          if (ins.range === InsightRange.DAY_7) {
            sumMetrics(bucket.last7d, ins);
            updateBounds('last7d', ins);
          }
          if (ins.range === InsightRange.DAY_3) {
            sumMetrics(bucket.last3d, ins);
            updateBounds('last3d', ins);
          }
          if (ins.range === InsightRange.TODAY) {
            sumMetrics(bucket.today, ins);
            updateBounds('today', ins);
          }
          if (ins.range === InsightRange.DAILY) {
            if (!bucket.daily[ins.dateStart])
              bucket.daily[ins.dateStart] = {};
            sumMetrics(bucket.daily[ins.dateStart], ins);
          }
        }
      }

      recalculateDerivedMetrics(bucket.max);
      recalculateDerivedMetrics(bucket.last7d);
      recalculateDerivedMetrics(bucket.last3d);
      recalculateDerivedMetrics(bucket.today);
      for (const dateStart of Object.keys(bucket.daily)) {
        recalculateDerivedMetrics(bucket.daily[dateStart]);
      }

      // Add to upsert list
      const ranges: any[] = [
        {
          range: InsightRange.MAX,
          dateStart: dateBounds.max.minStart || '1975-01-01',
          dateStop: dateBounds.max.maxStop || today,
          data: bucket.max,
        },
        {
          range: InsightRange.DAY_7,
          dateStart: dateBounds.last7d.minStart || sevenDaysAgo,
          dateStop: dateBounds.last7d.maxStop || today,
          data: bucket.last7d,
        },
        {
          range: InsightRange.DAY_3,
          dateStart: dateBounds.last3d.minStart || threeDaysAgo,
          dateStop: dateBounds.last3d.maxStop || today,
          data: bucket.last3d,
        },
        {
          range: InsightRange.TODAY,
          dateStart: dateBounds.today.minStart || today,
          dateStop: dateBounds.today.maxStop || today,
          data: bucket.today,
        },
      ];

      for (const dateStart of Object.keys(bucket.daily)) {
        ranges.push({
          range: InsightRange.DAILY,
          dateStart: dateStart,
          dateStop: dateStart,
          data: bucket.daily[dateStart],
        });
      }

      for (const r of ranges) {
        creativeInsightUpserts.push({
          creativeId: creative.id,
          dateStart: r.dateStart,
          range: r.range,
          data: { dateStop: r.dateStop, ...r.data },
        });
      }

      // Calculate performance status
      const maxSpend = bucket.max.spend ?? 0;
      const maxRevenue = bucket.max.purchaseValue ?? 0;
      const maxPurchases = bucket.max.purchases ?? 0;
      const maxClicks = bucket.max.clicks ?? 0;
      const maxImpressions = bucket.max.impressions ?? 0;

      const roasMax = maxSpend > 0 ? maxRevenue / maxSpend : 0;
      const ctrMax = maxImpressions > 0 ? maxClicks / maxImpressions : 0;
      const roas7d =
        (bucket.last7d.spend ?? 0) > 0
          ? (bucket.last7d.purchaseValue ?? 0) / bucket.last7d.spend
          : 0;
      const roas3d =
        (bucket.last3d.spend ?? 0) > 0
          ? (bucket.last3d.purchaseValue ?? 0) / bucket.last3d.spend
          : 0;

      let status: CreativeStatus = CreativeStatus.OTHER;
      if (maxSpend === 0) status = CreativeStatus.OTHER;
      else if (maxSpend <= 100000) status = CreativeStatus.NEED_SPEND;
      else if (
        ((maxSpend <= 500000 && roasMax >= 2) ||
          (maxSpend > 500000 && roasMax >= 2.2)) &&
        roas7d >= 2.5
      ) {
        status = CreativeStatus.SCALE_P1;
      } else if (
        ((maxSpend <= 500000 && roasMax >= 1.5) ||
          (maxSpend > 500000 && roasMax >= 1.8 && ctrMax > 0.03)) &&
        roas7d >= 2.2 &&
        roas3d >= 2.2
      ) {
        status = CreativeStatus.SCALE_P2;
      } else if (
        (maxSpend <= 500000 && maxPurchases < 1 && ctrMax > 0.03) ||
        (maxSpend > 500000 && roasMax < 1.8 && ctrMax > 0.03)
      ) {
        status = CreativeStatus.REVIEW;
      } else if (
        (maxSpend <= 500000 && maxPurchases < 1 && ctrMax < 0.03) ||
        (maxSpend > 500000 && roasMax < 1.8 && ctrMax < 0.03)
      ) {
        status = CreativeStatus.OFF;
      }

      creativeUpdates.push({
        id: creative.id,
        data: { performanceStatus: status, ...bucket.max },
      });
    }

    // 5. Delete existing insights for these ranges to avoid duplicates
    await prisma.creativeInsight.deleteMany({
      where: {
        creativeId: { in: batch.map((c) => c.id) },
        range: {
          in: [
            InsightRange.MAX,
            InsightRange.DAY_7,
            InsightRange.DAY_3,
            InsightRange.TODAY,
            InsightRange.DAILY,
          ],
        },
      },
    });

    // 5.1 Batch Create CreativeInsights
    const createData = creativeInsightUpserts.map((item) => ({
      creativeId: item.creativeId,
      dateStart: item.dateStart,
      range: item.range,
      ...item.data,
    }));

    await createManySafe(prisma.creativeInsight, createData, 100);

    // 6. Fetch Insight IDs to update relations on Creative
    const insightRecords = await prisma.creativeInsight.findMany({
      where: {
        creativeId: { in: batch.map((c) => c.id) },
        range: {
          in: [
            InsightRange.MAX,
            InsightRange.DAY_7,
            InsightRange.DAY_3,
            InsightRange.TODAY,
          ],
        },
      },
      select: { id: true, creativeId: true, range: true },
    });

    const insightMapByCreative = new Map<string, any>();
    for (const r of insightRecords) {
      if (!insightMapByCreative.has(r.creativeId))
        insightMapByCreative.set(r.creativeId, {});
      const obj = insightMapByCreative.get(r.creativeId);
      if (r.range === InsightRange.MAX) obj.max = r.id;
      if (r.range === InsightRange.DAY_7) obj.d7 = r.id;
      if (r.range === InsightRange.DAY_3) obj.d3 = r.id;
      if (r.range === InsightRange.TODAY) obj.today = r.id;
    }

    // 7. Update Creative performance status and insight IDs
    for (const item of creativeUpdates) {
      const ref = insightMapByCreative.get(item.id);
      try {
        await prisma.creative.update({
          where: { id: item.id },
          data: {
            ...item.data,
            insightMaxId: ref?.max,
            insight7dId: ref?.d7,
            insight3dId: ref?.d3,
            insightTodayId: ref?.today,
          },
        });
      } catch (err: any) {
        if (
          err instanceof Prisma.PrismaClientKnownRequestError &&
          err.code === 'P2003'
        ) {
          console.warn(`      ⚠️ Foreign key constraint violation on creative ${item.id} update. Skipping...`);
        } else {
          throw err;
        }
      }
    }
  }
}

// -------------------------------------------------------------
// MAIN RECALCULATION LOOP WITH CURSOR PAGINATION & CONCURRENCY CONTROL
// -------------------------------------------------------------
async function run() {
  console.log('🚀 Starting historical metrics recalculation script (standalone rawPayload version)...');

  await prisma.$connect();
  console.log('✨ Connected to PostgreSQL database via adapter');

  const levels = ['CampaignInsight', 'AdSetInsight', 'AdInsight'];
  const pageSize = 1000;
  const writeConcurrency = 50;

  for (const level of levels) {
    console.log(`\n⏳ Processing ${level} in pages of ${pageSize}...`);
    
    let lastId = '';
    let totalProcessed = 0;
    
    while (true) {
      const records = await (prisma[level as any] as any).findMany({
        where: lastId ? { id: { gt: lastId } } : {},
        orderBy: { id: 'asc' },
        take: pageSize,
        select: {
          id: true,
          spend: true,
          clicks: true,
          actions: true,
          actionValues: true,
          rawPayload: true,
        },
      });

      if (records.length === 0) {
        break;
      }

      for (let j = 0; j < records.length; j += writeConcurrency) {
        const subBatch = records.slice(j, j + writeConcurrency);
        
        await Promise.all(
          subBatch.map(async (record: any) => {
            let metrics: any;

            if (record.rawPayload) {
              metrics = extractCampaignMetricsFromRaw(record.rawPayload);
            } else {
              // Fallback if rawPayload is missing
              const spend = toNumber(record.spend);
              const clicks = toNumber(record.clicks);
              const actions = record.actions;
              const actionValues = record.actionValues;

              const registrationComplete =
                getActionValue(actions, 'complete_registration') +
                getActionValue(actions, 'offsite_conversion.complete_registration');

              const purchases =
                getActionValue(actions, 'purchase') +
                getActionValue(actions, 'onsite_conversion.purchase');

              const purchaseValue =
                getActionValueFromValues(actionValues, 'purchase') +
                getActionValueFromValues(actionValues, 'onsite_conversion.purchase');

              const roas = spend > 0 ? purchaseValue / spend : 0;
              const cvr = clicks > 0 ? registrationComplete / clicks : 0;
              const adsCostRatio = roas > 0 ? 1 / roas : 0;
              const results = purchases + registrationComplete;
              const costPerResult = results > 0 ? spend / results : 0;
              const aov = results > 0 ? purchaseValue / results : 0;

              const registrationCompleteValue =
                getActionValueFromValues(actionValues, 'complete_registration') +
                getActionValueFromValues(
                  actionValues,
                  'offsite_conversion.complete_registration',
                );

              metrics = {
                purchases,
                purchaseValue,
                roas,
                cvr,
                adsCostRatio,
                results,
                costPerResult,
                aov,
                registrationComplete,
                registrationCompleteValue,
              };
            }

            await (prisma[level as any] as any).update({
              where: { id: record.id },
              data: metrics,
            });
          })
        );
      }

      totalProcessed += records.length;
      lastId = records[records.length - 1].id;
      console.log(`   Processed page: cumulative total ${totalProcessed} records...`);
    }

    console.log(`✅ Completed ${level}. Total: ${totalProcessed} records`);
  }

  console.log('\n🚀 Triggering Creative Aggregation to update creative statistics and status...');
  const accounts = await prisma.account.findMany({
    where: { needsReauth: false },
    select: { id: true },
  });
  for (const account of accounts) {
    console.log(`   Aggregating creative insights for account: ${account.id}...`);
    await aggregateCreativeInsights(account.id);
  }
  console.log('✅ Creative Aggregation completed successfully!');

  await prisma.$disconnect();
  await pool.end();
  console.log('\n🎉 Finished recalculation script successfully!');
}

run().catch(async (err) => {
  console.error('❌ Error running recalculation script:', err);
  await prisma.$disconnect();
  await pool.end();
  process.exit(1);
});
