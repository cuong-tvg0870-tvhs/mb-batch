import { Injectable, Logger } from '@nestjs/common';
import { createHash, randomUUID } from 'crypto';
import { PrismaService } from '../prisma/prisma.service';

export type AiApiProvider = 'gemini' | 'deepseek';

const SUPPORTED_AI_PROVIDERS: AiApiProvider[] = ['gemini', 'deepseek'];
const PROVIDER_DISPLAY_NAMES: Record<AiApiProvider, string> = {
  gemini: 'Gemini',
  deepseek: 'DeepSeek',
};

export type GeminiApiKeyLease = {
  id: string;
  provider: AiApiProvider;
  apiKey: string;
  keyHash: string;
  redacted: string;
};

type GeminiApiKeyRow = {
  id: string;
  provider: AiApiProvider;
  apiKey: string;
  keyHash: string;
};

type GeminiErrorInfo = {
  status?: number;
  code?: string;
  message: string;
  body?: any;
};

const parseInteger = (
  value: string | undefined,
  defaultValue: number,
  minValue: number,
) => {
  const parsed = Number(value ?? defaultValue);
  return Number.isFinite(parsed)
    ? Math.max(minValue, Math.floor(parsed))
    : defaultValue;
};

const parseOptionalInteger = (value: string | undefined) => {
  if (!value) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : null;
};

const parseRatio = (value: string | undefined, defaultValue: number) => {
  const parsed = Number(value ?? defaultValue);
  if (!Number.isFinite(parsed)) return defaultValue;
  return Math.min(1, Math.max(0.1, parsed));
};

@Injectable()
export class GeminiApiKeyManager {
  private readonly logger = new Logger(GeminiApiKeyManager.name);
  private ensureTablePromise?: Promise<void>;
  private readonly usageThreshold = parseRatio(
    process.env.HELP_AI_API_KEY_USAGE_THRESHOLD,
    0.9,
  );
  private readonly resetHour = parseInteger(
    process.env.HELP_AI_API_KEY_RESET_HOUR,
    0,
    0,
  );
  private readonly resetMinute = parseInteger(
    process.env.HELP_AI_API_KEY_RESET_MINUTE,
    0,
    0,
  );
  private readonly resetOffsetMinutes = parseInteger(
    process.env.HELP_AI_API_KEY_RESET_UTC_OFFSET_MINUTES,
    7 * 60,
    -14 * 60,
  );
  private readonly defaultCooldownMs = parseInteger(
    process.env.HELP_AI_API_KEY_DEFAULT_COOLDOWN_MS,
    60 * 60 * 1000,
    60 * 1000,
  );
  private readonly envDailyRequestLimit = parseOptionalInteger(
    process.env.HELP_AI_API_KEY_DAILY_REQUEST_LIMIT,
  );
  private readonly envDailyTokenLimit = parseOptionalInteger(
    process.env.HELP_AI_API_KEY_DAILY_TOKEN_LIMIT,
  );

  constructor(private readonly prisma: PrismaService) {}

  async hasConfiguredKeys() {
    await this.prepareKeyState();
    const rows = await this.prisma.$queryRawUnsafe<Array<{ count: number }>>(
      'SELECT COUNT(*)::int AS "count" FROM "HelpAiApiKey" WHERE "provider" = ANY($1::text[]) AND "status" != $2',
      SUPPORTED_AI_PROVIDERS,
      'DISABLED',
    );
    return Number(rows[0]?.count || 0) > 0;
  }

  async getKeyOrder(excludeKeyHashes: string[] = []) {
    await this.prepareKeyState();

    const rows = await this.prisma.$queryRawUnsafe<GeminiApiKeyRow[]>(
      `
        SELECT "id", "provider", "apiKey", "keyHash"
        FROM "HelpAiApiKey"
        WHERE "provider" = ANY($1::text[])
          AND "status" = 'ACTIVE'
          AND NOT ("keyHash" = ANY($3::text[]))
          AND ("blockedUntil" IS NULL OR "blockedUntil" <= CURRENT_TIMESTAMP)
          AND (
            "dailyRequestLimit" IS NULL
            OR "dailyRequestLimit" <= 0
            OR "requestsUsed" < GREATEST(1, FLOOR("dailyRequestLimit" * $2::double precision))::int
          )
          AND (
            "dailyTokenLimit" IS NULL
            OR "dailyTokenLimit" <= 0
            OR "tokensUsed" < GREATEST(1, FLOOR("dailyTokenLimit" * $2::double precision))::int
          )
        ORDER BY
          CASE "provider"
            WHEN 'gemini' THEN 1
            WHEN 'deepseek' THEN 2
            ELSE 3
          END,
          "lastUsedAt" ASC NULLS FIRST,
          "createdAt" ASC
      `,
      SUPPORTED_AI_PROVIDERS,
      this.usageThreshold,
      excludeKeyHashes,
    );

    return rows.map((row) => ({
      id: row.id,
      provider: this.normalizeProvider(row.provider),
      apiKey: row.apiKey,
      keyHash: row.keyHash,
      redacted: this.redact(row.apiKey),
    }));
  }

  async recordAttempt(key: GeminiApiKeyLease) {
    await this.prisma.$executeRawUnsafe(
      `
        UPDATE "HelpAiApiKey"
        SET "requestsUsed" = "requestsUsed" + 1,
            "lastUsedAt" = CURRENT_TIMESTAMP,
            "updatedAt" = CURRENT_TIMESTAMP
        WHERE "id" = $1
      `,
      key.id,
    );
  }

  async recordSuccess(key: GeminiApiKeyLease, responseJson?: any) {
    const tokens = this.extractTotalTokens(responseJson);
    await this.prisma.$executeRawUnsafe(
      `
        UPDATE "HelpAiApiKey"
        SET "tokensUsed" = "tokensUsed" + $2,
            "lastSuccessAt" = CURRENT_TIMESTAMP,
            "lastErrorCode" = NULL,
            "lastErrorMessage" = NULL,
            "updatedAt" = CURRENT_TIMESTAMP
        WHERE "id" = $1
      `,
      key.id,
      tokens,
    );
  }

  async recordFailure(key: GeminiApiKeyLease, error: GeminiErrorInfo) {
    await this.prisma.$executeRawUnsafe(
      `
        UPDATE "HelpAiApiKey"
        SET "lastFailureAt" = CURRENT_TIMESTAMP,
            "lastErrorCode" = $2,
            "lastErrorMessage" = $3,
            "updatedAt" = CURRENT_TIMESTAMP
        WHERE "id" = $1
      `,
      key.id,
      error.code || String(error.status || ''),
      error.message.slice(0, 1000),
    );
  }

  async freezeForQuota(key: GeminiApiKeyLease, error: GeminiErrorInfo) {
    const blockedUntil = this.resolveBlockedUntil(
      error.body?.error || error.body,
    );
    await this.prisma.$executeRawUnsafe(
      `
        UPDATE "HelpAiApiKey"
        SET "status" = 'COOLDOWN',
            "blockedUntil" = $2,
            "blockedReason" = $3,
            "quotaResetAt" = $2,
            "lastFailureAt" = CURRENT_TIMESTAMP,
            "lastErrorCode" = $4,
            "lastErrorMessage" = $5,
            "updatedAt" = CURRENT_TIMESTAMP
        WHERE "id" = $1
      `,
      key.id,
      blockedUntil,
      error.message.slice(0, 1000),
      error.code || String(error.status || ''),
      error.message.slice(0, 1000),
    );

    this.logger.warn(
      `${this.providerDisplayName(key.provider)} API key ${key.redacted} quota/rate-limited. Cooling down until ${blockedUntil.toISOString()}`,
    );
  }

  async disableKey(key: GeminiApiKeyLease, error: GeminiErrorInfo) {
    await this.prisma.$executeRawUnsafe(
      `
        UPDATE "HelpAiApiKey"
        SET "status" = 'DISABLED',
            "blockedReason" = $2,
            "lastFailureAt" = CURRENT_TIMESTAMP,
            "lastErrorCode" = $3,
            "lastErrorMessage" = $4,
            "updatedAt" = CURRENT_TIMESTAMP
        WHERE "id" = $1
      `,
      key.id,
      error.message.slice(0, 1000),
      error.code || String(error.status || ''),
      error.message.slice(0, 1000),
    );
  }

  async thawExpiredKeys() {
    await this.ensureTable();
    await this.resetExpiredUsageWindows();
    await this.prisma.$executeRawUnsafe(
      `
        UPDATE "HelpAiApiKey"
        SET "status" = 'ACTIVE',
            "blockedUntil" = NULL,
            "blockedReason" = NULL,
            "quotaResetAt" = NULL,
            "updatedAt" = CURRENT_TIMESTAMP
        WHERE "provider" = ANY($1::text[])
          AND "status" = 'COOLDOWN'
          AND "blockedUntil" IS NOT NULL
          AND "blockedUntil" <= CURRENT_TIMESTAMP
      `,
      SUPPORTED_AI_PROVIDERS,
    );
  }

  async readAiError(response: globalThis.Response): Promise<GeminiErrorInfo> {
    const status = response.status;
    const text = await response.text();
    let body: any = null;

    try {
      body = text ? JSON.parse(text) : null;
    } catch {
      body = null;
    }

    const apiError = body?.error || body;
    return {
      status,
      code: apiError?.status || apiError?.code || String(status),
      message: apiError?.message || text || `AI provider error ${status}`,
      body,
    };
  }

  async readGeminiError(
    response: globalThis.Response,
  ): Promise<GeminiErrorInfo> {
    return this.readAiError(response);
  }

  isQuotaError(error: GeminiErrorInfo) {
    const status = String(error.code || '').toUpperCase();
    const message = error.message.toLowerCase();

    return (
      error.status === 429 ||
      status === 'RESOURCE_EXHAUSTED' ||
      message.includes('quota') ||
      message.includes('rate limit') ||
      message.includes('too many requests')
    );
  }

  isAuthError(error: GeminiErrorInfo) {
    const status = String(error.code || '').toUpperCase();
    const message = error.message.toLowerCase();

    return (
      error.status === 401 ||
      error.status === 403 ||
      status === 'PERMISSION_DENIED' ||
      status === 'UNAUTHENTICATED' ||
      message.includes('api key not valid') ||
      message.includes('api_key_invalid')
    );
  }

  isRetryableStatus(status?: number) {
    return status === 429 || status === 500 || status === 502 || status === 503;
  }

  redact(key: string) {
    if (key.length <= 8) return '***';
    return `${key.slice(0, 4)}...${key.slice(-4)}`;
  }

  private async prepareKeyState() {
    await this.ensureTable();
    await this.seedEnvKeys();
    await this.thawExpiredKeys();
  }

  private async ensureTable() {
    if (!this.ensureTablePromise) {
      this.ensureTablePromise = this.createTable();
    }

    return this.ensureTablePromise;
  }

  private async createTable() {
    await this.prisma.$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS "HelpAiApiKey" (
        "id" TEXT PRIMARY KEY,
        "provider" TEXT NOT NULL DEFAULT 'gemini',
        "label" TEXT,
        "apiKey" TEXT NOT NULL,
        "keyHash" TEXT NOT NULL UNIQUE,
        "status" TEXT NOT NULL DEFAULT 'ACTIVE',
        "blockedUntil" TIMESTAMP(3),
        "blockedReason" TEXT,
        "quotaResetAt" TIMESTAMP(3),
        "requestsUsed" INTEGER NOT NULL DEFAULT 0,
        "tokensUsed" INTEGER NOT NULL DEFAULT 0,
        "usageWindowStartedAt" TIMESTAMP(3),
        "usageWindowResetAt" TIMESTAMP(3),
        "dailyRequestLimit" INTEGER,
        "dailyTokenLimit" INTEGER,
        "lastUsedAt" TIMESTAMP(3),
        "lastSuccessAt" TIMESTAMP(3),
        "lastFailureAt" TIMESTAMP(3),
        "lastErrorCode" TEXT,
        "lastErrorMessage" TEXT,
        "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
        "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
      )
    `);
    await this.prisma.$executeRawUnsafe(
      'CREATE INDEX IF NOT EXISTS "HelpAiApiKey_provider_status_blockedUntil_idx" ON "HelpAiApiKey" ("provider", "status", "blockedUntil")',
    );
    await this.prisma.$executeRawUnsafe(
      'CREATE INDEX IF NOT EXISTS "HelpAiApiKey_usageWindowResetAt_idx" ON "HelpAiApiKey" ("usageWindowResetAt")',
    );
  }

  private async seedEnvKeys() {
    await this.seedProviderEnvKeys('gemini', [
      process.env.GEMINI_API_KEY,
      process.env.GEMINI_API_KEYS,
    ]);
    await this.seedProviderEnvKeys('deepseek', [
      process.env.DEEPSEEK_API_KEY,
      process.env.DEEPSEEK_API_KEYS,
    ]);
  }

  private async seedProviderEnvKeys(
    provider: AiApiProvider,
    envValues: Array<string | undefined>,
  ) {
    const keys = envValues
      .filter(Boolean)
      .join(',')
      .split(',')
      .map((key) => key.trim())
      .filter(Boolean);

    for (const key of keys) {
      await this.prisma.$executeRawUnsafe(
        `
          INSERT INTO "HelpAiApiKey" (
            "id",
            "provider",
            "label",
            "apiKey",
            "keyHash",
            "status",
            "usageWindowStartedAt",
            "usageWindowResetAt",
            "dailyRequestLimit",
            "dailyTokenLimit",
            "createdAt",
            "updatedAt"
          )
          VALUES ($1, $2, $3, $4, $5, 'ACTIVE', CURRENT_TIMESTAMP, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          ON CONFLICT ("keyHash") DO NOTHING
        `,
        randomUUID(),
        provider,
        `${this.providerDisplayName(provider)} ${this.redact(key)}`,
        key,
        this.hashKey(key),
        this.getNextDailyResetAt(new Date()),
        this.envDailyRequestLimit,
        this.envDailyTokenLimit,
      );
    }
  }

  private async resetExpiredUsageWindows() {
    await this.prisma.$executeRawUnsafe(
      `
        UPDATE "HelpAiApiKey"
        SET "requestsUsed" = 0,
            "tokensUsed" = 0,
            "usageWindowStartedAt" = CURRENT_TIMESTAMP,
            "usageWindowResetAt" = $2,
            "updatedAt" = CURRENT_TIMESTAMP
        WHERE "provider" = ANY($1::text[])
          AND ("usageWindowResetAt" IS NULL OR "usageWindowResetAt" <= CURRENT_TIMESTAMP)
      `,
      SUPPORTED_AI_PROVIDERS,
      this.getNextDailyResetAt(new Date()),
    );
  }

  private resolveBlockedUntil(error: any) {
    const retryDelayMs = this.extractRetryDelayMs(error);
    if (retryDelayMs > 0) return new Date(Date.now() + retryDelayMs);
    return new Date(Date.now() + this.defaultCooldownMs);
  }

  private extractRetryDelayMs(error: any) {
    const details = Array.isArray(error?.details) ? error.details : [];
    for (const detail of details) {
      const retryDelay = detail?.retryDelay;
      if (typeof retryDelay !== 'string') continue;
      const match = retryDelay.match(/^(\d+(?:\.\d+)?)s$/);
      if (!match) continue;
      return Math.ceil(Number(match[1]) * 1000);
    }

    return 0;
  }

  private getNextDailyResetAt(now: Date) {
    const offsetMs = this.resetOffsetMinutes * 60 * 1000;
    const shiftedNow = new Date(now.getTime() + offsetMs);
    const resetAtShifted = new Date(
      Date.UTC(
        shiftedNow.getUTCFullYear(),
        shiftedNow.getUTCMonth(),
        shiftedNow.getUTCDate(),
        Math.min(23, this.resetHour),
        Math.min(59, this.resetMinute),
        0,
        0,
      ),
    );

    if (resetAtShifted.getTime() <= shiftedNow.getTime()) {
      resetAtShifted.setUTCDate(resetAtShifted.getUTCDate() + 1);
    }

    return new Date(resetAtShifted.getTime() - offsetMs);
  }

  private extractTotalTokens(responseJson: any) {
    const total = Number(
      responseJson?.usageMetadata?.totalTokenCount ||
        responseJson?.usage?.total_tokens ||
        0,
    );
    return Number.isFinite(total) && total > 0 ? Math.floor(total) : 0;
  }

  private hashKey(key: string) {
    return createHash('sha256').update(key).digest('hex');
  }

  private normalizeProvider(provider?: string): AiApiProvider {
    const normalized = String(provider || 'gemini')
      .toLowerCase()
      .trim();
    if (normalized === 'gemini' || normalized === 'deepseek') {
      return normalized;
    }

    return 'gemini';
  }

  private providerDisplayName(provider: AiApiProvider) {
    return PROVIDER_DISPLAY_NAMES[provider] || provider;
  }
}
