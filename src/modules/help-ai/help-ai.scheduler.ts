import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { BatchRunLoggerService } from '../batch-run-log/batch-run-logger.service';
import { DistributedLockService } from '../distributed-lock/distributed-lock.service';
import {
  HELP_AI_API_KEY_THAW_CRON,
  HELP_AI_KNOWLEDGE_REFRESH_CRON,
  HELP_AI_TIME_ZONE,
  HELP_AI_TRIAGE_CRON,
} from './help-ai.constants';
import { GeminiApiKeyManager } from './gemini-api-key-manager.service';
import { HelpAiService } from './help-ai.service';

const HELP_AI_QUEUE = 'help-ai';
// TTL khóa cross-replica cho từng job (giây). Đặt rộng hơn thời lượng chạy tối đa để
// khóa không hết hạn giữa chừng. Thaw API-key rất nhanh nên TTL ngắn.
const LOCK_TTL_REFRESH = 30 * 60;
const LOCK_TTL_TRIAGE = 30 * 60;
const LOCK_TTL_THAW = 5 * 60;

@Injectable()
export class HelpAiScheduler implements OnModuleInit {
  private readonly logger = new Logger(HelpAiScheduler.name);
  private refreshingKnowledge = false;
  private triaging = false;
  private thawingApiKeys = false;

  constructor(
    private readonly helpAiService: HelpAiService,
    private readonly geminiKeyManager: GeminiApiKeyManager,
    private readonly batchRunLogger: BatchRunLoggerService,
    private readonly lock: DistributedLockService,
  ) {}

  async onModuleInit() {
    this.logger.log('HelpAiScheduler initialized');
    await this.refreshHelpKnowledge();
  }

  @Cron(HELP_AI_KNOWLEDGE_REFRESH_CRON, { timeZone: HELP_AI_TIME_ZONE })
  async refreshHelpKnowledge() {
    return this.batchRunLogger.track(
      'HELP_AI_REFRESH_KNOWLEDGE',
      HELP_AI_QUEUE,
      async (ctx) => {
        if (this.refreshingKnowledge) {
          ctx.skip('a run is already active');
          this.logger.warn(
            'Help knowledge refresh skipped because a run is active',
          );
          return;
        }

        this.refreshingKnowledge = true;
        try {
          const ran = await this.lock.runExclusive(
            'help-ai:refresh-knowledge',
            LOCK_TTL_REFRESH,
            () => this.helpAiService.refreshHelpKnowledge(),
          );
          if (!ran) ctx.skip('another instance holds the lock');
        } finally {
          this.refreshingKnowledge = false;
        }
      },
    );
  }

  @Cron(HELP_AI_TRIAGE_CRON, { timeZone: HELP_AI_TIME_ZONE })
  async triagePendingContributions() {
    return this.batchRunLogger.track(
      'HELP_AI_TRIAGE_CONTRIBUTIONS',
      HELP_AI_QUEUE,
      async (ctx) => {
        if (this.triaging) {
          ctx.skip('a run is already active');
          this.logger.warn(
            'Contribution triage skipped because a run is active',
          );
          return;
        }

        this.triaging = true;
        try {
          const ran = await this.lock.runExclusive(
            'help-ai:triage',
            LOCK_TTL_TRIAGE,
            () => this.helpAiService.triagePendingContributions(),
          );
          if (!ran) ctx.skip('another instance holds the lock');
        } finally {
          this.triaging = false;
        }
      },
    );
  }

  @Cron(HELP_AI_API_KEY_THAW_CRON, { timeZone: HELP_AI_TIME_ZONE })
  async thawExpiredApiKeys() {
    return this.batchRunLogger.track(
      'HELP_AI_THAW_API_KEYS',
      HELP_AI_QUEUE,
      async (ctx) => {
        if (this.thawingApiKeys) {
          ctx.skip('a run is already active');
          this.logger.warn(
            'Gemini API key thaw skipped because a run is active',
          );
          return;
        }

        this.thawingApiKeys = true;
        try {
          const ran = await this.lock.runExclusive(
            'help-ai:thaw-api-keys',
            LOCK_TTL_THAW,
            () => this.geminiKeyManager.thawExpiredKeys(),
          );
          if (!ran) ctx.skip('another instance holds the lock');
        } finally {
          this.thawingApiKeys = false;
        }
      },
    );
  }
}
