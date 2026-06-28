import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { BatchRunLoggerService } from '../batch-run-log/batch-run-logger.service';
import {
  HELP_AI_API_KEY_THAW_CRON,
  HELP_AI_KNOWLEDGE_REFRESH_CRON,
  HELP_AI_TIME_ZONE,
  HELP_AI_TRIAGE_CRON,
} from './help-ai.constants';
import { GeminiApiKeyManager } from './gemini-api-key-manager.service';
import { HelpAiService } from './help-ai.service';

const HELP_AI_QUEUE = 'help-ai';

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
          await this.helpAiService.refreshHelpKnowledge();
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
          await this.helpAiService.triagePendingContributions();
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
          await this.geminiKeyManager.thawExpiredKeys();
        } finally {
          this.thawingApiKeys = false;
        }
      },
    );
  }
}
