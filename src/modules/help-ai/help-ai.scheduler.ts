import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import {
  HELP_AI_KNOWLEDGE_REFRESH_CRON,
  HELP_AI_TIME_ZONE,
  HELP_AI_TRIAGE_CRON,
} from './help-ai.constants';
import { HelpAiService } from './help-ai.service';

@Injectable()
export class HelpAiScheduler implements OnModuleInit {
  private readonly logger = new Logger(HelpAiScheduler.name);
  private refreshingKnowledge = false;
  private triaging = false;

  constructor(private readonly helpAiService: HelpAiService) {}

  async onModuleInit() {
    this.logger.log('HelpAiScheduler initialized');
    await this.refreshHelpKnowledge();
  }

  @Cron(HELP_AI_KNOWLEDGE_REFRESH_CRON, { timeZone: HELP_AI_TIME_ZONE })
  async refreshHelpKnowledge() {
    if (this.refreshingKnowledge) {
      this.logger.warn('Help knowledge refresh skipped because a run is active');
      return;
    }

    this.refreshingKnowledge = true;
    try {
      await this.helpAiService.refreshHelpKnowledge();
    } finally {
      this.refreshingKnowledge = false;
    }
  }

  @Cron(HELP_AI_TRIAGE_CRON, { timeZone: HELP_AI_TIME_ZONE })
  async triagePendingContributions() {
    if (this.triaging) {
      this.logger.warn('Contribution triage skipped because a run is active');
      return;
    }

    this.triaging = true;
    try {
      await this.helpAiService.triagePendingContributions();
    } finally {
      this.triaging = false;
    }
  }
}
