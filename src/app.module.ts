import { BullModule } from '@nestjs/bull';
import { Logger, Module, OnModuleInit } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { configLoads } from './config';
import { BatchRunLogModule } from './modules/batch-run-log/batch-run-log.module';
import { CampaignRuleRunnerModule } from './modules/campaign-rule-runner/campaign-rule-runner.module';
import { DistributedLockModule } from './modules/distributed-lock/distributed-lock.module';
import { DraftAutomationModule } from './modules/draft-automation/draft-automation.module';
import { EntitySyncModule } from './modules/entity-sync/entity-sync.module';
import { HealthModule } from './modules/health/health.module';
import { InsightSyncModule } from './modules/insight-sync/insight-sync.module';
import { HelpAiModule } from './modules/help-ai/help-ai.module';
import { LarkSyncModule } from './modules/lark-sync/lark-sync.module';
import { MediaSyncModule } from './modules/media-sync/media-sync.module';
import { MetaApiModule } from './modules/meta-api/meta-api.module';
import { MetaMediaSyncModule } from './modules/meta-media-sync/meta-media-sync.module';
import { MetaMediaUploadModule } from './modules/meta-media-upload/meta-media-upload.module';
import { MetaSyncModule } from './modules/meta-sync/meta-sync.module';
import { UserLarkSyncModule } from './modules/user-lark-sync/user-lark-sync.module';
import { AutoLaunchTriggerModule } from './modules/auto-launch-trigger/auto-launch-trigger.module';

export const global_modules = [
  ScheduleModule.forRoot(),
  ConfigModule.forRoot({
    load: configLoads,
    isGlobal: true,
    envFilePath: ['.env'],
  }),
  BullModule.forRootAsync({
    imports: [ConfigModule],
    useFactory: async (configService: ConfigService) => ({
      redis: {
        host: configService.get<string>('REDIS_HOST', 'localhost'),
        port: Number(configService.get<number>('REDIS_PORT', 6379)),
        maxRetriesPerRequest: null,
      },
    }),
    inject: [ConfigService],
  }),
];

@Module({
  imports: [
    ...global_modules,
    DistributedLockModule,
    HealthModule,
    BatchRunLogModule,
    AutoLaunchTriggerModule,
    MetaApiModule,
    InsightSyncModule,
    MetaSyncModule,
    EntitySyncModule,
    LarkSyncModule,
    MediaSyncModule,
    DraftAutomationModule,
    MetaMediaSyncModule,
    MetaMediaUploadModule,
    HelpAiModule,
    UserLarkSyncModule,
    CampaignRuleRunnerModule,
  ],
})
export class AppModule implements OnModuleInit {
  private readonly logger = new Logger(AppModule.name);

  constructor(private readonly configService: ConfigService) {}

  onModuleInit() {
    const host = this.configService.get('REDIS_HOST', 'localhost');
    const port = this.configService.get('REDIS_PORT', 6379);
    this.logger.log(`[Redis] Attempting to use Redis at ${host}:${port}`);
    this.logger.log('🚀 mb-batch application initialized and modules loaded');
  }
}
