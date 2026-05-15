import { BullModule } from '@nestjs/bull';
import { Logger, Module, OnModuleInit } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { configLoads } from './config';
import { InsightSyncModule } from './modules/insight-sync/insight-sync.module';

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
    InsightSyncModule,
    MetaSyncModule,
    LarkSyncModule,
    // MediaSyncModule,
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
