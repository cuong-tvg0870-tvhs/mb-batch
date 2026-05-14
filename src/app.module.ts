import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { configLoads } from './config';
import { InsightSyncModule } from './modules/insight-sync/insight-sync.module';
import { LarkSyncModule } from './modules/lark-sync/lark-sync.module';
import { MediaSyncModule } from './modules/media-sync/media-sync.module';
import { MetaSyncModule } from './modules/meta-sync/meta-sync.module';

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
    MediaSyncModule,
  ],
})
export class AppModule {
  constructor() {}
}
