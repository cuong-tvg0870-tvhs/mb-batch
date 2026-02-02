import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { configLoads } from './config';
import { MetaModule } from './modules/meta/meta.module';
import { PrismaModule } from './modules/prisma/prisma.module';
import { TaskModule } from './task/task.module';

export const global_modules = [
  BullModule.forRoot({
    redis: {
      host: process.env.REDIS_HOST,
      port: Number(process.env.REDIS_PORT),
    },
  }),
  ConfigModule.forRoot({
    load: configLoads,
    isGlobal: true,
    envFilePath: ['.env'],
  }),
];

@Module({
  imports: [
    ...global_modules,
    ScheduleModule.forRoot(),
    TaskModule,
    PrismaModule,
    MetaModule,
  ],
})
export class AppModule {
  constructor() {}
}
