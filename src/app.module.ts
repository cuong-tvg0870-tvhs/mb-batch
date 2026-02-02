import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { configLoads } from './config';
import { TaskModule } from './task/task.module';

export const global_modules = [
  ScheduleModule.forRoot(),
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
  imports: [...global_modules, TaskModule],
})
export class AppModule {
  constructor() {}
}
