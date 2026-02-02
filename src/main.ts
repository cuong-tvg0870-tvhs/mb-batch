import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  // Admin Module Setup
  const app = await NestFactory.create(AppModule);
  console.log('===== Application running =====');
}
bootstrap();
