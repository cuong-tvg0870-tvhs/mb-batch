import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  // Admin Module Setup
  const app = await NestFactory.create(AppModule);
  await app.listen(3030);
  console.log('===== Worker running on 5000 =====');
}
bootstrap();
