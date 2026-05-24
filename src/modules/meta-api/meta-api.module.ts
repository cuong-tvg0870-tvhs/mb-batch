import { Module, Global } from '@nestjs/common';
import { MetaApiService } from './meta-api.service';

@Global()
@Module({
  providers: [MetaApiService],
  exports: [MetaApiService],
})
export class MetaApiModule {}
