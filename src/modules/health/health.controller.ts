import { Controller, Get } from '@nestjs/common';

/**
 * Endpoint liveness cho Docker HEALTHCHECK / orchestrator. mb-batch là worker thuần
 * (không phục vụ traffic) nên chỉ cần xác nhận process còn sống và HTTP server phản hồi.
 */
@Controller('health')
export class HealthController {
  @Get()
  check() {
    return { status: 'ok', service: 'mb-batch', ts: new Date().toISOString() };
  }
}
