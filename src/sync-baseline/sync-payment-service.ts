import fastify from 'fastify';
import { config } from '../common/config';
import { logger } from '../common/logger';

async function bootstrap() {
  const app = fastify({ logger: false });

  app.get('/health', async () => {
    return { status: 'UP', service: 'sync-payment' };
  });

  app.post('/process-payment', async (request, reply) => {
    logger.info(`[SyncPaymentService] Received payment request. Simulating ${config.payment.delayMs}ms delay...`);
    
    // Simulate payment latency (e.g. 3000ms)
    if (config.payment.delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, config.payment.delayMs));
    }

    return { status: 'PAID' };
  });

  try {
    await app.listen({ port: config.sync.paymentPort, host: '0.0.0.0' });
    logger.info(`Sync Payment Service running on port ${config.sync.paymentPort} (Delay: ${config.payment.delayMs}ms)`);
  } catch (err) {
    logger.error('Failed to start Sync Payment Service:', err);
    process.exit(1);
  }
}

bootstrap();
