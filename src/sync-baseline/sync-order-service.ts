import fastify from 'fastify';
import { config } from '../common/config';
import { logger } from '../common/logger';
import { OrderRequest } from '../common/types';

async function bootstrap() {
  const app = fastify({ logger: false });

  app.get('/health', async () => {
    return { status: 'UP', broker: 'synchronous-http' };
  });

  app.post<{ Body: OrderRequest }>('/orders', async (request, reply) => {
    const { orderId, userId, amount } = request.body;

    logger.info(`[SyncOrderService] Received orderId=${orderId}. Synchronously calling Payment Service...`);

    try {
      // Synchronous HTTP call to downstream Payment Service
      const response = await fetch(config.sync.paymentUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ orderId, amount, userId }),
      });

      if (!response.ok) {
        reply.status(502);
        return { error: 'Payment failed synchronously' };
      }

      const paymentResult = await response.json();
      return {
        orderId,
        status: 'COMPLETED',
        payment: paymentResult,
      };
    } catch (err: any) {
      logger.error(`[SyncOrderService] Error calling payment service:`, err.message);
      reply.status(504);
      return { error: 'Payment service timed out or unavailable' };
    }
  });

  try {
    await app.listen({ port: config.sync.orderPort, host: '0.0.0.0' });
    logger.info(`Sync Order Service running on port ${config.sync.orderPort} (Blocking downstream call)`);
  } catch (err) {
    logger.error('Failed to start Sync Order Service:', err);
    process.exit(1);
  }
}

bootstrap();
