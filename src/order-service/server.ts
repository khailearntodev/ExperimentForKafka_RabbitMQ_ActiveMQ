import fastify from 'fastify';
import { BrokerFactory } from '../broker/broker.factory';
import { OrderController } from './order.controller';
import { config } from '../common/config';
import { logger } from '../common/logger';
import { OrderRequest } from '../common/types';

async function bootstrap() {
  const app = fastify({ logger: false });

  // Initialize Message Broker Publisher
  const publisher = BrokerFactory.createPublisher(config.mqType);
  await publisher.connect();

  const orderController = new OrderController(publisher);

  app.get('/health', async () => {
    return { status: 'UP', broker: config.mqType };
  });

  app.post<{ Body: OrderRequest }>('/orders', async (request, reply) => {
    return orderController.createOrder(request, reply);
  });

  // Graceful shutdown
  const closeGracefully = async (signal: string) => {
    logger.info(`Received ${signal}. Shutting down Order Service...`);
    await app.close();
    await publisher.disconnect();
    process.exit(0);
  };

  process.on('SIGINT', () => closeGracefully('SIGINT'));
  process.on('SIGTERM', () => closeGracefully('SIGTERM'));

  try {
    await app.listen({ port: config.port, host: '0.0.0.0' });
    logger.info(`Order Service running on port ${config.port} (MQ: ${config.mqType})`);
  } catch (err) {
    logger.error('Failed to start Order Service:', err);
    process.exit(1);
  }
}

bootstrap();
