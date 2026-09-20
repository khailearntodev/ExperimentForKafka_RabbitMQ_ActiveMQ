import fastify from 'fastify';
import { BrokerFactory } from '../broker/broker.factory';
import { PaymentProcessor } from './payment.processor';
import { config } from '../common/config';
import { logger } from '../common/logger';
import { OrderCreatedEvent } from '../common/types';

async function bootstrap() {
  logger.info(`Starting Payment Worker (MQ: ${config.mqType}, Admin Port: ${config.payment.workerPort})...`);

  const consumer = BrokerFactory.createConsumer(config.mqType);
  const processor = new PaymentProcessor();

  await consumer.connect();

  const destination = BrokerFactory.getConsumeDestination(config.mqType);

  await consumer.subscribe(destination, async (event: OrderCreatedEvent) => {
    logger.debug(`Received OrderCreatedEvent: ${event.orderId}`);
    const result = await processor.processPayment(event);
    return result.success;
  });

  logger.info(`Payment Worker listening on destination: ${destination}`);

  // Lightweight HTTP Admin server for metrics & reset
  const adminApp = fastify({ logger: false });

  adminApp.get('/health', async () => {
    return { status: 'UP', worker: 'payment', broker: config.mqType };
  });

  adminApp.get('/metrics', async () => {
    return processor.getMetrics();
  });

  adminApp.addContentTypeParser('*', (_req, _payload, done) => done(null, null));

  adminApp.get('/reset', async () => {
    processor.reset();
    return { status: 'RESET_OK' };
  });

  adminApp.post('/reset', async () => {
    processor.reset();
    return { status: 'RESET_OK' };
  });

  try {
    await adminApp.listen({ port: config.payment.workerPort, host: '0.0.0.0' });
    logger.info(`Payment Worker Admin API running on port ${config.payment.workerPort}`);
  } catch (err) {
    logger.warn(`Could not start Payment Worker Admin API on port ${config.payment.workerPort}:`, err);
  }

  // Graceful shutdown
  const closeGracefully = async (signal: string) => {
    logger.info(`Received ${signal}. Shutting down Payment Worker...`);
    try {
      await adminApp.close();
    } catch {}
    await consumer.disconnect();
    process.exit(0);
  };

  process.on('SIGINT', () => closeGracefully('SIGINT'));
  process.on('SIGTERM', () => closeGracefully('SIGTERM'));
}

bootstrap().catch((err) => {
  logger.error('Failed to start Payment Worker:', err);
  process.exit(1);
});
