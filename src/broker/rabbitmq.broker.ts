import amqp, { ChannelModel, Channel } from 'amqplib';
import { IMessagePublisher, IMessageConsumer, MessageHandler } from './broker.interface';
import { OrderCreatedEvent } from '../common/types';
import { config } from '../common/config';
import { logger } from '../common/logger';

export class RabbitMqPublisher implements IMessagePublisher {
  private connection: ChannelModel | null = null;
  private channel: Channel | null = null;

  async connect(): Promise<void> {
    logger.info(`Connecting to RabbitMQ at ${config.rabbitmq.url}...`);
    this.connection = await amqp.connect(config.rabbitmq.url);
    this.channel = await this.connection.createChannel();
    await this.channel.assertExchange(config.rabbitmq.exchange, 'direct', { durable: true });
    logger.info('RabbitMQ Publisher connected and exchange asserted.');
  }

  async publish(routingKey: string, event: OrderCreatedEvent): Promise<void> {
    if (!this.channel) {
      throw new Error('RabbitMQ channel not initialized');
    }
    const messageBuffer = Buffer.from(JSON.stringify(event));
    this.channel.publish(config.rabbitmq.exchange, routingKey, messageBuffer, {
      persistent: true,
      contentType: 'application/json',
    });
  }

  async disconnect(): Promise<void> {
    if (this.channel) await this.channel.close();
    if (this.connection) await this.connection.close();
    logger.info('RabbitMQ Publisher disconnected.');
  }
}

export class RabbitMqConsumer implements IMessageConsumer {
  private connection: ChannelModel | null = null;
  private channel: Channel | null = null;

  async connect(): Promise<void> {
    logger.info(`Connecting to RabbitMQ Consumer at ${config.rabbitmq.url}...`);
    this.connection = await amqp.connect(config.rabbitmq.url);
    this.channel = await this.connection.createChannel();
    await this.channel.assertExchange(config.rabbitmq.exchange, 'direct', { durable: true });
    await this.channel.assertQueue(config.rabbitmq.queue, { durable: true });
    await this.channel.bindQueue(config.rabbitmq.queue, config.rabbitmq.exchange, config.rabbitmq.routingKey);
    await this.channel.prefetch(10);
    logger.info('RabbitMQ Consumer connected and queue bound.');
  }

  async subscribe(queue: string, handler: MessageHandler): Promise<void> {
    if (!this.channel) {
      throw new Error('RabbitMQ channel not initialized');
    }
    const targetQueue = queue || config.rabbitmq.queue;
    logger.info(`Subscribing to RabbitMQ queue: ${targetQueue}`);
    await this.channel.consume(
      targetQueue,
      async (msg) => {
        if (!msg) return;
        try {
          const event: OrderCreatedEvent = JSON.parse(msg.content.toString());
          const success = await handler(event);
          if (success) {
            this.channel?.ack(msg);
          } else {
            // Requeue message on failure for retry
            this.channel?.nack(msg, false, true);
          }
        } catch (err) {
          logger.error('Error processing RabbitMQ message:', err);
          this.channel?.nack(msg, false, true);
        }
      },
      { noAck: false }
    );
  }

  async disconnect(): Promise<void> {
    if (this.channel) await this.channel.close();
    if (this.connection) await this.connection.close();
    logger.info('RabbitMQ Consumer disconnected.');
  }
}
