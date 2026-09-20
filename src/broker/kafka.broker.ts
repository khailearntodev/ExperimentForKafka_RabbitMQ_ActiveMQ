import { Kafka, Producer, Consumer } from 'kafkajs';
import { IMessagePublisher, IMessageConsumer, MessageHandler } from './broker.interface';
import { OrderCreatedEvent } from '../common/types';
import { config } from '../common/config';
import { logger } from '../common/logger';

export class KafkaPublisher implements IMessagePublisher {
  private kafka: Kafka;
  private producer: Producer;

  constructor() {
    this.kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
    });
    this.producer = this.kafka.producer();
  }

  async connect(): Promise<void> {
    logger.info(`Connecting Kafka Producer to ${config.kafka.brokers.join(',')}...`);
    await this.producer.connect();
    logger.info('Kafka Producer connected.');
  }

  async publish(topic: string, event: OrderCreatedEvent): Promise<void> {
    await this.producer.send({
      topic,
      messages: [
        {
          key: event.orderId,
          value: JSON.stringify(event),
        },
      ],
    });
  }

  async disconnect(): Promise<void> {
    await this.producer.disconnect();
    logger.info('Kafka Producer disconnected.');
  }
}

export class KafkaConsumer implements IMessageConsumer {
  private kafka: Kafka;
  private consumer: Consumer;

  constructor() {
    this.kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
    });
    this.consumer = this.kafka.consumer({ groupId: config.kafka.groupId });
  }

  async connect(): Promise<void> {
    logger.info(`Connecting Kafka Consumer to ${config.kafka.brokers.join(',')}...`);
    await this.consumer.connect();
    logger.info('Kafka Consumer connected.');
  }

  async subscribe(topic: string, handler: MessageHandler): Promise<void> {
    await this.consumer.subscribe({ topic, fromBeginning: true });
    logger.info(`Subscribed to Kafka topic: ${topic}`);

    await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) return;
        try {
          const event: OrderCreatedEvent = JSON.parse(message.value.toString());
          let success = await handler(event);
          let retries = 0;
          while (!success && retries < config.payment.maxRetries) {
            retries++;
            logger.warn(`[KafkaConsumer] Retrying order ${event.orderId} (attempt ${retries + 1})...`);
            await new Promise((res) => setTimeout(res, 50));
            success = await handler(event);
          }
          if (success) {
            await this.consumer.commitOffsets([
              {
                topic,
                partition,
                offset: (BigInt(message.offset) + 1n).toString(),
              },
            ]);
          }
        } catch (err) {
          logger.error('Error handling Kafka message:', err);
        }
      },
    });
  }

  async disconnect(): Promise<void> {
    await this.consumer.disconnect();
    logger.info('Kafka Consumer disconnected.');
  }
}
