import { IMessagePublisher, IMessageConsumer } from './broker.interface';
import { RabbitMqPublisher, RabbitMqConsumer } from './rabbitmq.broker';
import { ActiveMqPublisher, ActiveMqConsumer } from './activemq.broker';
import { KafkaPublisher, KafkaConsumer } from './kafka.broker';
import { config } from '../common/config';
import { BrokerType } from '../common/types';
import { logger } from '../common/logger';

export class BrokerFactory {
  static createPublisher(type: BrokerType = config.mqType): IMessagePublisher {
    logger.info(`Creating Publisher for MQ_TYPE=${type}`);
    switch (type) {
      case 'rabbitmq':
        return new RabbitMqPublisher();
      case 'activemq':
        return new ActiveMqPublisher();
      case 'kafka':
        return new KafkaPublisher();
      default:
        throw new Error(`Unsupported broker type: ${type}`);
    }
  }

  static createConsumer(type: BrokerType = config.mqType): IMessageConsumer {
    logger.info(`Creating Consumer for MQ_TYPE=${type}`);
    switch (type) {
      case 'rabbitmq':
        return new RabbitMqConsumer();
      case 'activemq':
        return new ActiveMqConsumer();
      case 'kafka':
        return new KafkaConsumer();
      default:
        throw new Error(`Unsupported broker type: ${type}`);
    }
  }

  static getPublishDestination(type: BrokerType = config.mqType): string {
    switch (type) {
      case 'rabbitmq':
        return config.rabbitmq.routingKey;
      case 'activemq':
        return config.activemq.queue;
      case 'kafka':
        return config.kafka.topic;
      default:
        throw new Error(`Unsupported broker type: ${type}`);
    }
  }

  static getConsumeDestination(type: BrokerType = config.mqType): string {
    switch (type) {
      case 'rabbitmq':
        return config.rabbitmq.queue;
      case 'activemq':
        return config.activemq.queue;
      case 'kafka':
        return config.kafka.topic;
      default:
        throw new Error(`Unsupported broker type: ${type}`);
    }
  }

  // Backward compatibility
  static getDestination(type: BrokerType = config.mqType): string {
    return this.getPublishDestination(type);
  }
}
