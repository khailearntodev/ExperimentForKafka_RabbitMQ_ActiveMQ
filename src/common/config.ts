import dotenv from 'dotenv';
import { BrokerType } from './types';

dotenv.config();

export const config = {
  port: parseInt(process.env.PORT || '3000', 10),
  mqType: (process.env.MQ_TYPE || 'rabbitmq') as BrokerType,

  rabbitmq: {
    url: process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672',
    exchange: process.env.RABBITMQ_EXCHANGE || 'order.exchange',
    queue: process.env.RABBITMQ_QUEUE || 'payment.queue',
    routingKey: process.env.RABBITMQ_ROUTING_KEY || 'order.created',
  },

  activemq: {
    host: process.env.ACTIVEMQ_HOST || 'localhost',
    port: parseInt(process.env.ACTIVEMQ_PORT || '61613', 10),
    login: process.env.ACTIVEMQ_LOGIN || 'admin',
    passcode: process.env.ACTIVEMQ_PASSCODE || 'admin',
    queue: process.env.ACTIVEMQ_QUEUE || '/queue/payment.queue',
  },

  kafka: {
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    clientId: process.env.KAFKA_CLIENT_ID || 'techlab-mq-poc',
    topic: process.env.KAFKA_TOPIC || 'order.created',
    groupId: process.env.KAFKA_GROUP_ID || 'payment-worker-group',
  },

  payment: {
    delayMs: parseInt(process.env.PAYMENT_DELAY_MS || '0', 10),
    failureMode: process.env.PAYMENT_FAILURE_MODE || 'none', // 'none' | 'retry_test'
    maxRetries: parseInt(process.env.PAYMENT_MAX_RETRIES || '3', 10),
    workerPort: parseInt(process.env.WORKER_PORT || '3002', 10),
  },

  order: {
    dbInsertDelayMs: parseInt(process.env.DB_INSERT_DELAY_MS || '0', 10),
  },

  sync: {
    orderPort: parseInt(process.env.SYNC_ORDER_PORT || '3000', 10),
    paymentPort: parseInt(process.env.SYNC_PAYMENT_PORT || '3001', 10),
    paymentUrl: process.env.SYNC_PAYMENT_URL || 'http://localhost:3001/process-payment',
  },
};
