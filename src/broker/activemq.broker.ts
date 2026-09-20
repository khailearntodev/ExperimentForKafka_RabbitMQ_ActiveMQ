import stompit from 'stompit';
import { IMessagePublisher, IMessageConsumer, MessageHandler } from './broker.interface';
import { OrderCreatedEvent } from '../common/types';
import { config } from '../common/config';
import { logger } from '../common/logger';

export class ActiveMqPublisher implements IMessagePublisher {
  private client: stompit.Client | null = null;

  async connect(): Promise<void> {
    logger.info(`Connecting to ActiveMQ STOMP at ${config.activemq.host}:${config.activemq.port}...`);
    return new Promise((resolve, reject) => {
      const connectOptions = {
        host: config.activemq.host,
        port: config.activemq.port,
        connectHeaders: {
          host: '/',
          login: config.activemq.login,
          passcode: config.activemq.passcode,
        },
      };

      stompit.connect(connectOptions, (error, client) => {
        if (error) {
          logger.error('ActiveMQ connection error:', error);
          return reject(error);
        }
        this.client = client;
        logger.info('ActiveMQ Publisher connected.');
        resolve();
      });
    });
  }

  async publish(destination: string, event: OrderCreatedEvent): Promise<void> {
    if (!this.client) {
      throw new Error('ActiveMQ client not initialized');
    }
    const sendHeaders = {
      destination: destination.startsWith('/queue/') ? destination : `/queue/${destination}`,
      'content-type': 'application/json',
      persistent: 'true',
    };

    const frame = this.client.send(sendHeaders);
    frame.write(JSON.stringify(event));
    frame.end();
  }

  async disconnect(): Promise<void> {
    if (this.client) {
      this.client.disconnect();
      logger.info('ActiveMQ Publisher disconnected.');
    }
  }
}

export class ActiveMqConsumer implements IMessageConsumer {
  private client: stompit.Client | null = null;

  async connect(): Promise<void> {
    logger.info(`Connecting to ActiveMQ STOMP Consumer at ${config.activemq.host}:${config.activemq.port}...`);
    return new Promise((resolve, reject) => {
      const connectOptions = {
        host: config.activemq.host,
        port: config.activemq.port,
        connectHeaders: {
          host: '/',
          login: config.activemq.login,
          passcode: config.activemq.passcode,
        },
      };

      stompit.connect(connectOptions, (error, client) => {
        if (error) {
          logger.error('ActiveMQ Consumer connection error:', error);
          return reject(error);
        }
        this.client = client;
        logger.info('ActiveMQ Consumer connected.');
        resolve();
      });
    });
  }

  async subscribe(destination: string, handler: MessageHandler): Promise<void> {
    if (!this.client) {
      throw new Error('ActiveMQ client not initialized');
    }
    const subscribeHeaders = {
      destination: destination.startsWith('/queue/') ? destination : `/queue/${destination}`,
      ack: 'client-individual',
    };

    this.client.subscribe(subscribeHeaders, (error, message) => {
      if (error) {
        logger.error('ActiveMQ subscribe error:', error);
        return;
      }

      message.readString('utf-8', async (readError, body) => {
        if (readError || !body) {
          logger.error('ActiveMQ read message error:', readError);
          return;
        }

        try {
          const event: OrderCreatedEvent = JSON.parse(body);
          let success = await handler(event);
          let retries = 0;
          while (!success && retries < config.payment.maxRetries) {
            retries++;
            logger.warn(`[ActiveMqConsumer] Retrying order ${event.orderId} (attempt ${retries + 1})...`);
            await new Promise((res) => setTimeout(res, 50));
            success = await handler(event);
          }
          if (success) {
            this.client?.ack(message);
          } else {
            this.client?.nack(message);
          }
        } catch (err) {
          logger.error('Error handling ActiveMQ message body:', err);
          this.client?.nack(message);
        }
      });
    });
  }

  async disconnect(): Promise<void> {
    if (this.client) {
      this.client.disconnect();
      logger.info('ActiveMQ Consumer disconnected.');
    }
  }
}
