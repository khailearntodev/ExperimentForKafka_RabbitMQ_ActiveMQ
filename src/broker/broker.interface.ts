import { OrderCreatedEvent } from '../common/types';

export interface IMessagePublisher {
  connect(): Promise<void>;
  publish(topicOrQueue: string, event: OrderCreatedEvent): Promise<void>;
  disconnect(): Promise<void>;
}

export type MessageHandler = (event: OrderCreatedEvent) => Promise<boolean>;

export interface IMessageConsumer {
  connect(): Promise<void>;
  subscribe(topicOrQueue: string, handler: MessageHandler): Promise<void>;
  disconnect(): Promise<void>;
}
