import { FastifyRequest, FastifyReply } from 'fastify';
import { v4 as uuidv4 } from 'uuid';
import { OrderRequest, OrderResponse, OrderCreatedEvent } from '../common/types';
import { IMessagePublisher } from '../broker/broker.interface';
import { BrokerFactory } from '../broker/broker.factory';
import { config } from '../common/config';

export class OrderController {
  private publisher: IMessagePublisher;
  private destination: string;

  constructor(publisher: IMessagePublisher) {
    this.publisher = publisher;
    this.destination = BrokerFactory.getPublishDestination(config.mqType);
  }

  async createOrder(
    request: FastifyRequest<{ Body: OrderRequest }>,
    reply: FastifyReply
  ): Promise<OrderResponse> {
    const { orderId, userId, amount } = request.body;

    const event: OrderCreatedEvent = {
      eventId: uuidv4(),
      orderId: orderId || `ORD-${Date.now()}`,
      userId: userId || 'USER-DEFAULT',
      amount: amount || 100000,
      eventType: 'ORDER_CREATED',
      timestamp: new Date().toISOString(),
      attempt: 1,
    };

    // Simulate DB persistence (Order Service persists order data to database)
    if (config.order.dbInsertDelayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, config.order.dbInsertDelayMs));
    }

    // Publish to message broker asynchronously without waiting for downstream payment
    await this.publisher.publish(this.destination, event);

    // Return HTTP 202 Accepted
    reply.status(202);
    return {
      orderId: event.orderId,
      status: 'PROCESSING',
    };
  }
}
