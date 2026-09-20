export type BrokerType = 'rabbitmq' | 'activemq' | 'kafka';

export interface OrderRequest {
  orderId: string;
  userId: string;
  amount: number;
}

export interface OrderResponse {
  orderId: string;
  status: 'PROCESSING';
}

export interface OrderCreatedEvent {
  eventId: string;
  orderId: string;
  userId: string;
  amount: number;
  eventType: 'ORDER_CREATED';
  timestamp: string;
  attempt?: number;
}

export interface PaymentProcessingResult {
  orderId: string;
  success: boolean;
  attempts: number;
  durationMs: number;
  error?: string;
}

export interface WorkerMetrics {
  totalReceived: number;
  successfulPayments: number;
  failedPayments: number;
  retryCount: number;
  lostMessages: number;
  totalProcessingTimeMs: number;
  successRate: number;
}

