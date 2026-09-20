import { OrderCreatedEvent, PaymentProcessingResult, WorkerMetrics } from '../common/types';
import { config } from '../common/config';
import { logger } from '../common/logger';

export class PaymentProcessor {
  // Map to track retry attempts per orderId
  private attemptTracker = new Map<string, number>();
  private completedOrders = new Set<string>();
  private permanentlyFailedOrders = new Set<string>();

  private totalReceivedCount = 0;
  private totalRetryCount = 0;
  private firstMessageTime: number | null = null;
  private lastMessageTime: number | null = null;

  async processPayment(event: OrderCreatedEvent): Promise<PaymentProcessingResult> {
    const startTime = Date.now();
    if (!this.firstMessageTime) {
      this.firstMessageTime = startTime;
    }
    this.lastMessageTime = startTime;

    this.totalReceivedCount++;
    const currentAttempt = (this.attemptTracker.get(event.orderId) || 0) + 1;
    this.attemptTracker.set(event.orderId, currentAttempt);

    if (currentAttempt > 1) {
      this.totalRetryCount++;
    }

    logger.info(
      `[PaymentWorker] [RECEIVED] orderId=${event.orderId} (Attempt: ${currentAttempt}, Delay: ${config.payment.delayMs}ms, FailureMode: ${config.payment.failureMode})`
    );

    // Simulate configurable processing delay (0ms in Test 1, 3000ms in Test 2)
    if (config.payment.delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, config.payment.delayMs));
    }

    // Controlled failure mode (Test 3: Fail on attempts 1 and 2, succeed on attempt 3)
    if (config.payment.failureMode === 'retry_test') {
      if (currentAttempt < 3) {
        logger.warn(
          `[PaymentWorker] [FAILED] Controlled retry simulation for orderId=${event.orderId} on attempt ${currentAttempt}. Will trigger retry.`
        );
        return {
          orderId: event.orderId,
          success: false,
          attempts: currentAttempt,
          durationMs: Date.now() - startTime,
          error: `Failed on attempt ${currentAttempt} (Controlled Failure Mode)`,
        };
      }
    }

    // Successfully completed
    this.completedOrders.add(event.orderId);
    this.lastMessageTime = Date.now();

    logger.info(
      `[PaymentWorker] [SUCCESS] Payment processed for orderId=${event.orderId} on attempt ${currentAttempt} (Duration: ${Date.now() - startTime
      }ms)`
    );

    return {
      orderId: event.orderId,
      success: true,
      attempts: currentAttempt,
      durationMs: Date.now() - startTime,
    };
  }

  getMetrics(): WorkerMetrics {
    const totalOrders = this.attemptTracker.size;
    const successful = this.completedOrders.size;
    const failed = this.permanentlyFailedOrders.size;
    const totalDuration = (this.firstMessageTime && this.lastMessageTime)
      ? Math.max(0, this.lastMessageTime - this.firstMessageTime)
      : 0;
    const successRate = totalOrders > 0 ? (successful / totalOrders) * 100 : 0;

    return {
      totalReceived: this.totalReceivedCount,
      successfulPayments: successful,
      failedPayments: failed,
      retryCount: this.totalRetryCount,
      lostMessages: Math.max(0, totalOrders - successful - failed),
      totalProcessingTimeMs: totalDuration,
      successRate: Math.round(successRate * 100) / 100,
    };
  }

  reset(): void {
    this.attemptTracker.clear();
    this.completedOrders.clear();
    this.permanentlyFailedOrders.clear();
    this.totalReceivedCount = 0;
    this.totalRetryCount = 0;
    this.firstMessageTime = null;
    this.lastMessageTime = null;
    logger.info('[PaymentWorker] Metrics reset for new benchmark run.');
  }
}
