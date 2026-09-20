# Kế Hoạch Kiến Trúc Hệ Thống & Khung Package: Message Queue PoC & Benchmark (RabbitMQ vs ActiveMQ vs Kafka)

Tài liệu này đề xuất kiến trúc hệ thống, lựa chọn công nghệ, cấu trúc thư mục/package và lộ trình triển khai chi tiết cho thí nghiệm so sánh 3 Message Broker (**RabbitMQ**, **ActiveMQ**, **Kafka**) phục vụ phỏng vấn Techlab Dev 2026, dựa trên tài liệu yêu cầu tại [Techlab_MQ_Experiment_Code_Agent_Instructions.txt](file:///d:/work/techlab/Experiment/requirements/Techlab_MQ_Experiment_Code_Agent_Instructions.txt).

---

## 1. User Review Required

> [!IMPORTANT]
> **Lựa chọn Tech Stack**:
> - **Runtime & Ngôn ngữ**: Đề xuất **Node.js (v22.19.0 LTS)** + **TypeScript**. (Node.js và Docker đã có sẵn trên máy người dùng; Go chưa được cài đặt).
> - **Web Framework**: **Fastify** thay vì Express (Fastify có throughput cực cao, độ trễ cực thấp, phù hợp nhất cho bài toán benchmark tải cao p95 latency).
> - **Thư viện Message Broker**:
>   - RabbitMQ: `amqplib` (AMQP protocol chuẩn)
>   - ActiveMQ: `stompit` (STOMP protocol qua port 61613 của ActiveMQ Classic)
>   - Kafka: `kafkajs` (Thuần JS, không phụ thuộc C++ build tool)
> - **Broker Containers**:
>   - `rabbitmq:3.13-management-alpine`
>   - `apache/activemq-classic:5.18.3`
>   - `apache/kafka:3.7.0` (chạy chế độ **KRaft** - không cần ZooKeeper, khởi động cực nhanh và nhẹ)
> - **Công cụ Load Test**: **k6** (chạy trực tiếp qua Docker container `grafana/k6:latest` để không cần cài đặt binary k6 vào OS) kết hợp script automation PowerShell.

---

## 2. Mục Tiêu & Phạm Vi Thí Nghiệm (Experiment Scope)

Hệ thống được thiết kế theo đúng tinh thần của bản yêu cầu:
1. **Không xây dựng hệ thống e-commerce hoàn chỉnh** (bỏ qua Inventory, Notification, Analytics, DB phức tạp, Kubernetes).
2. **Tập trung trả lời 3 câu hỏi cốt lõi**:
   - Giao tiếp bất đồng bộ qua Message Queue có giải quyết triệt để vấn đề Order Service bị nghẽn do downstream (Payment) chậm/lỗi hay không?
   - 3 Message Broker (RabbitMQ, ActiveMQ, Kafka) ứng xử ra sao dưới cùng một khối lượng tải (workload)?
   - Giải pháp nào là phù hợp nhất với đặc thù và tốc độ tăng trưởng của hệ thống (50k DAU -> 3-5x trong 12-18 tháng)?
3. **Quy tắc công bằng tuyệt đối (Fairness Rules)**:
   - Dùng chung 1 codebase cho Order Service và Payment Worker.
   - Dùng chung 1 cấu trúc payload JSON (`OrderCreated`).
   - Dùng chung cấu hình tải (k6 scripts, 1.000 orders, 50 concurrency).
   - Tách biệt từng Docker Compose file cho từng broker để dọn dẹp sạch sẽ tài nguyên/queue trước mỗi lượt test (3 runs mỗi kịch bản).

---

## 3. Kiến Trúc Hệ Thống (System Architecture)

### 3.1 Sơ Đồ Kiến Trúc Tổng Thể

```
+-------------------------------------------------------------+
|                      Load Generator                         |
|           (k6 Runner / Docker `grafana/k6`)                 |
+------------------------------+------------------------------+
                               |
                               | POST /orders (JSON)
                               v
+-------------------------------------------------------------+
|                       Order Service                         |
|     (Fastify + TypeScript - Port 3000)                      |
|  - Nhận OrderRequest                                        |
|  - Đóng gói sự kiện OrderCreatedEvent                       |
|  - Gửi tới Broker thông qua IMessagePublisher               |
|  - Trả về ngay HTTP 202 Accepted {"status": "PROCESSING"}   |
+------------------------------+------------------------------+
                               |
                               | publish(topic/queue, event)
                               v
+-------------------------------------------------------------+
|                  Common Broker Abstraction                  |
|                   (IMessagePublisher)                       |
|       +----------------------+----------------------+       |
|       |                      |                      |       |
|  RabbitMqPublisher    ActiveMqPublisher     KafkaPublisher  |
+-------+----------------------+----------------------+-------+
        |                      |                      |
        v                      v                      v
+---------------+      +---------------+      +---------------+
|   RabbitMQ    |      |   ActiveMQ    |      |     Kafka     |
| (Ex: order.ex)|      | (Queue:       |      | (Topic:       |
| (Q: payment.q)|      |  payment.q)   |      |  order.created|
+-------+-------+      +-------+-------+      +-------+-------+
        |                      |                      |
        |                      v                      |
+-------+----------------------+----------------------+-------+
|                  Common Broker Abstraction                  |
|                   (IMessageConsumer)                        |
|       +----------------------+----------------------+       |
|       |                      |                      |       |
|  RabbitMqConsumer     ActiveMqConsumer      KafkaConsumer   |
+-------+----------------------+----------------------+-------+
                               |
                               | consume(event)
                               v
+-------------------------------------------------------------+
|                      Payment Worker                         |
|  - Lắng nghe message OrderCreatedEvent                      |
|  - Gọi Mock Payment Logic                                   |
|  - Xử lý Delay (PAYMENT_DELAY_MS = 0ms / 3000ms)            |
|  - Xử lý Failure & Retry (thất bại 2 lần, thành công lần 3) |
|  - Ghi nhận Metrics (received, duration, retry count)       |
+-------------------------------------------------------------+
```

### 3.2 Chuẩn Hóa Event Message Payload (Section 5.2)

Toàn bộ 3 broker sẽ sử dụng duy nhất cấu trúc JSON này:
```json
{
  "eventId": "c7a8b38a-36b1-4f11-8be9-e64e1c2e6f42",
  "orderId": "ORD-2026-0001",
  "userId": "USER-1002",
  "amount": 100000,
  "eventType": "ORDER_CREATED",
  "timestamp": "2026-09-20T07:48:30.123Z"
}
```

### 3.3 Broker Abstraction Layer (Section 6)

Xây dựng interface thống nhất để tách biệt hoàn toàn business logic khỏi client library của từng broker:
- `IMessagePublisher`:
  - `connect(): Promise<void>`
  - `publish(topicOrQueue: string, event: OrderCreatedEvent): Promise<void>`
  - `disconnect(): Promise<void>`
- `IMessageConsumer`:
  - `connect(): Promise<void>`
  - `subscribe(topicOrQueue: string, handler: (event: OrderCreatedEvent) => Promise<boolean>): Promise<void>`
  - `disconnect(): Promise<void>`

Được quản lý thông qua **`BrokerFactory`** dựa trên biến môi trường:
`MQ_TYPE=rabbitmq | activemq | kafka`

---

## 4. Ba Kịch Bản Thí Nghiệm (Test Scenarios & Metrics)

| Kịch Bản | Mục Đích | Tham Số Tải | Tham Số Payment Worker | Chỉ Số Cần Thu Thập |
|---|---|---|---|---|
| **Test 1: Normal Workload** | Đo baseline hiệu năng khi downstream hoạt động bình thường | 1.000 orders, 50 concurrency | `PAYMENT_DELAY_MS=0`<br>`FAILURE_MODE=none` | Avg Latency, P95 Latency, Throughput (req/s), Error Rate |
| **Test 2: Slow Payment** | Kiểm chứng Order Service không bị block khi downstream bị chậm trễ | 1.000 orders, 50 concurrency | `PAYMENT_DELAY_MS=3000`<br>`FAILURE_MODE=none` | Avg Latency, P95 Latency, Throughput (req/s), Timeout Rate *(so sánh đối chiếu với Sync HTTP)* |
| **Test 3: Retry & Failures** | Đánh giá cơ chế xử lý lỗi và retry công bằng giữa các broker | 100 orders, 10 concurrency | `PAYMENT_DELAY_MS=0`<br>`FAILURE_MODE=fail_twice_then_succeed` | Tỷ lệ thành công (Success Rate), Retry Count, Lost Messages, Tổng thời gian xử lý |

---

## 5. Cấu Trúc Package & Thư Mục Đề Xuất (Package Architecture)

Dự án sẽ được tổ chức theo cấu trúc module rõ ràng, phân tách rành mạch giữa hạ tầng, mã nguồn dịch vụ, kịch bản benchmark và nơi lưu kết quả:

```
Experiment/
├── requirements/
│   ├── Techlab_MQ_Experiment_Code_Agent_Instructions.txt  # File đề bài gốc
│   └── SYSTEM_ARCHITECTURE_AND_PLAN.md                    # File tài liệu kiến trúc & hướng dẫn chi tiết [NEW]
├── docker/
│   ├── docker-compose.rabbitmq.yml                        # Docker setup cho RabbitMQ + Management UI [NEW]
│   ├── docker-compose.activemq.yml                        # Docker setup cho ActiveMQ Classic [NEW]
│   ├── docker-compose.kafka.yml                           # Docker setup cho Kafka (KRaft mode) [NEW]
│   └── docker-compose.sync.yml                            # Setup cho kịch bản đối chứng Synchronous HTTP [NEW]
├── src/
│   ├── common/                                            # Core domain, types, interfaces & logger [NEW]
│   │   ├── types.ts                                       # Định nghĩa OrderRequest, OrderCreatedEvent, ...
│   │   ├── config.ts                                      # Load env variables tập trung
│   │   └── logger.ts                                      # Format log chuẩn cho benchmark
│   ├── broker/                                            # Broker Abstraction Layer [NEW]
│   │   ├── broker.interface.ts                            # IMessagePublisher, IMessageConsumer
│   │   ├── broker.factory.ts                              # Factory cấp phát instance theo MQ_TYPE
│   │   ├── rabbitmq.broker.ts                             # amqplib implementation
│   │   ├── activemq.broker.ts                             # stompit implementation
│   │   └── kafka.broker.ts                                # kafkajs implementation
│   ├── order-service/                                     # Order API Service [NEW]
│   │   ├── server.ts                                      # Fastify HTTP Server
│   │   └── order.controller.ts                            # POST /orders handler
│   ├── payment-worker/                                    # Asynchronous Payment Worker [NEW]
│   │   ├── worker.ts                                      # Worker entry point
│   │   └── payment.processor.ts                           # Mock logic: delay, fail twice, retry
│   └── sync-baseline/                                     # Phục vụ Test 2 đối chứng Sync HTTP [NEW]
│       ├── sync-order-service.ts                          # Order gọi trực tiếp Sync HTTP sang Payment
│       └── sync-payment-service.ts                        # Payment Service giả lập sleep 3000ms
├── benchmarks/                                            # Load testing scripts [NEW]
│   ├── k6/
│   │   ├── test1-normal.js                                # k6 script: 1000 orders, 50 VUs, delay 0
│   │   ├── test2-slow-payment.js                          # k6 script: 1000 orders, 50 VUs, delay 3000ms
│   │   └── test3-retry.js                                 # k6 script: 100 orders, 10 VUs, retry test
│   └── scripts/
│       ├── run-benchmark.ps1                              # Script tự động hóa 27 runs & xuất CSV
│       └── run-k6.ps1                                     # Wrapper chạy k6 qua Docker container
├── results/                                               # Chứa kết quả raw đo đạc thực tế [NEW]
│   ├── normal.csv
│   ├── slow-payment.csv
│   ├── retry.csv
│   └── summary.csv
├── package.json                                           # Khai báo dependencies Node.js [NEW]
├── tsconfig.json                                          # TypeScript config [NEW]
├── .gitignore                                             # Git ignore rules [NEW]
└── README.md                                              # Hướng dẫn setup, vận hành và tái tạo kết quả [NEW]
```

---

## 6. Lộ Trình Thực Hiện (Execution Roadmap)

1. **Giai đoạn 1: Lập tài liệu kiến trúc & Tạo khung Package (Nhiệm vụ hiện tại)**:
   - Tạo file `requirements/SYSTEM_ARCHITECTURE_AND_PLAN.md` đầy đủ, chi tiết bằng tiếng Việt và tiếng Anh.
   - Tạo toàn bộ cây thư mục và khung các file nguồn (`package.json`, `tsconfig.json`, các file `.ts`, file docker-compose, script benchmark).
2. **Giai đoạn 2: Cài đặt Dependencies & Base Code**:
   - `npm install` các package cần thiết (`fastify`, `amqplib`, `stompit`, `kafkajs`, `uuid`, `dotenv`, v.v.).
   - Hoàn thiện module `common` và `broker/broker.interface.ts`.
3. **Giai đoạn 3: Hiện thực luồng Priority 1 (RabbitMQ)**:
   - Viết `rabbitmq.broker.ts`, hoàn thiện `order-service` và `payment-worker`.
   - Chạy thử nghiệm luồng khép kín: k6 -> Order Service -> RabbitMQ -> Payment Worker.
4. **Giai đoạn 4: Mở rộng ActiveMQ & Kafka (Priority 2)**:
   - Viết `activemq.broker.ts` và `kafka.broker.ts`.
   - Kiểm tra khả năng hoán đổi chỉ bằng cách đổi `MQ_TYPE`.
5. **Giai đoạn 5: Hiện thực 3 Kịch Bản Test & Automation Script (Priority 3 & 4)**:
   - Viết k6 test scripts cho Test 1, Test 2 (kèm Sync baseline), Test 3.
   - Viết PowerShell automation script để tự động chạy lặp 3 lần cho mỗi broker, dọn dẹp hàng đợi, và xuất dữ liệu ra file CSV.
6. **Giai đoạn 6: Thu thập dữ liệu & Hoàn thiện README (Priority 5 & 6)**:
   - Thực thi các bài test, thu thập số liệu thực tế (không bịa đặt kết quả).
   - Điền số liệu vào các bảng trong README.md.

---

## 7. Verification Plan

### Kiểm tra tự động & Build
- `npm run build` hoặc `tsc --noEmit` để đảm bảo code TypeScript biên dịch không lỗi cú pháp và typing.
- Khởi động từng Docker container (`docker compose -f docker/docker-compose.rabbitmq.yml up -d`, v.v.) và kiểm tra health check.

### Kiểm tra thủ công
- Gửi 1 POST request `/orders` qua `curl` hoặc PowerShell `Invoke-RestMethod` và kiểm tra phản hồi HTTP 202 Accepted.
- Xem log của `Payment Worker` để xác nhận message đã được consume, delay đúng cấu hình và retry đúng số lần.
