# Message Queue Evaluation — RabbitMQ vs ActiveMQ vs Kafka

## TECHLAB – DEV INTERVIEW 2026

| | |
|---|---|
| **Position** | Intern Developer |
| **Candidate** | Nguyen Quang Khai |
| **Submission Date** | 21/09/2026 |
| **Duration** | 2 weeks |

---

## 📋 Table of Contents

1. [Bối Cảnh & Bài Toán](#1-bối-cảnh--bài-toán)
2. [Mục Tiêu Thí Nghiệm](#2-mục-tiêu-thí-nghiệm)
3. [Kiến Trúc Hệ Thống](#3-kiến-trúc-hệ-thống)
4. [Cấu Trúc Thư Mục](#4-cấu-trúc-thư-mục)
5. [Cấu Hình Thí Nghiệm](#5-cấu-hình-thí-nghiệm)
6. [Hướng Dẫn Vận Hành & Benchmark](#6-hướng-dẫn-vận-hành--benchmark)
7. [Kết Quả Đo Đạc Thực Tế](#7-kết-quả-đo-đạc-thực-tế)
8. [Phân Tích Kỹ Thuật Chuyên Sâu](#8-phân-tích-kỹ-thuật-chuyên-sâu)
9. [Ma Trận Quyết Định](#9-ma-trận-quyết-định)
10. [Kết Luận & Khuyến Nghị](#10-kết-luận--khuyến-nghị)

---

## 1. Bối Cảnh & Bài Toán

Công ty đang phát triển một **nền tảng thương mại điện tử quy mô lớn** phục vụ thị trường Việt Nam với các chỉ số:

| Chỉ Số | Hiện Tại | Dự Kiến (12–18 tháng) |
|---|---|---|
| Số người dùng đăng ký | 500,000 | 1,500,000 – 2,500,000 |
| Người dùng hoạt động hàng ngày (DAU) | 50,000 | 150,000 – 250,000 |
| Số đơn hàng / ngày | 3,000 – 5,000 | 15,000 – 25,000 |

**Vấn đề cốt lõi**: Backend hiện tại sử dụng kiến trúc microservices với giao tiếp **Synchronous HTTP REST** giữa các service. Khi Payment Service bị chậm hoặc gặp sự cố, Order Service bị **nghẽn luồng (blocking)**, gây timeout và ảnh hưởng trực tiếp trải nghiệm người dùng.

**Giải pháp đề xuất**: Đưa vào một **Message Queue** để xử lý bất đồng bộ (async) và tách rời (decouple) các service. Cần chọn ra giải pháp phù hợp nhất trong 3 ứng viên:
- **RabbitMQ**
- **Apache ActiveMQ**
- **Apache Kafka**

---

## 2. Mục Tiêu Thí Nghiệm

Thí nghiệm tập trung trả lời 3 câu hỏi:

1. **Giao tiếp bất đồng bộ qua Message Broker** có giải quyết được bài toán Order Service bị nghẽn khi Payment Service chậm hoặc gặp sự cố hay không?
2. **Hành vi thực tế của 3 broker** dưới cùng điều kiện tải (workload, concurrency, delay) như thế nào?
3. **Giải pháp nào phù hợp nhất** với quy mô hiện tại và tốc độ tăng trưởng 3–5x trong 12–18 tháng?

### Nguyên Tắc Công Bằng (Fairness Rules)
- ✅ Cùng máy host, cùng Docker resource limits
- ✅ Cùng k6 test script, cùng payload JSON
- ✅ Cùng số lượng VUs và iterations cho mỗi kịch bản
- ✅ 3 lần chạy / kịch bản / broker → lấy trung bình
- ✅ Không giả định kết quả trước khi test

---

## 3. Kiến Trúc Hệ Thống

### 3.1 Kiến Trúc Async (Message Queue)

```
+------------------+
|  Load Generator  |  (k6 via Docker)
+--------+---------+
         | POST /orders
         v
+------------------+
|   Order Service  |  (Fastify + TypeScript, HTTP 202 Accepted)
+--------+---------+
         | publish(OrderCreatedEvent)
         v
+------------------+
|  Message Broker  |  (RabbitMQ / ActiveMQ / Kafka — lần lượt)
+--------+---------+
         | consume(OrderCreatedEvent)
         v
+------------------+
|  Payment Worker  |  (Async mock processing, configurable delay & retry)
+------------------+
```

### 3.2 Kiến Trúc Sync Baseline (Đối Chứng)

```
+------------------+
|  Load Generator  |  (k6 via Docker)
+--------+---------+
         | POST /orders
         v
+------------------+       Blocking HTTP
|   Order Service  | ───────────────────> Payment Service
|  (Sync Client)   | <──────────────────  (Mock 3s delay)
+------------------+       Wait 3,000ms
```

> Order Service bị **chặn hoàn toàn** cho đến khi Payment Service trả về response → chứng minh sự cần thiết của Message Queue.

### 3.3 Event Message Payload (Thống Nhất)

Toàn bộ 3 broker sử dụng chung 1 payload JSON duy nhất:

```json
{
  "eventId": "UUID",
  "orderId": "ORD-001",
  "userId": "USER-001",
  "amount": 100000,
  "eventType": "ORDER_CREATED",
  "timestamp": "2026-09-20T14:50:00.000Z"
}
```

---

## 4. Cấu Trúc Thư Mục

```
Experiment/
├── docker/                            # Docker Compose cho từng broker
│   ├── docker-compose.rabbitmq.yml
│   ├── docker-compose.activemq.yml
│   ├── docker-compose.kafka.yml
│   └── docker-compose.sync.yml
├── src/                               # Mã nguồn (TypeScript)
│   ├── common/                        # Types, config, logger
│   ├── broker/                        # Broker abstraction (Factory + Interface + Drivers)
│   │   ├── broker.interface.ts        # IMessagePublisher, IMessageConsumer
│   │   ├── broker.factory.ts          # createPublisher(), createConsumer()
│   │   ├── rabbitmq.publisher.ts
│   │   ├── rabbitmq.consumer.ts
│   │   ├── activemq.publisher.ts
│   │   ├── activemq.consumer.ts
│   │   ├── kafka.publisher.ts
│   │   └── kafka.consumer.ts
│   ├── order-service/                 # Fastify HTTP API → publish to MQ
│   ├── payment-worker/                # Consumer → process OrderCreatedEvent
│   └── sync-baseline/                 # Synchronous HTTP baseline (control)
├── benchmarks/                        # Kịch bản k6 & automation scripts
│   ├── k6/
│   │   ├── test1-normal.js            # 1000 iterations, 50 VUs, delay=0
│   │   ├── test2-slow-payment.js      # 1000 iterations, 50 VUs, delay=3000ms
│   │   └── test3-retry.js             # 100 iterations, 10 VUs, 50% fail rate
│   └── scripts/
│       ├── run-benchmark.ps1          # Full automation: start broker → run tests → collect data
│       └── run-k6.ps1                 # k6 runner with JSON export
├── results/                           # Dữ liệu đo đạc thực tế
│   ├── raw/                           # 31 file JSON raw metrics (k6 export)
│   └── summary.csv                    # Bảng tổng hợp tất cả kết quả
├── package.json
├── tsconfig.json
└── README.md
```

---

## 5. Cấu Hình Thí Nghiệm

### 5.1 Technology Stack

| Component | Technology | Version |
|---|---|---|
| Runtime | Node.js + TypeScript | tsx ^4.16.2, TS ^5.4.5 |
| HTTP Framework | Fastify | ^4.28.1 |
| RabbitMQ Client | amqplib | ^0.10.4 |
| ActiveMQ Client | stompit (STOMP) | ^1.0.0 |
| Kafka Client | kafkajs | ^2.2.4 |
| Load Testing | k6 (Grafana) | Docker image |
| Containerization | Docker Compose | — |

### 5.2 Broker Configuration

| Broker | Image | Protocol | Port |
|---|---|---|---|
| RabbitMQ | `rabbitmq:3.13-management-alpine` | AMQP 0-9-1 | 5672 |
| ActiveMQ | `apache/activemq-classic:5.18.3` | STOMP | 61613 |
| Kafka | `apache/kafka:3.7.0` (KRaft, no ZooKeeper) | Plaintext | 9092 |

### 5.3 Kịch Bản Test (3 Scenarios × 3 Runs × 3 Brokers + 3 Sync = 30 Runs Chính Thức)

| Kịch Bản | Mục Đích | VUs | Iterations | Payment Delay | Failure Mode |
|---|---|:---:|:---:|:---:|:---:|
| **Test 1** — Normal Throughput | Đo throughput & latency tối đa | 50 | 1,000 | 0 ms | none |
| **Test 2** — Slow Payment | Đo khả năng decoupling khi downstream chậm | 50 | 1,000 | 3,000 ms | none |
| **Test 3** — Retry & Failure | Đo xử lý lỗi, DLQ, retry mechanism | 10 | 100 | 0 ms | random-50 (50% fail) |

---

## 6. Hướng Dẫn Vận Hành & Benchmark

### 6.1 Cài Đặt

```powershell
npm install
```

### 6.2 Chạy từng Broker

#### RabbitMQ
```powershell
docker compose -f docker/docker-compose.rabbitmq.yml up -d
$env:MQ_TYPE="rabbitmq"
npm run start:worker    # Terminal 1
npm run start:order     # Terminal 2
```

#### ActiveMQ
```powershell
docker compose -f docker/docker-compose.activemq.yml up -d
$env:MQ_TYPE="activemq"
npm run start:worker
npm run start:order
```

#### Kafka
```powershell
docker compose -f docker/docker-compose.kafka.yml up -d
# Tạo topic trước khi chạy consumer (yêu cầu bắt buộc cho Kafka)
docker exec techlab-kafka /opt/kafka/bin/kafka-topics.sh --create --topic order.created --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
$env:MQ_TYPE="kafka"
npm run start:worker
npm run start:order
```

#### Synchronous Baseline (Đối chứng)
```powershell
npm run start:sync-payment    # Terminal 1 (Mock Payment Service, port 3001)
npm run start:sync-order      # Terminal 2 (Order Service → blocking call to Payment)
```

### 6.3 Chạy Test k6

```powershell
# Test 1: Normal Workload
.\benchmarks\scripts\run-k6.ps1 -ScriptPath .\benchmarks\k6\test1-normal.js

# Test 2: Slow Payment
.\benchmarks\scripts\run-k6.ps1 -ScriptPath .\benchmarks\k6\test2-slow-payment.js

# Test 3: Retry Handling
.\benchmarks\scripts\run-k6.ps1 -ScriptPath .\benchmarks\k6\test3-retry.js
```

### 6.4 Automation (Full Pipeline)

```powershell
# Chạy toàn bộ 3 tests × 3 runs cho 1 broker
.\benchmarks\scripts\run-benchmark.ps1 -Broker rabbitmq
.\benchmarks\scripts\run-benchmark.ps1 -Broker activemq
.\benchmarks\scripts\run-benchmark.ps1 -Broker kafka
```

---

## 7. Kết Quả Đo Đạc Thực Tế

> Tất cả kết quả dưới đây là **trung bình của 3 lần chạy** cho mỗi kịch bản. Dữ liệu raw (31 file JSON) được lưu trong `results/raw/`.

### 7.1 Test 1 — Normal Throughput (1,000 orders, 50 VUs, Delay = 0ms)

| Broker | Avg Latency | P95 Latency | Throughput | Error Rate |
|:---|:---:|:---:|:---:|:---:|
| 🟢 **RabbitMQ** | **17.28 ms** | **23.86 ms** | **2,778.92 req/s** | 0.00% |
| 🟡 Kafka | 40.80 ms | 68.41 ms | 1,345.43 req/s | 0.00% |
| 🔴 ActiveMQ | 43.84 ms | 51.91 ms | 1,092.11 req/s | 0.00% |

### 7.2 Test 2 — Slow Payment (1,000 orders, 50 VUs, Delay = 3,000ms)

| Architecture / Broker | Avg Latency | P95 Latency | Throughput | Ghi Chú |
|:---|:---:|:---:|:---:|:---|
| ⛔ **Synchronous HTTP** | **3,019.87 ms** | **3,052.33 ms** | **16.54 req/s** | Nghẽn hoàn toàn |
| 🟢 **RabbitMQ** | **7.21 ms** | **9.23 ms** | **6,139.06 req/s** | Nhanh hơn Sync **371 lần** |
| 🟡 Kafka | 20.72 ms | 33.86 ms | 2,258.77 req/s | — |
| 🔴 ActiveMQ | 33.38 ms | 39.48 ms | 1,421.53 req/s | — |

> **Kết quả quan trọng nhất**: Khi Payment Service chậm 3 giây, Synchronous HTTP chỉ xử lý được **16.54 req/s** (tương đương ~1,430 đơn/ngày), trong khi RabbitMQ đạt **6,139 req/s** mà Order Service vẫn trả về ngay lập tức (7.21ms). Đây là bằng chứng thực nghiệm mạnh mẽ nhất chứng minh sự cần thiết của Message Queue.

### 7.3 Test 3 — Retry & Error Recovery (100 orders, 10 VUs, 50% fail rate)

| Broker | Retry Success Rate | Avg Latency | Throughput | Lost Messages | Native DLQ/DLX |
|:---|:---:|:---:|:---:|:---:|:---:|
| 🟢 **RabbitMQ** | **100.00%** | **2.56 ms** | **3,105.92 req/s** | **0** | ✅ Native DLX |
| 🟡 ActiveMQ | 100.00% | 4.22 ms | 1,934.18 req/s | 0 | ✅ DLQ |
| 🔴 Kafka | 95.39% | 4.89 ms | 1,637.37 req/s | Messages lost | ❌ Application-level |

### 7.4 Bảng Tổng Hợp (`results/summary.csv`)

| Broker | Scenario | Avg Latency (ms) | P95 Latency (ms) | Throughput (req/s) | Error Rate (%) | Retry Success (%) |
|---|---|:---:|:---:|:---:|:---:|:---:|
| RabbitMQ | test1 | 17.28 | 23.86 | 2,778.92 | 0.00 | 100.00 |
| RabbitMQ | test2 | 7.21 | 9.23 | 6,139.06 | 0.00 | 100.00 |
| RabbitMQ | test3 | 2.56 | 4.07 | 3,105.92 | 0.00 | 100.00 |
| ActiveMQ | test1 | 43.84 | 51.91 | 1,092.11 | 0.00 | 100.00 |
| ActiveMQ | test2 | 33.38 | 39.48 | 1,421.53 | 0.00 | 100.00 |
| ActiveMQ | test3 | 4.22 | 5.36 | 1,934.18 | 0.00 | 100.00 |
| Kafka | test1 | 40.80 | 68.41 | 1,345.43 | 0.00 | 100.00 |
| Kafka | test2 | 20.72 | 33.86 | 2,258.77 | 0.00 | 100.00 |
| Kafka | test3 | 4.89 | 9.66 | 1,637.37 | 0.00 | 95.39 |
| Sync HTTP | test2 | 3,019.87 | 3,052.33 | 16.54 | 0.00 | 100.00 |

### 7.5 So Sánh Trực Quan — RabbitMQ Advantage Multiplier

| Metric | RabbitMQ vs ActiveMQ | RabbitMQ vs Kafka | RabbitMQ vs Sync HTTP |
|---|:---:|:---:|:---:|
| Test1 Throughput | **2.54x** nhanh hơn | **2.07x** nhanh hơn | — |
| Test2 Throughput | **4.32x** nhanh hơn | **2.72x** nhanh hơn | **371x** nhanh hơn |
| Test3 Throughput | **1.61x** nhanh hơn | **1.90x** nhanh hơn | — |
| Test1 P95 Latency | **2.18x** thấp hơn | **2.87x** thấp hơn | — |
| Test2 P95 Latency | **4.28x** thấp hơn | **3.67x** thấp hơn | **330x** thấp hơn |
| Test3 P95 Latency | **1.32x** thấp hơn | **2.37x** thấp hơn | — |

---

## 8. Phân Tích Kỹ Thuật Chuyên Sâu

### 8.1 Giao Thức & Kiến Trúc Xử Lý Message

**RabbitMQ (AMQP 0-9-1 trên nền Erlang OTP)**:
- Thiết kế cho **Transactional Messaging** — giao dịch đơn lẻ, độ trễ cực thấp.
- Erlang Actor Model cực nhẹ, scale hàng trăm nghìn tiến trình đồng thời với RAM tối thiểu.
- Định tuyến linh hoạt (Direct, Topic, Fanout exchanges) cho phép phân luồng sự kiện `OrderCreated` tới Payment Queue, Notification Queue, và Analytics Queue đồng thời mà không cần sửa Order Service.

**Apache ActiveMQ Classic (STOMP qua Java JVM)**:
- Giao thức text-based STOMP, chi phí serialize/deserialize header lớn.
- Kiến trúc lưu trữ Journal (KahaDB) + lock thread Java truyền thống → nút thắt cổ chai khi concurrency cao (chỉ đạt ~1,092 req/s).
- Thế hệ cũ, community giảm dần — ActiveMQ Artemis mới hơn nhưng không phải đề bài yêu cầu.

**Apache Kafka (KRaft Mode, Java/Scala)**:
- Thiết kế cho **Distributed Event Streaming** — Append-Only Commit Log.
- Cực mạnh với throughput hàng trăm nghìn msg/s khi gửi dạng **Batching**, nhưng với kịch bản e-commerce gửi từng request lẻ (`producer.send` per order), Kafka chịu overhead do metadata sync, partition leader discovery, và consumer group rebalance (P95 chạm 68.41ms).
- Kafka cold-start bất ổn: Run 1 của Test 1 có P95 lên tới **124ms** và max **436ms** do metadata refresh.

### 8.2 Độ Tin Cậy, Retry & Dead Letter Handling

| Đặc Điểm | RabbitMQ | ActiveMQ | Kafka |
|---|---|---|---|
| **Native DLX/DLQ** | ✅ Dead Letter Exchange | ✅ DLQ | ❌ Phải tự xây dựng |
| **Per-message requeue** | ✅ `nack(requeue=true)` | ✅ Redeliver | ❌ Vi phạm Log offset ordering |
| **TTL support** | ✅ Native | ✅ Hạn chế | ❌ Không có |
| **Retry thành công** | 100% (thực nghiệm) | 100% (thực nghiệm) | 95.39% (mất tin nhắn) |
| **Cách xử lý retry** | Config DLX + TTL | Config DLQ | Code custom Retry Topic + DLT Topic + offset commit |

### 8.3 Kịch Bản Đối Chứng Synchronous HTTP

Kết quả Test 2 cho thấy:
- **Sync HTTP**: Mỗi request bị **block 3,019ms** (gần bằng `PAYMENT_DELAY_MS=3000ms`) → Order Service hoàn toàn bị khóa.
- **RabbitMQ**: Order Service trả về **7.21ms** — publish xong message là trả về ngay, Payment Worker xử lý bất đồng bộ → **User không phải chờ**.
- Tỷ lệ cải thiện: **371x throughput**, **330x P95 latency** — con số này chứng minh rõ ràng nhất rằng **Message Queue là bắt buộc**, không phải "nice to have".

---

## 9. Ma Trận Quyết Định (Decision Matrix)

| Tiêu Chí Đánh Giá | Trọng Số | RabbitMQ | ActiveMQ | Kafka | Lý Do |
|---|:---:|:---:|:---:|:---:|---|
| Hiệu Năng & Độ Trễ Đơn Hàng Lẻ | 20% | **9.5** | 6.0 | 7.0 | RabbitMQ đạt 2,778 req/s, P95=23.86ms — vượt trội 2.5x |
| Khả Năng Decoupling (Delay 3s) | 20% | **9.5** | 7.5 | 8.5 | RabbitMQ 6,139 req/s — nhanh hơn Sync 371 lần |
| Độ Tin Cậy & Native DLQ/Retry | 15% | **9.5** | 8.5 | 6.5 | RabbitMQ & ActiveMQ: 100% success, Kafka: 95.39% |
| Phù Hợp Quy Mô (5k→25k đơn/ngày) | 15% | **9.5** | 7.0 | 7.0 | RabbitMQ tối ưu cho vài trăm req/s, Kafka overkill |
| Hỗ Trợ Pub/Sub Nhiều Consumers | 10% | **9.0** | 7.5 | **9.5** | Cả RabbitMQ (Exchange) và Kafka (Consumer Group) đều tốt |
| Độ Phức Tạp Vận Hành (K8s/DevOps) | 10% | **8.5** | 6.5 | 5.5 | RabbitMQ có K8s Operator chính thức, UI trực quan, ít RAM |
| Thời Gian Triển Khai | 10% | **9.0** | 8.0 | 6.0 | AMQP lib chuẩn, config đơn giản, dev làm quen nhanh |
| **TỔNG ĐIỂM (Có Trọng Số)** | **100%** | **9.20 / 10** | **7.05 / 10** | **7.15 / 10** | **RabbitMQ chiến thắng toàn diện** |

---

## 10. Kết Luận & Khuyến Nghị

Dựa trên **3 trụ cột vững chắc**:

### 📊 Bằng Chứng Thực Nghiệm (Empirical Evidence)
- Throughput cao nhất trên cả 3 kịch bản: **2,778 – 6,139 req/s**
- Độ trễ thấp nhất: P95 < **24ms** (test1), < **10ms** (test2)
- Error rate: **0.00%** trên tất cả runs
- Retry success: **100%**, mất **0 tin nhắn**

### 🔬 Kiến Trúc Kỹ Thuật (Technical Architecture)
- Giao thức AMQP nhị phân nhẹ, hiệu quả hơn STOMP (text-based) và Kafka Protocol (batch-optimized)
- Erlang OTP Actor Model — bất đồng bộ tự nhiên, tiêu thụ ít tài nguyên
- Dead Letter Exchange (DLX) native — giải quyết triệt để bài toán Payment retry mà không cần custom code

### 🏢 Phù Hợp Bối Cảnh Công Ty (System Context Fit)
- Hệ thống 500,000 users, 50,000 DAU, 3,000–5,000 đơn/ngày
- Tăng trưởng 3–5x → ~25,000 đơn/ngày — RabbitMQ xử lý dễ dàng
- Kafka là **"overkill"** cho quy mô này — chi phí vận hành, hạ tầng K8s, và độ phức tạp không cần thiết

---

> ### ✅ Khuyến Nghị Chính Thức
>
> **LỰA CHỌN RABBITMQ** là giải pháp tối ưu và phù hợp nhất để thay thế kiến trúc Synchronous HTTP REST hiện tại.
>
> - **ActiveMQ Classic** bị loại bỏ do hiệu năng thấp (nút thắt cổ chai ở 1,092 req/s) và công nghệ thế hệ cũ.
> - **Apache Kafka** không được chọn cho tầng giao dịch nghiệp vụ cốt lõi vì thiếu cơ chế native per-message retry/DLQ, P95 đơn lẻ cao (68.41ms), chi phí vận hành lớn, và là "overkill" đối với quy mô hiện tại và 18 tháng tới.

---

## 📁 Dữ Liệu Raw

Toàn bộ dữ liệu benchmark gốc (31 file JSON) được lưu trong thư mục `results/raw/`, bao gồm:

| Nhóm | Số file | Mô tả |
|---|:---:|---|
| RabbitMQ | 10 | 3 test × 3 runs + 1 calibration run |
| ActiveMQ | 9 | 3 test × 3 runs |
| Kafka | 9 | 3 test × 3 runs |
| Synchronous HTTP | 3 | Test 2 × 3 runs |
| **Tổng** | **31** | — |

File tổng hợp: `results/summary.csv`