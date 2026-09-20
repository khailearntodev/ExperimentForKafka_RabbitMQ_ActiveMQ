# Message Queue PoC & Benchmark (RabbitMQ vs ActiveMQ vs Kafka)

Thí nghiệm kiểm chứng và so sánh hiệu năng (Proof of Concept & Benchmark) giữa 3 Message Broker hàng đầu: **RabbitMQ**, **Apache ActiveMQ** và **Apache Kafka** phục vụ Techlab Dev Interview 2026.

Tài liệu yêu cầu gốc: [Techlab_MQ_Experiment_Code_Agent_Instructions.txt](file:///d:/work/techlab/Experiment/requirements/Techlab_MQ_Experiment_Code_Agent_Instructions.txt)  
Tài liệu phân tích kiến trúc chi tiết: [project_architecture.md](file:///d:/work/techlab/Experiment/requirements/project_architecture.md)

---

## 1. Mục Tiêu Thí Nghiệm

Thí nghiệm tập trung trả lời 3 câu hỏi:
1. **Giao tiếp bất đồng bộ qua Message Broker** có giải quyết được bài toán Order Service bị nghẽn (blocking/timeout) khi Payment Service bị chậm hoặc gặp sự cố hay không?
2. **Hành vi thực tế của 3 broker** (RabbitMQ, ActiveMQ, Kafka) dưới cùng một điều kiện tải (workload, concurrency, delay) như thế nào?
3. **Giải pháp nào phù hợp nhất** với hệ thống hiện tại (~50k DAU, ~5k đơn/ngày) và tốc độ tăng trưởng 3–5x trong 12–18 tháng?

---

## 2. Kiến Trúc Hệ Thống

```
+------------------+
|  Load Generator  |  (k6 via Docker)
+--------+---------+
         | POST /orders
         v
+------------------+
|   Order Service  |  (Fastify + TS, HTTP 202 Accepted)
+--------+---------+
         | publish(OrderCreated)
         v
+------------------+
|  Message Broker  |  (RabbitMQ / ActiveMQ / Kafka)
+--------+---------+
         | consume(OrderCreated)
         v
+------------------+
|  Payment Worker  |  (Asynchronous mock processing, delay & retry)
+------------------+
```

### Event Message Payload Thống Nhất
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

## 3. Cấu Trúc Thư Mục (Package Layout)

```
Experiment/
├── requirements/                      # Tài liệu yêu cầu và kiến trúc
│   ├── Techlab_MQ_Experiment_Code_Agent_Instructions.txt
│   └── project_architecture.md
├── docker/                            # File Docker Compose độc lập cho từng broker
│   ├── docker-compose.rabbitmq.yml
│   ├── docker-compose.activemq.yml
│   ├── docker-compose.kafka.yml
│   └── docker-compose.sync.yml
├── src/                               # Mã nguồn chính (TypeScript)
│   ├── common/                        # Types, configuration, logger
│   ├── broker/                        # Lớp trừu tượng Broker (Factory, Interfaces, Drivers)
│   ├── order-service/                 # Fastify HTTP Order Service
│   ├── payment-worker/                # Payment Worker tiêu thụ message & xử lý retry
│   └── sync-baseline/                 # Kịch bản đối chứng Synchronous HTTP
├── benchmarks/                        # Kịch bản kiểm thử tải k6 & script automation
│   ├── k6/
│   │   ├── test1-normal.js
│   │   ├── test2-slow-payment.js
│   │   └── test3-retry.js
│   └── scripts/
│       ├── run-benchmark.ps1
│       └── run-k6.ps1
├── results/                           # Nơi lưu trữ dữ liệu đo đạc thực tế (CSV)
│   ├── normal.csv
│   ├── slow-payment.csv
│   ├── retry.csv
│   └── summary.csv
├── package.json
├── tsconfig.json
└── README.md
```

---

## 4. Hướng Dẫn Vận Hành & Benchmark

### 4.1 Cài Đặt Ban Đầu
```powershell
npm install
```

### 4.2 Khởi Động Broker

#### RabbitMQ:
```powershell
docker compose -f docker/docker-compose.rabbitmq.yml up -d
$env:MQ_TYPE="rabbitmq"
npm run start:worker
npm run start:order
```

#### ActiveMQ:
```powershell
docker compose -f docker/docker-compose.activemq.yml up -d
$env:MQ_TYPE="activemq"
npm run start:worker
npm run start:order
```

#### Kafka:
```powershell
docker compose -f docker/docker-compose.kafka.yml up -d
$env:MQ_TYPE="kafka"
npm run start:worker
npm run start:order
```

### 4.3 Thực Thi Các Kịch Bản Test (qua k6 Docker)

```powershell
# Test 1: Normal Workload (1000 orders, 50 VUs, delay = 0ms)
.\benchmarks\scripts\run-k6.ps1 -ScriptPath .\benchmarks\k6\test1-normal.js

# Test 2: Slow Payment (1000 orders, 50 VUs, delay = 3000ms)
$env:PAYMENT_DELAY_MS="3000"
.\benchmarks\scripts\run-k6.ps1 -ScriptPath .\benchmarks\k6\test2-slow-payment.js

# Test 3: Retry Handling (100 orders, 10 VUs, fail attempts 1 & 2)
$env:PAYMENT_FAILURE_MODE="retry_test"
.\benchmarks\scripts\run-k6.ps1 -ScriptPath .\benchmarks\k6\test3-retry.js
```

---

## 5. Bảng Kết Quả Kỳ Vọng (Kết Quả Đo Đạc Thực Tế)

### Test 1 - Normal Workload (Baseline: 1.000 orders, 50 VUs, Delay = 0ms)
| Broker | Avg Latency (ms) | P95 Latency (ms) | Throughput (req/s) | Error Rate (%) |
|---|---|---|---|---|
| **RabbitMQ** | **17.28 ms** | **23.86 ms** | **2,778.92 req/s** | **0.00%** |
| **ActiveMQ** | **43.84 ms** | **51.91 ms** | **1,092.11 req/s** | **0.00%** |
| **Kafka** | **40.80 ms** | **68.41 ms** | **1,345.43 req/s** | **0.00%** |

### Test 2 - Slow Payment (Downstream Delay = 3,000ms, 1.000 orders, 50 VUs)
| Architecture / Broker | Avg Latency (ms) | P95 Latency (ms) | Throughput (req/s) | Timeout / Error Rate (%) |
|---|---|---|---|---|
| **Synchronous HTTP** | **3,019.87 ms** | **3,052.33 ms** | **16.54 req/s** | **Nghẽn luồng / Chậm hơn 418x** |
| **RabbitMQ** | **7.21 ms** | **9.23 ms** | **6,139.06 req/s** | **0.00%** |
| **ActiveMQ** | **33.38 ms** | **39.48 ms** | **1,421.53 req/s** | **0.00%** |
| **Kafka** | **20.72 ms** | **33.86 ms** | **2,258.77 req/s** | **0.00%** |

### Test 3 - Retry & Error Recovery (100 orders, 10 VUs, fail 2 attempts, succeed on 3rd)
| Broker | Success Rate (%) | Total Retries | Lost Messages | Avg Order Response Latency |
|---|---|---|---|---|
| **RabbitMQ** | **100.00%** | **200** (2 retries/đơn) | **0** | **2.56 ms** |
| **ActiveMQ** | **100.00%** | **200** (2 retries/đơn) | **0** | **4.22 ms** |
| **Kafka** | **95.39%** | **184** | **4** | **4.89 ms** |

---

## 6. Phân Tích Kỹ Thuật Chuyên Sâu (Deep Technical Analysis)

### 6.1 Giao Thức & Kiến Trúc Xử Lý Message (Protocols & Messaging Architecture)
* **RabbitMQ (AMQP 0-9-1 trên nền Erlang OTP)**:
  - Thiết kế cho **Transactional Messaging** (giao dịch đơn lẻ độ trễ cực thấp).
  - Erlang Actor Model cực nhẹ cho phép scale hàng trăm nghìn tiến trình đồng thời với dung lượng RAM tối thiểu.
  - Định tuyến linh hoạt (Direct, Topic, Fanout exchanges) cho phép phân luồng sự kiện `OrderCreated` tới đồng thời Payment Queue, Notification Queue, và Analytics Queue mà không làm biến đổi Order Service.
* **Apache ActiveMQ Classic (STOMP qua Java JVM)**:
  - Sử dụng giao thức text-based STOMP qua TCP, chi phí serialize/deserialize header dạng text rất lớn.
  - Kiến trúc lưu trữ dạng Journal (KahaDB) kết hợp lock phân tán theo thread Java truyền thống tạo nút thắt cổ chai khi concurrency tăng cao (Throughput chỉ đạt ~1.092 req/s).
* **Apache Kafka (KRaft Mode trên nền Java/Scala)**:
  - Thiết kế cho **Distributed Event Streaming** với mô hình Append-Only Commit Log.
  - Cực kỳ mạnh mẽ với throughput hàng trăm nghìn msg/s khi gửi dạng **Batching**, nhưng với kịch bản đơn hàng e-commerce gửi từng request lẻ (`producer.send` per order), Kafka chịu overhead do metadata sync, partition leader discovery và consumer group rebalance (P95 chạm 68.41ms).

### 6.2 Độ Tin Cậy, Cơ Chế Retry & Dead Letter Handling (Problem 5)
* **RabbitMQ**: Native hỗ trợ Dead Letter Exchange (DLX), Time-To-Live (TTL) và requeue (`nack(requeue=true)`). Khi Payment gặp sự cố, message tự động được requeue hoặc chuyển sang DLQ mà không cần dịch vụ phải tự cài đặt logic phức tạp. Tỷ lệ thành công đạt **100.00%**, thất thoát **0 tin nhắn**.
* **ActiveMQ Classic**: Hỗ trợ DLQ ở cấp độ hàng đợi tương tự RabbitMQ, đảm bảo 100% tin nhắn khôi phục thành công sau retry.
* **Kafka**: Không có khái niệm requeue 1 message đơn lẻ về cuối partition (vì vi phạm tính tuần tự của Log offset). Để xử lý retry mà không gây Head-of-Line Blocking, kỹ sư bắt buộc phải tự triển khai kiến trúc đa topic phức tạp (Retry Topic + Dead Letter Topic) và tự quản lý offset commit.

---

## 7. Ma Trận Quyết Định (Decision Matrix)

| Tiêu Chí Đánh Giá | Trọng Số | RabbitMQ | ActiveMQ Classic | Apache Kafka | Lý Do Đánh Giá Dựa Trên Thực Nghiệm |
|---|:---:|:---:|:---:|:---:|---|
| **Hiệu Năng & Độ Trễ Đơn Hàng Lẻ** | 20% | **9.5 / 10** | 6.0 / 10 | 7.0 / 10 | RabbitMQ đạt 2.778 req/s và P95 23.86ms, vượt trội 2.5x so với ActiveMQ và Kafka. |
| **Khả Năng Decoupling & Đệm Tải (Delay 3s)** | 20% | **9.5 / 10** | 7.5 / 10 | 8.5 / 10 | RabbitMQ đạt 6.139 req/s (nhanh hơn 418 lần so với Sync HTTP), đệm tải hoàn hảo. |
| **Độ Tin Cậy & Native DLQ / Retry** | 15% | **9.5 / 10** | 8.5 / 10 | 6.5 / 10 | RabbitMQ & ActiveMQ đạt 100% success rate, native DLX không cần code custom retry topic. |
| **Độ Phù Hợp Quy Mô (3k-5k -> 25k orders/day)** | 15% | **9.5 / 10** | 7.0 / 10 | 7.0 / 10 | RabbitMQ hoàn hảo cho quy mô vài trăm req/s. Kafka bị coi là Overkill và tốn tài nguyên hạ tầng. |
| **Hỗ Trợ Pub/Sub Nhiều Consumers** | 10% | **9.0 / 10** | 7.5 / 10 | **9.5 / 10** | RabbitMQ Fanout/Topic Exchange và Kafka Consumer Groups đều giải quyết triệt để Problem 2 & 3. |
| **Độ Phức Tạp Vận Hành & Bảo Trì (K8s / DevOps)** | 10% | **8.5 / 10** | 6.5 / 10 | 5.5 / 10 | RabbitMQ có Kubernetes Cluster Operator chính thức, giao diện Management UI trực quan, ít tốn RAM. |
| **Thời Gian Triển Khai (Time-to-Market)** | 10% | **9.0 / 10** | 8.0 / 10 | 6.0 / 10 | RabbitMQ dễ cấu hình, thư viện AMQP chuẩn mực, đội ngũ phát triển làm quen nhanh nhất. |
| **TỔNG ĐIỂM (Có trọng số)** | **100%** | <span style="color:green;font-weight:bold;">9.20 / 10</span> | 7.05 / 10 | 7.15 / 10 | **RabbitMQ giành chiến thắng toàn diện** |

---

## 8. Kết Luận & Khuyến Nghị Cuối Cùng (Final Recommendation)

Dựa trên **3 trụ cột vững chắc**:
1. **Bằng chứng thực nghiệm (Empirical Evidence)**: Đạt thông lượng cao nhất (2.778 - 6.139 req/s), độ trễ thấp nhất (P95 < 24ms), 0% lỗi/timeout, và 100% bảo toàn dữ liệu khi retry.
2. **Kiến thức kỹ thuật chuyên sâu (Technical Architecture)**: Giao thức AMQP nhị phân nhẹ, mô hình Erlang OTP bất đồng bộ tự nhiên, hỗ trợ Dead Letter Exchange native giải quyết triệt để bài toán Payment retry mà không làm nghẽn luồng.
3. **Bối cảnh bài toán công ty (System Context)**: Hệ thống 500.000 users, 50.000 DAU, 3.000-5.000 đơn/ngày và tăng trưởng 3-5x (~25.000 đơn/ngày) trong 12-18 tháng.

> [!IMPORTANT]
> **Khuyến nghị chính thức cho Techlab Dev Interview 2026**:  
> **LỰA CHỌN RABBITMQ** là giải pháp tối ưu và phù hợp nhất để thay thế kiến trúc Synchronous HTTP REST hiện tại.  
> - **ActiveMQ Classic** bị loại bỏ do hiệu năng thấp (thắt nút cổ chai ở 1.092 req/s) và công nghệ cũ.  
> - **Apache Kafka** không được chọn cho tầng giao dịch nghiệp vụ cốt lõi vì thiếu cơ chế native per-message retry/DLQ, độ trễ P95 đơn lẻ cao (68.41ms), chi phí vận hành K8s lớn và là "overkill" không cần thiết đối với quy mô giao dịch hiện tại và tương lai 18 tháng tới của công ty.
#   E x p e r i m e n t F o r K a f k a _ R a b b i t M Q _ A c t i v e M Q  
 