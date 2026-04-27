# LedgerFlow

High-performance, event-driven FinTech ledger application demonstrating strict double-entry accounting and distributed systems resilience.

## 🚀 Architecture & Key Features

* **Distributed Transactions (Saga Pattern):** Orchestrates complex multi-wallet transfers safely using Kafka as an event broker.
* **Concurrency Control:** Utilizes PostgreSQL optimistic locking (`@Version`) to completely eliminate double-spend anomalies under heavy load.
* **Idempotency:** Implements Redis-backed distributed locks to ensure messages are processed exactly once, protecting the ledger from network retries.
* **Modern Messaging:** Uses Kafka in KRaft mode (no Zookeeper) for lightweight, high-throughput event streaming.
* **Observability:** Fully integrated Spring Boot Actuator for enterprise-grade health and metrics monitoring.

## 🛠️ Tech Stack

* **Core:** Java 21, Spring Boot 3.x
* **Database & Cache:** PostgreSQL 15, Redis 7
* **Message Broker:** Confluent Kafka 7.4 (KRaft Mode)
* **Testing:** JUnit 5, Testcontainers, Awaitility
* **Infrastructure:** Docker & Docker Compose
* **API Documentation:** Springdoc OpenAPI (Swagger UI)

## ⚡ Quick Start (One-Click Setup)

The entire distributed architecture (API, Database, Cache, and Message Broker) is fully containerized. You do not need to install anything other than Docker.

### 1. Clone the repository

```bash
git clone https://github.com/ayushcodes27/ledger-flow.git
cd ledgerflow
```

### 2. Spin up the infrastructure

```bash
docker-compose up -d
```

> **Note:** This will download the required images and start PostgreSQL, Redis, Kafka, and the LedgerFlow application.

### 3. Verify the deployment

```bash
docker ps
```

## 📖 API Documentation & Monitoring

Once the application is running, you can interact with the API and monitor its health using the built-in dashboards:

| Tool | URL |
|------|-----|
| Swagger UI (Interactive API Docs) | http://localhost:8088/swagger-ui.html |
| Actuator Health Check | http://localhost:8088/actuator/health |

## 🧪 Testing

The project uses Testcontainers to spin up ephemeral Docker containers for PostgreSQL, Redis, and Kafka during integration tests. This ensures tests run in an identical environment to production.

To run the complete test suite, including the high-concurrency stress tests:

```bash
mvn clean test
```