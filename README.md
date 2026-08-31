# Payment Gateway Notification Demo

A runnable interview demo showing:

- Payment Service → Kafka → Notification Service
- PostgreSQL persistence
- Redis-backed retry/circuit state
- Exponential backoff
- Per-client circuit breaker
- Amazon/Udemy webhook simulators
- React live dashboard
- Docker Compose

## Requirements

Only Docker Desktop is required to run the stack.

## Start

```bash
docker compose up --build
```

Open:

http://localhost:3000

## Demo script

### 1. Happy path

1. Keep Amazon UP.
2. Select Amazon.
3. Click MAKE PAYMENT.
4. Watch the notification become DELIVERED.

### 2. Retry + circuit breaker

1. Click MAKE DOWN for Amazon.
2. Make a payment for Amazon.
3. Watch attempts and `RETRYING`.
4. With threshold 3, the circuit becomes `OPEN`.
5. While open, no HTTP request is sent to Amazon; delivery remains retryable.

### 3. Recovery

1. Click MAKE UP for Amazon.
2. Wait for the circuit cooldown (15 seconds by default).
3. It transitions to HALF_OPEN.
4. A successful request closes the circuit.
5. Pending delivery is sent.

### 4. Client isolation

1. Keep Amazon DOWN.
2. Keep Udemy UP.
3. Make an Amazon payment and an Udemy payment.
4. Amazon can enter OPEN while Udemy remains CLOSED.

## Architecture

Kafka is used for durable payment-event ingestion. Once Notification Service consumes an event, it persists a delivery task in PostgreSQL and the Kafka consumer can progress. Webhook retries are handled independently so a broken Amazon webhook does not block unrelated Kafka events.

Redis is included as the fast distributed-state component for the demo stack. The sample circuit breaker uses in-process state to keep the project compact; for a production multi-instance deployment, move breaker state/locks fully into Redis (or use a shared distributed circuit-breaker implementation). PostgreSQL remains the durable delivery record.

## Important production considerations

- Outbox pattern between payment DB and Kafka
- Transactional/idempotent consumer
- Distributed circuit-breaker state
- Retry queue / delayed queue rather than polling at scale
- HMAC webhook signatures
- Authentication and mTLS where appropriate
- Per-client rate limits
- Dead-letter queue
- Jitter on exponential backoff
- Metrics, tracing and alerting
- Webhook payload versioning
- Idempotency-Key on every webhook
