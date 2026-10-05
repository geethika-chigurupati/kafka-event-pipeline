# kafka-event-pipeline

![CI](https://github.com/geethika-chigurupati/kafka-event-pipeline/actions/workflows/ci.yml/badge.svg)

An async Python pipeline that reads transaction events from Kafka, validates them,
and writes them to PostgreSQL. Bad messages are retried or routed to a dead-letter topic
instead of crashing the consumer.

Built with [aiokafka](https://github.com/aio-libs/aiokafka), asyncpg, and Pydantic.

## Architecture

```
producer.py ──► Kafka topic "transactions" ──► consumer.py ──► PostgreSQL (transactions)
                                                    │
                                                    └─(invalid or failed after retries)─► Kafka topic "transactions.dlq"
```

## Design decisions

- **Idempotent writes:** `event_id` is the primary key and inserts use `ON CONFLICT DO NOTHING`,
  so redelivered messages never create duplicate rows.
- **At-least-once delivery:** offsets are committed only after a message is stored or sent to the DLQ.
- **Retries with exponential backoff** for transient database errors.
- **Dead-letter topic:** invalid messages and exhausted retries are wrapped with the error,
  original topic, and offset so they can be inspected or replayed.
- **Testable core:** parsing, retry, and DLQ logic live in `processing.py` with no Kafka or DB imports.

## Quickstart

Requires Python 3.11+ and Docker.

```bash
docker compose up -d                  # Kafka (KRaft) + PostgreSQL
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

Run the consumer in one terminal:

```bash
python -m pipeline.consumer
```

Send test messages from a second terminal (about 5% are intentionally invalid):

```bash
python -m pipeline.producer --count 200 --bad-rate 0.05
```

Check the results:

```bash
docker compose exec postgres psql -U pipeline -c "SELECT count(*) FROM transactions;"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic transactions.dlq --from-beginning --max-messages 5
```
## Example output

Consumer log (valid events stored, an invalid one sent to the dead-letter topic):

```
INFO event 33f69cd1-844c-4221-993a-6f6c4e104804 stored
INFO event 35a0b76b-6c34-4dc4-9512-978a68ccfc0f stored
WARNING sending offset 32 to DLQ: 3 validation errors for TransactionEvent
INFO event 2458059f-b327-4269-901f-034408357a62 stored
```

In a run of 200 messages, 9 failed validation and went to `transactions.dlq`.
Startup errors such as "Topic transactions not found" are expected on the first run,
because Kafka creates the topic on first use and the client retries.

## Tests

```bash
pytest -q
```

## Configuration

Environment variables: `KAFKA_BOOTSTRAP_SERVERS`, `EVENTS_TOPIC`, `DLQ_TOPIC`,
`CONSUMER_GROUP`, `DATABASE_URL`, `MAX_ATTEMPTS`, `BASE_DELAY_SECONDS`.

## Testing

- Unit tests (`pytest`) cover validation, retry/backoff, and dead-letter payloads, and run in CI on every push.
- The full pipeline (Kafka, PostgreSQL, producer, consumer) was run manually with Docker Compose. There is no automated integration test yet.

  ## Roadmap
  
- [ ] Prometheus metrics (messages processed, DLQ count, processing latency) and a Grafana dashboard
- [ ] Redis cache for hot account lookups
- [ ] Load test with Locust or k6, and publish the measured results here
- [ ] Integration test using Testcontainers

## License

MIT
