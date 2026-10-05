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

```bash
docker compose up -d                  # Kafka (KRaft) + PostgreSQL
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

python -m pipeline.consumer &         # start the consumer
python -m pipeline.producer --count 200 --bad-rate 0.05
```

Check the results:

```bash
docker compose exec postgres psql -U pipeline -c "SELECT count(*) FROM transactions;"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic transactions.dlq --from-beginning --max-messages 5
```

## Tests

```bash
pytest -q
```

## Configuration

Environment variables: `KAFKA_BOOTSTRAP_SERVERS`, `EVENTS_TOPIC`, `DLQ_TOPIC`,
`CONSUMER_GROUP`, `DATABASE_URL`, `MAX_ATTEMPTS`, `BASE_DELAY_SECONDS`.

## Roadmap

- [ ] Prometheus metrics (messages processed, DLQ count, processing latency) and a Grafana dashboard
- [ ] Redis cache for hot account lookups
- [ ] Load test with Locust or k6, and publish the measured results here
- [ ] Integration test using Testcontainers

## License

MIT
