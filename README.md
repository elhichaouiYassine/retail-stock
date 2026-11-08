# Retail Stock

Backend for real-time store inventory. API only, there's no UI.
Sized for about 6M stock movements a day, with bursts above 1,000 per second.

Kotlin + Ktor (coroutines) on Java 21, PostgreSQL, Kafka (one broker, Strimzi), Keycloak (passkeys and OAuth2),
Kubernetes.

## Run it

You need Docker, JDK 21 and Maven. The test scripts also need `curl`, `jq` and `openssl`.

```bash
docker compose up -d --wait                                  # Postgres :5433, Keycloak :8180, Kafka :9092
mvn -q -DskipTests package
java -jar retail-stock-api/target/retail-stock-api-all.jar   # API on :8080
./test.sh                                                    # end-to-end test
./load-test.sh                                               # load test
```

Nothing to configure. `.env` is already generated, and the app has the same values built in.
Prefer containers for everything? `docker compose --profile app up -d --build --wait`.

## Tests

`./test.sh` runs the whole thing with plain curl and the Kafka CLI:

1. waits for the API, Keycloak and Kafka
2. logs `alice` in with a **passkey**. The script acts as the authenticator: first run it enrolls a key (password used
   once), every run after that signs Keycloak's challenge, no password
3. checks 401/403, RBAC, ABAC and idempotency
4. sends adjustments through the API, waits for them on `retail.stock.events.v1`, reads them back with a consumer group
   and prints offsets and lag before and after
5. pushes stock counts with the Kafka console producer and waits until the API's consumer group has lag 0

`./load-test.sh` hammers the write endpoint, then checks every event reached Kafka:

```bash
./load-test.sh -n 10000 -c 50      # bash workers + curl, no extra tools
./load-test.sh -t wrk -d 30s       # wrk, if installed
./load-test.sh --reads -t ab       # read load with ApacheBench
./load-test.sh --hot               # one item only: shows lock contention (503 STOCK_BUSY)
```

Other scripts:

| Script                                             | What for                                        |
|----------------------------------------------------|-------------------------------------------------|
| `scripts/token.sh teammate\|partner\|service\|dev` | print a token                                   |
| `scripts/kafka-lag.sh [group] [topic]`             | offsets and consumer lag (`WATCH=2` to refresh) |
| `scripts/publish-sync.sh SKU-1 STORE-001 42`       | send one stock count to Kafka                   |

How lag is read: for each partition, `LAG = LOG-END-OFFSET - CURRENT-OFFSET`, meaning records written but not committed
by the group yet. Group lag is the sum. Details in `scripts/lib/kafka.sh`.

The software passkey keys live in `.e2e/` (git ignored). Delete that folder and the next run enrolls a new one.

## Logins

| Who                         | How                                                  | Can do                  | Stores               |
|-----------------------------|------------------------------------------------------|-------------------------|----------------------|
| `alice`                     | passkey (`./test.sh` or `scripts/token.sh teammate`) | everything, incl. admin | all                  |
| `bob`                       | passkey, password `Bob-Passkey-2026!` to enroll      | read, write, reserve    | STORE-001            |
| `partner-acme`              | client credentials                                   | read, reserve           | STORE-001, STORE-002 |
| `retail-stock-sync-service` | client credentials                                   | read, write, sync       | all                  |
| `dev.ops`                   | password grant, local only                           | everything              | all                  |

Keycloak admin: http://localhost:8180, `admin` / `admin_local_pwd`.
In Kubernetes the API only accepts teammate tokens from the passkey client `retail-stock-web`.

## API

Every `/api/v1` call needs `Authorization: Bearer <token>`. Every write needs an `Idempotency-Key` header: same key
again returns the first result, same key with a different body returns 422.

| Call                                                                                 | Permission      |
|--------------------------------------------------------------------------------------|-----------------|
| `GET /api/v1/me`                                                                     | any             |
| `GET /api/v1/stock/{sku}`                                                            | `stock:read`    |
| `GET /api/v1/stock/{sku}/locations/{loc}` and `/movements`                           | `stock:read`    |
| `POST /api/v1/stock/{sku}/locations/{loc}/adjustments` `{delta, reason, reference?}` | `stock:write`   |
| `POST /api/v1/stock/{sku}/locations/{loc}/sync` `{onHand, source, countedAt?}`       | `stock:sync`    |
| `POST /api/v1/batches/adjustments` `{lines: [...]}`, up to 500, all or nothing       | `stock:write`   |
| `POST /api/v1/reservations`, `GET/commit/release /api/v1/reservations/{id}`          | `stock:reserve` |
| `GET /api/v1/admin/dead-letters`, `POST .../{id}/replay`, `.../{id}/discard`         | `stock:admin`   |
| `GET /health/live`, `/health/ready`, `/metrics`                                      | none            |

Location checks: callers only see and touch the stores their Keycloak group allows.
Errors: `INSUFFICIENT_STOCK` 409, `ACCESS_DENIED` 403, `IDEMPOTENCY_CONFLICT` 422, `STOCK_BUSY` / `OVERLOADED` 503 with
`Retry-After`.

## Kafka topics

* `retail.stock.events.v1`, written by the API: one event per stock change, key `sku|location`. Consumers dedupe on
  `eventId` and ignore a `stockVersion` they've already seen.
* `retail.stock.sync.v1`, read by the API: absolute counts `{eventId, sku, locationId, onHand, source, countedAt?}`. Bad
  messages go to the dead-letter table.

## Layout

```
retail-stock-domain/          business rules, no frameworks
retail-stock-infrastructure/  Postgres, outbox, dead letters, Kafka
retail-stock-api/             Ktor server, auth, routes, metrics
keycloak/                     realm import
scripts/                      token, lag, sync helpers and the shared bash libs
test.sh, load-test.sh         end-to-end and load tests
docs/ARCHITECTURE.md          the why behind the design
```

Unit tests: `mvn test`.
