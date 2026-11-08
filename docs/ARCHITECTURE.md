# Architecture notes

## Load

* 6M movements a day is about 70 per second on average. We size for 1,000/s sustained for a few minutes.
* 80k users, mostly reads. Tokens are checked locally against cached Keycloak keys, so there's no Keycloak call per
  request.
* The `stock_movements` ledger grows by about 180M rows a month, so it's partitioned by month.

## Write path

One write is one Postgres transaction: claim the idempotency key, lock the row, update it, add a movement, add an outbox
row, save the result. At 1,000/s that's around 6k statements/s, which one tuned Postgres handles fine.

Hot items (a flash sale on one SKU in one store) queue on their row lock, roughly 200 to 500 writes/s for that one row.
After 2 seconds of waiting the API answers `503 STOCK_BUSY` and the client retries with the same `Idempotency-Key`. Rows
are always locked in the same order, so batches can't deadlock.

Connections: 20 per pod, 10 pods max, 200 total (Postgres allows 300). Extra requests wait without blocking Netty
threads. Past 512 in-flight requests a pod sheds load with 503.

## One Kafka broker

Throughput is not the issue (about 0.6 MB/s at peak). Availability is: if the broker goes down, nothing else takes over.
So the API never waits on Kafka.

| What can go wrong           | What happens                                                                                                                    |
|-----------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| Broker down                 | Events are saved in the outbox table in the same transaction as the stock change. Writes keep working; readiness ignores Kafka. |
| Broker down for long        | The relay backs off and catches up later. Watch `outbox.pending` and `outbox.oldest_age_ms`.                                    |
| Same event sent twice       | Idempotent producer (`enable.idempotence=true`, `acks=all`), and each event has a fixed `eventId`.                              |
| Events out of order         | Key is `sku                                                                                                                     |location`, so one item stays on one partition. `stockVersion` only goes up. |
| Bad event                   | Moves to the `dead_letters` table. Replay with `POST /api/v1/admin/dead-letters/{id}/replay`.                                   |
| Incoming sync message fails | Retried, then dead-lettered. If Postgres itself is down, the consumer pauses and doesn't skip anything.                         |
| Broker disk lost            | Events can be rebuilt from `stock_movements`, which has the same ids and versions.                                              |

Between the outbox poller and the Kafka publisher there's a bounded channel. If Kafka slows down, the poller waits
instead of piling up rows.

## Memory per pod

1.5 GiB container. About 1 GiB heap (Generational ZGC), 32 MiB Kafka producer buffer, about 2 MiB of in-flight outbox
batches, the rest for Netty buffers, metaspace and threads.

## Security

* Teammates log in with passkeys (WebAuthn). New users enroll one on first login. `test.sh` does this with a software
  passkey.
* Partners and services use client credentials. The API checks signature, issuer, audience (`retail-stock-api`) and
  expiry.
* Roles `stock:read|write|reserve|sync|admin` are checked per route.
* Store access comes from the Keycloak group `/locations/<ID>` (or `/locations/ALL`). It's checked on the path, then
  again in the domain service for stores in the request body.