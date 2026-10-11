# Kafkaesque HTTP API

Kafkaesque exposes live, read-only Kafka snapshots under `/api/v1`. Start it
with one authenticated Kafka connection:

```sh
kafkaesque serve --config kafkaesque.yaml --port 8085
```

The listener defaults to `0.0.0.0:8085`. Explicit `--host` and `--port` flags
override `api.host` and `api.port` from YAML. The API has no application
authentication yet, so restrict the listener on untrusted networks.

## Response format

Successful responses use a data envelope. Non-list endpoints return an empty
metadata object:

```json
{"success":true,"status":200,"data":{"status":"CONNECTED","latency_ms":42,"protocol":"SASL_SSL","sasl_mechanism":"SCRAM-SHA-256","mtls":false},"meta":{}}
```

List endpoints include pagination metadata:

```json
{"success":true,"status":200,"data":[],"meta":{"page":1,"page_size":25,"total_items":0,"total_pages":0}}
```

Errors never include Kafka credentials, certificate paths, or raw security
errors:

```json
{"success":false,"status":400,"data":null,"error":{"code":"invalid_request","message":"page must be positive"}}
```

Every response is JSON and includes `Cache-Control: no-store`.
Every Kafka-backed endpoint first verifies connectivity with one complete
metadata request and reuses that validated snapshot. Connection failures return
`502`, and timeouts return `504`.

## Metadata

| Method | Path | Query Params | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/metadata/connection` | — | Connection status, latency in milliseconds, protocol, SASL mechanism, and mTLS indicator. |
| `GET` | `/api/v1/metadata/cluster` | — | Cluster ID, controller ID, and broker, topic, and partition totals. |

```http
GET /api/v1/metadata/cluster
```

## Health

| Method | Path | Query Params | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/health` | — | API process uptime without checking Kafka connectivity. |
| `GET` | `/api/v1/health/cluster` | — | Current `HEALTHY` or `DEGRADED` status and cluster, partition, and consumer-group counts. |

Degraded health is a successful snapshot and returns HTTP `200`.
The uptime endpoint is not protected by Kafka connection middleware.

## Consumers

| Method | Path | Query Params | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/consumers/groups` | `q`, `state`, `page`, `page_size` | Group name, topic names, member count, state, and current total lag. |
| `GET` | `/api/v1/consumers/groups/{group}` | — | Current group state, lag summary, members, assignments, and topic lag. |
| `GET` | `/api/v1/consumers/groups/{group}/topics/{topic}/partitions` | `page`, `page_size` | Committed offset, log-end offset, and lag for each partition. |

```http
GET /api/v1/consumers/groups?state=stable&page_size=10
```

## Topics

| Method | Path | Query Params | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/topics` | `q`, `level`, `page`, `page_size` | Topic name, internal/external level, partition count, and replication factor. |
| `GET` | `/api/v1/topics/{topic}` | — | One topic summary. |
| `GET` | `/api/v1/topics/{topic}/partitions` | `status`, `page`, `page_size` | Partition leader, replicas, ISR, and health. |

## Brokers

| Method | Path | Query Params | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/brokers` | `q`, `page`, `page_size` | Broker ID, advertised address, effective connect address, and rack. |

Search matches broker IDs, addresses, and racks.

## Query parameters

| Parameter | Applies to | Rules |
| --- | --- | --- |
| `q` | List endpoints | Case-insensitive name or address search. |
| `page` | List and partition endpoints | Positive integer; default `1`. |
| `page_size` | List and partition endpoints | Integer from `1` through `100`; default `25`. |
| `state` | Consumer-group list | Case-insensitive exact state match. |
| `level` | Topic list | `internal` or `external`. |
| `status` | Topic partitions | `healthy` or `degraded`. |

Pages beyond the available results return an empty array with valid pagination
metadata. Results are sorted by group name, topic name, broker ID, or partition
ID.

## HTTP status codes

| Status | Meaning |
| --- | --- |
| `200` | Snapshot returned, including degraded health. |
| `400` | Invalid query parameter. |
| `404` | Requested group or topic does not exist. |
| `502` | Kafka rejected or could not complete the request. |
| `504` | Kafka request timed out. |
