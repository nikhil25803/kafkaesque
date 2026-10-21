# HTTP API Overview

Kafkaesque exposes versioned JSON endpoints under `/api/v1`. The same server
process also serves the web UI under `/ui`.

Start it with effective Kafka, PostgreSQL, and authentication configuration:

```sh
kafkaesque serve --config kafkaesque.yaml --port 8085
```

See [Authentication](authentication.md) before calling protected endpoints and
[Endpoints](endpoints.md) for the complete route list.

## Public discovery

```http
GET /
```

```json
{
  "success": true,
  "status": 200,
  "data": {
    "name": "Kafkaesque",
    "api": "/api/v1",
    "health": "/api/v1/health",
    "ui": "/ui"
  }
}
```

## Response envelope

Successful responses always contain `success`, numeric HTTP `status`, and
`data`. `meta` is included when relevant:

```json
{
  "success": true,
  "status": 200,
  "data": [],
  "meta": {
    "page": 1,
    "page_size": 25,
    "total_items": 0,
    "total_pages": 0
  }
}
```

Errors contain `data: null` and a sanitized error:

```json
{
  "success": false,
  "status": 400,
  "data": null,
  "error": {
    "code": "invalid_request",
    "message": "page must be positive"
  }
}
```

The envelope status always matches the HTTP response. Responses use
`Content-Type: application/json` and `Cache-Control: no-store`.

## Middleware order

Registered protected routes authenticate the account before performing Kafka
work. Kafka-backed routes then fetch and validate one complete metadata
snapshot, record its latency, and reuse it in the handler. Unauthenticated
requests therefore do not contact Kafka.

Unknown routes return `404` without checking Kafka connectivity. Metadata
availability errors needed to describe offline or under-replicated partitions
remain available to health handlers; authorization and other metadata errors
fail the request.

## Pagination and filtering

List endpoints return:

- `page`: positive integer, default `1`.
- `page_size`: integer from `1` through `100`, default `25`.
- `total_items` and `total_pages`.

Pages beyond the result set return an empty array with valid metadata.

Additional filters:

| Parameter | Values | Applies to |
| --- | --- | --- |
| `q` | Case-insensitive search text | Consumer groups, topics, and brokers. |
| `state` | `stable`, `rebalancing`, `empty`, `dead`, `unknown` | Consumer groups. |
| `level` | `internal`, `external` | Topics. |
| `status` | `healthy`, `degraded` | Topic partitions. |

Results are deterministically sorted by group name, topic name, broker ID, or
partition ID.

## HTTP status codes

| Status | Meaning |
| --- | --- |
| `200` | Request succeeded, including degraded cluster health and deletion. |
| `201` | User or custom role created. |
| `400` | Invalid query, JSON body, field, or role request. |
| `401` | Authentication is missing, invalid, or expired. |
| `403` | Permission or CSRF validation failed. |
| `404` | Route, group, topic, user, or role does not exist. |
| `409` | Username, email, or role conflict; or assigned role deletion. |
| `429` | Login attempts were throttled. |
| `502` | Kafka rejected or could not complete the request. |
| `503` | PostgreSQL-backed authentication or account service is unavailable. |
| `504` | Kafka request timed out. |

Kafka credentials, certificate paths, database URLs, passwords, hashes, and
tokens are not included in API errors.
