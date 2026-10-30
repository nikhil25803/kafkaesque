# API Endpoints

All routes use JSON and the envelope described in the
[API overview](overview.md). Except where noted, they require a valid access
token. Path values must be URL encoded.

## Discovery

| Method | Path | Query Params | Description |
| --- | --- | --- | --- |
| `GET` | `/` | — | Public service name and API, health, and UI links. |

## Authentication

| Method | Path | Query Params | Description |
| --- | --- | --- | --- |
| `POST` | `/api/v1/auth/login` | — | Public credential login and token issuance. |
| `POST` | `/api/v1/auth/refresh` | — | Public refresh-token rotation. |
| `POST` | `/api/v1/auth/logout` | — | Revoke the current session. |
| `GET` | `/api/v1/auth/me` | — | Current identity, role, and permissions. |

See [Authentication](authentication.md) for token and CSRF behavior.

## Users

| Method | Path | Query Params | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/users` | `page`, `page_size` | List accounts with `user:account:view`. |
| `POST` | `/api/v1/users` | — | Create an account with `user:account:create`. |
| `GET` | `/api/v1/users/{id}` | — | Get an account with `user:account:view`. |
| `PATCH` | `/api/v1/users/{id}` | — | Edit an account with `user:account:edit`. |
| `DELETE` | `/api/v1/users/{id}` | — | Permanently delete an account; Super Admin only. |

Create request:

```json
{
  "name": "Jane Doe",
  "username": "jane",
  "email": "jane@example.com",
  "password": "<policy-compliant-password>",
  "role": "user"
}
```

Role defaults to `user`. An email-style username must equal the normalized
email. Requests are limited to 1 MiB, reject unknown fields, and contain one
JSON object. Responses never include password hashes.

Patch accepts optional `name`, `username`, `email`, and `role` fields. Scope and
role restrictions are described in [Authentication](authentication.md).

## Roles

| Method | Path | Query Params | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/roles` | — | List system and custom roles; Super Admin only. |
| `POST` | `/api/v1/roles` | — | Create a custom role; Super Admin only. |
| `GET` | `/api/v1/roles/{id}` | — | Get one role and its permissions; Super Admin only. |
| `PATCH` | `/api/v1/roles/{id}` | — | Rename a custom role or replace grants; Super Admin only. |
| `DELETE` | `/api/v1/roles/{id}` | — | Delete an unused custom role; Super Admin only. |

Create and patch body:

```json
{
  "name": "support_operator",
  "permissions": ["user:account:view"]
}
```

## Metadata

| Method | Path | Query Params | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/metadata/connection` | — | Connection latency, protocol, SASL mechanism, mTLS, and status. |
| `GET` | `/api/v1/metadata/cluster` | — | Cluster ID, controller, and broker/topic/partition totals. |

## Health

| Method | Path | Query Params | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/health` | — | Public process uptime without Kafka access. |
| `GET` | `/api/v1/health/cluster` | — | Authenticated live cluster, partition, and consumer health. |

`HEALTHY` and `DEGRADED` both return `200`. Collection errors return `502` or
`504`; partial health is not returned.

## Consumers

| Method | Path | Query Params | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/consumers/groups` | `q`, `state`, `page`, `page_size` | Group name, topics, members, normalized state, and total lag. |
| `GET` | `/api/v1/consumers/groups/{group}` | — | Raw state, lag summary, members, assignments, and topic lag. |
| `GET` | `/api/v1/consumers/groups/{group}/topics/{topic}/partitions` | `page`, `page_size` | Committed offset, log-end offset, and partition lag. |

List state accepts `stable`, `rebalancing`, `empty`, `dead`, and `unknown`.
Classic and modern transition states are grouped as `Rebalancing`; detail
retains Kafka's raw state.

## Topics

| Method | Path | Query Params | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/topics` | `q`, `level`, `page`, `page_size` | Topic name, level, message rates, partitions, and replication factor. |
| `GET` | `/api/v1/topics/{topic}` | — | One topic summary, including message rates. |
| `GET` | `/api/v1/topics/{topic}/partitions` | `status`, `page`, `page_size` | Leader, replicas, ISR, and partition health. |

Topic level accepts `internal` or `external`. Partition status accepts
`healthy` or `degraded`.

`messages_per_second`, `messages_per_minute`, and `messages_per_hour` are
derived from hourly log-end-offset snapshots. They are `null` during the first
hour after startup and whenever a topic's partition set or offsets reset.

## Brokers

| Method | Path | Query Params | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/brokers` | `q`, `page`, `page_size` | Broker ID, advertised/connect addresses, and rack. |

Broker search matches ID, advertised address, connect address, and rack.
