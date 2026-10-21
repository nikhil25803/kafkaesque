# Troubleshooting

Start with configuration validation, then test connectivity:

```sh
kafkaesque --check config
kafkaesque --check
```

For server deployments, also verify the process endpoint:

```http
GET /api/v1/health
```

## Common failures

| Symptom | Likely cause | Resolution |
| --- | --- | --- |
| `connection refused` | Wrong host/port, broker down, listener not exposed, or network policy. | Verify bootstrap addresses from the Kafkaesque network namespace and check broker listeners. |
| `connection timeout` / exit `3` | Firewall, unreachable route, TLS handshake stall, or broker latency. | Test TCP reachability and review `kafka.connection_timeout`. Do not only increase the timeout without finding the network cause. |
| SASL failure / exit `2` | Wrong mechanism, username, password, or broker listener. | Confirm broker SASL configuration and use PLAIN or supported SCRAM values exactly. |
| TLS or x509 failure / exit `2` | Untrusted CA, hostname mismatch, expired certificate, or invalid client certificate. | Verify CA chain, `server_name`, certificate dates, and the mTLS pair. |
| Kafka authorization failure / exit `2` | Principal cannot read metadata, groups, or offsets. | Grant the Kafka principal the read and describe permissions needed for the requested inspection. |
| Bootstrap works but group/topic calls fail | Kafka advertises internal broker, coordinator, or leader addresses. | Correct `advertised.listeners` or configure exact broker address overrides. |
| Cluster health request fails | Metadata, coordinator, group description, or offset collection failed. | Inspect the returned upstream error and test affected groups individually; Kafkaesque does not return partial health. |
| Consumer group is `Rebalancing` | Kafka is changing group membership or assignment. | Retry after the rebalance and inspect unstable members or deployments. |
| Lag is positive but marked healthy | Lag is below the configured warning threshold. | Review lag thresholds and trend the value externally if rate/history is required. |
| UI returns to login | Access session expired and refresh failed, cookies are blocked, or the session was revoked. | Confirm cookie settings, system time, PostgreSQL availability, and account state. |
| Login works over HTTP only when secure cookies are disabled | Browsers do not send `Secure` cookies over HTTP. | Use HTTPS and keep secure cookies enabled; disable them only for trusted local HTTP. |
| Server reports PostgreSQL version failure | Database is older than PostgreSQL 18. | Upgrade PostgreSQL; migrations use native UUIDv7 support. |
| Super Admin mismatch stops startup | Environment/YAML credentials differ from the stored bootstrap account. | Use `verify` for safety, `update` for one valid Super Admin, or carefully follow the recovery procedure below. |
| API returns `502` | Kafka failed or rejected the request. | Check Kafka connectivity and authorization with the CLI connection check. |
| API returns `503` | PostgreSQL authentication or account storage is unavailable. | Check database connectivity, credentials, TLS, and migrations. |
| API returns `504` | Kafka operation timed out. | Investigate Kafka/network latency and configured timeout. |

## Advertised broker addresses

Kafka clients bootstrap once, then connect to addresses returned by Kafka. A
successful bootstrap does not prove that coordinators and partition leaders are
reachable.

Preferred fix: configure correct Kafka `advertised.listeners`. If that is not
possible, map every advertised broker address to its corresponding reachable
listener:

```yaml
kafka:
  broker_address_overrides:
    "broker-0.kafka.svc.cluster.local:9092": "10.100.0.72:9094"
```

Do not map several brokers to one non-sticky load balancer.

## Super Admin recovery

Normal startup should use:

```yaml
authentication:
  super_admin_recovery: verify
```

Use `update` only when exactly one valid Super Admin exists and its configured
username, email, or password must change. Existing sessions are revoked.

Use `reinitialize` only for deliberate recovery from credential mismatch,
schema corruption, missing role assignment, or migration checksum mismatch:

1. Confirm the database is dedicated to Kafkaesque.
2. Back it up.
3. Set recovery to `reinitialize`.
4. Start Kafkaesque once and verify the fresh Super Admin.
5. Return recovery to `verify` and restart.

The reset atomically drops and recreates the complete `public` schema. It
permanently deletes all data and unrelated objects stored there.

## Local development diagnostics

```sh
make kafka-logs
make kafka-simulator-logs
make postgres-logs
```

Reset only disposable fixtures:

```sh
make kafka-reset
make postgres-reset
```

`postgres-reset` removes the local development volume. Authentication
integration tests use the separate `_test` database through
`make postgres-test`.

## Before sharing diagnostics

Remove credentials, tokens, cookies, database URLs, private certificate paths,
internal hostnames, and customer topic/group names. Kafkaesque avoids logging
known secrets, but operators should still review captured output.
