# Features and Limitations

Kafkaesque is a focused Kafka inspection tool. It provides current snapshots;
it is not a historical metrics platform or a Kafka administration console.

## Cluster inspection

| Feature | What it tells you |
| --- | --- |
| Connection check | Whether bootstrap, DNS, TCP, TLS, SASL, and Kafka metadata access succeed. |
| Metadata | Cluster ID, controller, and broker/topic totals. |
| Brokers | Advertised address, effective connect address, and rack. |
| Topics | Internal/external level, hourly message throughput, partition count, and replication factor. |
| Partitions | Leader, replicas, ISR, and healthy/degraded state. |
| Cluster health | Offline and under-replicated partitions plus consumer-group health signals. |

## Consumer observability

- List consumer groups, normalized state, member count, topic count, and lag.
- Inspect active members, client IDs, hosts, and topic/partition assignments.
- Compare committed and log-end offsets for each committed partition.
- Summarize total, maximum, and average lag using configurable thresholds.
- Refresh consumer detail in the web UI or use CLI watch mode.

Kafkaesque reports current Kafka values. It does not store lag history, produce
charts, calculate message rates, or predict future lag.

## Interfaces

- CLI text output for interactive inspection.
- CLI JSON output for scripts and automation.
- Responsive server-rendered web UI with HTMX-enhanced filtering and refresh.
- Versioned JSON API under `/api/v1`.
- Public process health check at `/api/v1/health`.

## Security and accounts

- PostgreSQL-backed users, roles, opaque sessions, and CSRF protection.
- Argon2id password hashing.
- Built-in Super Admin, Admin, and User roles plus custom account-management roles.
- TLS, mTLS, SASL PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512 for Kafka.

Every authenticated role can inspect Kafka. RBAC currently controls only user
and role management.

## Distribution

- Checksummed Linux, macOS, and Windows release archives.
- Linux container images for AMD64 and ARM64.
- Embedded templates and assets with no Node.js or CDN runtime dependency.

## Not supported

- Producing, modifying, or deleting Kafka data and configuration.
- OAuth/OIDC, Kerberos/GSSAPI, AWS MSK IAM, or provider-specific Kafka auth.
- MFA, SSO, password reset, or external identity providers.
- Slack alerts, generic webhooks, or alert history.
- Historical Kafka metrics, message browsing, or message-rate calculations.
- Caching or stale-data fallback when Kafka is unavailable.
- SQLite; server mode requires PostgreSQL 18 or newer.

For operational boundaries and deployment risks, read
[Runtime security](security.md) and [Architecture](architecture.md).
