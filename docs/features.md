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
charts, or predict future lag.

## Slack alerts

- Persist Slack subscriptions for average or maximum partition lag.
- Evaluate unique consumer/topic targets every minute.
- Send one firing notification per breach and an optional immediate recovery notice.
- Include partition and active-member details in native cards or multiline messages.
- Encrypt Slack webhook URLs at rest with an operator-managed AES key.

Kafkaesque accepts both Slack webhook types and selects the payload automatically:

- `/services/` [Incoming Webhooks](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/)
  receive fallback text plus a native Block Kit card.
- `/triggers/` [Workflow Builder webhooks](https://slack.com/help/articles/360041352714-Build-a-workflow--Create-a-workflow-that-starts-outside-of-Slack)
  receive a flat `message` input containing formatted multiline text.

For Workflow Builder, create a text input named `message`, insert it into a
“Send a message” step, publish the workflow, and paste its URL into Kafkaesque.

Kafkaesque sends a flat payload whose `message` value contains real line breaks:

```json
{
  "message": "🔴 Kafkaesque Alert Update: FIRED\nTopic: orders\nConsumer: order-processor\nCurrent lag: 18"
}
```

Workflow Builder webhooks may require a paid Slack plan. Custom payload
templates remain unsupported.

## Interfaces

- CLI text output for interactive inspection.
- CLI JSON output for scripts and automation.
- Responsive server-rendered web UI with HTMX-enhanced filtering and refresh.
- Versioned JSON API under `/api/v1`.
- Public process health check at `/api/v1/health`.

## Security and accounts

- PostgreSQL-backed users, roles, opaque sessions, and CSRF protection.
- Argon2id password hashing.
- Built-in Super Admin, Admin, and User roles plus custom account/alert-management roles.
- TLS, mTLS, SASL PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512 for Kafka.

Every authenticated role can inspect Kafka. RBAC controls user, role, and alert
subscription management.

## Distribution

- Checksummed Linux, macOS, and Windows release archives.
- Linux container images for AMD64 and ARM64.
- Embedded templates and assets with no Node.js or CDN runtime dependency.

## Not supported

- Producing, modifying, or deleting Kafka data and configuration.
- OAuth/OIDC, Kerberos/GSSAPI, AWS MSK IAM, or provider-specific Kafka auth.
- MFA, SSO, password reset, or external identity providers.
- Generic webhooks, Slack OAuth installation, custom payloads, or alert history.
- Historical Kafka metrics or message browsing.
- Caching or stale-data fallback when Kafka is unavailable.
- SQLite; server mode requires PostgreSQL 18 or newer.

For operational boundaries and deployment risks, read
[Runtime security](security.md) and [Architecture](architecture.md).
