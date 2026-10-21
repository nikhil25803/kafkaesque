# Configure Kafka TLS and SASL

Kafkaesque uses one shared Kafka transport for metadata, coordinators,
partition leaders, consumer operations, checks, API handlers, and UI handlers.
Security configuration therefore applies consistently to every Kafka request.

## Effective protocols

| TLS | SASL mechanism | Protocol |
| --- | --- | --- |
| No | None | `PLAINTEXT` |
| Yes | None | `SSL` |
| No | PLAIN or SCRAM | `SASL_PLAINTEXT` |
| Yes | PLAIN or SCRAM | `SASL_SSL` |

Do not use SASL PLAIN without TLS on an untrusted network; the credentials are
not protected by the SASL mechanism itself.

## TLS with system roots

```yaml
kafka:
  bootstrap_servers:
    - kafka.example.com:9093
  security:
    tls:
      enabled: true
      server_name: kafka.example.com
```

Omit `server_name` when the bootstrap and advertised hostnames already match
the certificate. Kafkaesque does not provide an insecure skip-verification
option.

## TLS with a custom CA

```yaml
kafka:
  security:
    tls:
      enabled: true
      ca_file: certificates/ca.pem
```

Relative YAML paths are resolved from the YAML directory. Environment paths
are not rewritten.

## Mutual TLS

```yaml
kafka:
  security:
    tls:
      enabled: true
      ca_file: certificates/ca.pem
      client_cert_file: certificates/client.pem
      client_key_file: certificates/client-key.pem
```

The client certificate and key must be supplied together.

## SASL PLAIN over TLS

```yaml
kafka:
  security:
    tls:
      enabled: true
    sasl:
      mechanism: PLAIN
      username: kafkaesque
      password: <secret>
```

## SCRAM over TLS

```yaml
kafka:
  bootstrap_servers:
    - kafka-1.example.com:9093
    - kafka-2.example.com:9093
  security:
    tls:
      enabled: true
      ca_file: certificates/ca.pem
      server_name: kafka.example.com
    sasl:
      mechanism: SCRAM-SHA-256
      username: kafkaesque
      password: <secret>
```

Change the mechanism to `SCRAM-SHA-512` when required by the broker.

## Environment configuration

```sh
export KAFKAESQUE_KAFKA_SECURITY_TLS_ENABLED=true
export KAFKAESQUE_KAFKA_SECURITY_TLS_CA_FILE=/etc/kafka/tls/ca.pem
export KAFKAESQUE_KAFKA_SECURITY_SASL_MECHANISM=SCRAM-SHA-256
export KAFKAESQUE_KAFKA_SECURITY_SASL_USERNAME=kafkaesque
export KAFKAESQUE_KAFKA_SECURITY_SASL_PASSWORD='<secret>'
```

Use a secret manager, container secret, or Kubernetes Secret for passwords and
private keys. Never include them in image layers or command history.

## Test the connection

```sh
kafkaesque --config kafkaesque.yaml --check
```

Security and Kafka authorization failures exit with `2`; timeouts exit with
`3`. Output is sanitized but operating-system or Kafka library errors can still
contain infrastructure names, so review logs before sharing them publicly.

## Unsupported mechanisms

OAuth/OIDC, OAUTHBEARER, Kerberos/GSSAPI, AWS MSK IAM, and other provider IAM
mechanisms are not currently implemented.
