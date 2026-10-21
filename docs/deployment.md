# Deployment

Kafkaesque server mode needs three external capabilities:

- Network access to every Kafka broker, coordinator, and partition leader.
- PostgreSQL 18 or newer for accounts, roles, and sessions.
- HTTPS in front of the web UI when secure cookies are enabled.

Use a managed PostgreSQL service in production when possible. The examples
below include PostgreSQL to provide a complete evaluation environment.

## Docker Compose

Create `.env` with values that are not committed:

```dotenv
KAFKAESQUE_IMAGE=ghcr.io/nikhil25803/kafkaesque:<release-tag>
POSTGRES_PASSWORD=<database-password>
KAFKAESQUE_DATABASE_URL=postgres://kafkaesque:<url-encoded-database-password>@postgres:5432/kafkaesque?sslmode=disable
KAFKAESQUE_SUPER_ADMIN_PASSWORD=<policy-compliant-password>
KAFKAESQUE_KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092
```

Use `sslmode=disable` only for this private Compose network. Managed production
PostgreSQL URLs should enforce TLS.

Create `compose.yaml`:

```yaml
services:
  postgres:
    image: postgres:18
    restart: unless-stopped
    environment:
      POSTGRES_DB: kafkaesque
      POSTGRES_USER: kafkaesque
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?set POSTGRES_PASSWORD}
    volumes:
      - kafkaesque-postgres:/var/lib/postgresql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U kafkaesque -d kafkaesque"]
      interval: 5s
      timeout: 3s
      retries: 20

  kafkaesque:
    image: ${KAFKAESQUE_IMAGE:?set KAFKAESQUE_IMAGE}
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
    ports:
      - "8085:8085"
    environment:
      KAFKAESQUE_KAFKA_BOOTSTRAP_SERVERS: ${KAFKAESQUE_KAFKA_BOOTSTRAP_SERVERS:?set Kafka brokers}
      KAFKAESQUE_DATABASE_URL: ${KAFKAESQUE_DATABASE_URL:?set database URL}
      KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_USERNAME: super_admin
      KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_EMAIL: super-admin@example.com
      KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_PASSWORD: ${KAFKAESQUE_SUPER_ADMIN_PASSWORD:?set Super Admin password}
      KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_RECOVERY: verify
      KAFKAESQUE_AUTHENTICATION_COOKIE_SECURE: "false"
      KAFKAESQUE_API_HOST: 0.0.0.0
      KAFKAESQUE_API_PORT: "8085"

volumes:
  kafkaesque-postgres:
```

Start and verify it:

```sh
docker compose config
docker compose up -d
curl --fail http://127.0.0.1:8085/api/v1/health
```

The example disables secure cookies for local HTTP. Put Kafkaesque behind HTTPS
and set `KAFKAESQUE_AUTHENTICATION_COOKIE_SECURE=true` in production.

## Kubernetes

The following evaluation manifest uses a persistent PostgreSQL 18 StatefulSet.
Replace every placeholder and pin the image to an immutable digest before use.
For production, prefer managed PostgreSQL and an external secret provider.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafkaesque-secrets
type: Opaque
stringData:
  postgres-password: <database-password>
  database-url: postgres://kafkaesque:<url-encoded-database-password>@kafkaesque-postgres:5432/kafkaesque?sslmode=disable
  super-admin-password: <policy-compliant-password>
---
apiVersion: v1
kind: Service
metadata:
  name: kafkaesque-postgres
spec:
  selector:
    app: kafkaesque-postgres
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafkaesque-postgres
spec:
  serviceName: kafkaesque-postgres
  replicas: 1
  selector:
    matchLabels:
      app: kafkaesque-postgres
  template:
    metadata:
      labels:
        app: kafkaesque-postgres
    spec:
      containers:
        - name: postgres
          image: postgres:18
          ports:
            - containerPort: 5432
          env:
            - name: POSTGRES_DB
              value: kafkaesque
            - name: POSTGRES_USER
              value: kafkaesque
            - name: POSTGRES_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: kafkaesque-secrets
                  key: postgres-password
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "kafkaesque", "-d", "kafkaesque"]
            initialDelaySeconds: 5
            periodSeconds: 5
          volumeMounts:
            - name: data
              mountPath: /var/lib/postgresql
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 10Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafkaesque
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafkaesque
  template:
    metadata:
      labels:
        app: kafkaesque
    spec:
      securityContext:
        runAsNonRoot: true
      containers:
        - name: kafkaesque
          image: ghcr.io/nikhil25803/kafkaesque:<release-tag>
          ports:
            - name: http
              containerPort: 8085
          env:
            - name: KAFKAESQUE_KAFKA_BOOTSTRAP_SERVERS
              value: kafka-1.kafka.svc.cluster.local:9092,kafka-2.kafka.svc.cluster.local:9092
            - name: KAFKAESQUE_DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: kafkaesque-secrets
                  key: database-url
            - name: KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_USERNAME
              value: super_admin
            - name: KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_EMAIL
              value: super-admin@example.com
            - name: KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: kafkaesque-secrets
                  key: super-admin-password
            - name: KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_RECOVERY
              value: verify
            - name: KAFKAESQUE_AUTHENTICATION_COOKIE_SECURE
              value: "true"
            - name: KAFKAESQUE_API_HOST
              value: 0.0.0.0
          readinessProbe:
            httpGet:
              path: /api/v1/health
              port: http
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /api/v1/health
              port: http
            initialDelaySeconds: 10
            periodSeconds: 20
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
---
apiVersion: v1
kind: Service
metadata:
  name: kafkaesque
spec:
  selector:
    app: kafkaesque
  ports:
    - name: http
      port: 8085
      targetPort: http
```

Expose the Service through an HTTPS ingress. The process health endpoint does
not test Kafka or PostgreSQL. Monitor the authenticated
`GET /api/v1/metadata/connection` endpoint separately when Kafka availability
must be part of operational monitoring.

## Kafka TLS and mTLS files

Mount CA, client certificate, and key files read-only, then supply absolute
paths:

```text
KAFKAESQUE_KAFKA_SECURITY_TLS_ENABLED=true
KAFKAESQUE_KAFKA_SECURITY_TLS_CA_FILE=/etc/kafka/tls/ca.pem
KAFKAESQUE_KAFKA_SECURITY_TLS_CLIENT_CERT_FILE=/etc/kafka/tls/client.pem
KAFKAESQUE_KAFKA_SECURITY_TLS_CLIENT_KEY_FILE=/etc/kafka/tls/client-key.pem
```

Use a Kubernetes Secret volume or an external secrets CSI provider. Restrict
key-file permissions to the Kafkaesque container user.

## Scaling

Multiple Kafkaesque replicas may share PostgreSQL. Migration and bootstrap
operations use advisory locks, while sessions and permissions are stored in
PostgreSQL. Login throttling is in-process and therefore per replica; enforce
an additional rate limit at the ingress for public deployments.

Kafka and health collection is performed on request. Consumer-group lag is
currently collected sequentially, so dashboard and health latency grows with
the number of groups.

## Backups

Back up PostgreSQL before upgrades or recovery changes. The database contains
accounts, role assignments, password hashes, and sessions, but not Kafka
messages or historical Kafka metrics.

## Upgrades

1. Read [CHANGELOG.md](../CHANGELOG.md) for breaking configuration or schema changes.
2. Back up PostgreSQL.
3. Pin the new image tag or digest.
4. Keep `super_admin_recovery: verify` for normal upgrades.
5. Roll out the new version and wait for `/api/v1/health`.
6. Sign in and verify Kafka connectivity and account permissions.

Kafkaesque applies embedded migrations at startup. Do not use `reinitialize`
as an upgrade mechanism: it deletes the complete `public` schema when recovery
is needed.
