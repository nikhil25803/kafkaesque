# Kafkaesque Demo Stack

This local-only stack builds Kafkaesque from the current checkout and starts a
three-broker Kafka cluster, deterministic fixture data, continuous producer and
consumer activity, and PostgreSQL 18.

Docker should have roughly 4 GB of memory available.

## Start

From the repository root, create the ignored environment file:

```sh
cp dev/demo/.env.example dev/demo/.env
```

Set a policy-compliant Super Admin password in `dev/demo/.env`. Generate the
alert-encryption key and paste its output into the same file:

```sh
openssl rand -base64 32
```

Keep both values stable while retaining demo volumes. Changing either value can
prevent startup or make stored Slack webhook URLs unreadable. Then start the
stack:

```sh
make demo-up
```

Open <http://127.0.0.1:8085/ui> and sign in with:

| Field | Demo value |
| --- | --- |
| Username | `demo_captain` |
| Password | Value from `dev/demo/.env` |

These local values are intentionally disposable. Never commit `dev/demo/.env`
or reuse its values outside this localhost demo.

Set a different host port when `8085` is occupied:

```sh
KAFKAESQUE_DEMO_PORT=18085 make demo-up
```

Use a different environment file when needed:

```sh
make demo-up DEMO_ENV_FILE=/absolute/path/to/demo.env
```

Kafka and PostgreSQL are reachable only inside the Compose network. The UI is
the only host-exposed service.

## Demo data

The stack creates:

- Three Kafka brokers with replication factor `3` and minimum ISR `2`.
- Five topics: `orders`, `payments`, `inventory`, `notifications`, and
  `audit-events`.
- Consumer groups including `order-processor`, `payment-worker`,
  `inventory-sync`, and `notification-dispatcher`.
- Continuous producers and consumers so topic and group pages show activity.

Topic message rates need two hourly snapshots, so they initially display
`Collecting…`.

## Accounts and permissions

Only the disposable Super Admin is bootstrapped. To create a restricted demo
login, open **Settings → Users**, add an account, and assign the built-in
`user` role. That role can inspect Kafka and view permitted settings data but
cannot create, edit, or delete users, roles, or alerts.

## Operate and reset

```sh
make demo-status
make demo-logs
make demo-down
```

`demo-down` preserves Kafka and PostgreSQL volumes. Start the stack again with
`make demo-up`.

To remove all demo data and recreate the seeded environment:

```sh
make demo-reset
```

The demo is separate from the existing `dev/kafka` and `dev/postgres`
fixtures and does not reuse their containers, networks, or volumes.
