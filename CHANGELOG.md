# Changelog

All notable changes to Kafkaesque are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- PostgreSQL-backed authentication with opaque access and refresh sessions,
  secure browser cookies, CSRF protection, and login throttling.
- User and role management with account permissions and protected API and UI
  routes.
- Consumer-lag auto refresh, local-time refresh timestamps, configurable list
  page sizes, and accessible row action menus.
- Task-oriented user, operator, API, security, architecture, and development
  documentation.

### Changed

- Configuration now uses hierarchical Kafka, lag, authentication, database,
  and API sections with matching environment-variable names.
- All authenticated roles retain Kafka observability access while account
  management follows role permissions.

## [v0.7.0] - 2026-10-12

### Added

- Multi-architecture GHCR images for Linux AMD64 and ARM64.
- Tag-triggered container publishing with versioned and `latest` tags.
- Environment-based Kafka and API configuration for container deployments.

## [v0.6.0] - 2026-10-12

### Added

- Versioned read-only HTTP APIs for Kafka metadata, health, consumers, topics,
  partitions, and brokers.
- Embedded responsive web UI for dashboards and Kafka inspection.
- Shared live collectors used by CLI, API, and UI paths.

## [v0.5.0] - 2026-10-11

### Added

- Structured JSON output for information commands.
- In-place watch mode for repeated CLI snapshots.

## [v0.4.0] - 2026-10-11

### Added

- Cluster health summaries covering metadata, partitions, consumer states, and
  lagging groups.

## [v0.3.0] - 2026-10-11

### Added

- Consumer member assignments and detailed lag summaries.
- Warning and unhealthy lag thresholds.
- Partition replica, ISR, and health context.

## [v0.2.0] - 2026-10-11

### Added

- Multi-broker Kafka connectivity with TLS, mTLS, SASL PLAIN, and SCRAM.
- Connection checks with protocol reporting, sanitized failures, and stable
  exit codes.

## [v0.1.1] - 2026-10-10

### Added

- Broker address overrides for clusters with unreachable advertised listeners.
- Version reporting through the CLI.

## [v0.1.0] - 2026-10-10

### Added

- Consumer group inspection and per-topic partition lag.
- Activity simulator for local consumer and lag testing.

## [v0.0.1] - 2026-10-09

### Added

- Initial Kafka metadata, broker, topic, partition, and consumer inspection
  commands.
- Local Kafka fixtures, CI, installation, and release foundations.

[Unreleased]: https://github.com/nikhil25803/kafkaesque/compare/v0.7.0...HEAD
[v0.7.0]: https://github.com/nikhil25803/kafkaesque/compare/v0.6.0...v0.7.0
[v0.6.0]: https://github.com/nikhil25803/kafkaesque/compare/v0.5.0...v0.6.0
[v0.5.0]: https://github.com/nikhil25803/kafkaesque/compare/v0.4.0...v0.5.0
[v0.4.0]: https://github.com/nikhil25803/kafkaesque/compare/v0.3.0...v0.4.0
[v0.3.0]: https://github.com/nikhil25803/kafkaesque/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/nikhil25803/kafkaesque/compare/v0.1.1...v0.2.0
[v0.1.1]: https://github.com/nikhil25803/kafkaesque/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/nikhil25803/kafkaesque/compare/v0.0.1...v0.1.0
[v0.0.1]: https://github.com/nikhil25803/kafkaesque/releases/tag/v0.0.1
