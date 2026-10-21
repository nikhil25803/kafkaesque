# Contributing to Kafkaesque

Thanks for helping improve Kafkaesque. Kafka inspection must remain read-only;
account and session management may write only to PostgreSQL.

## Before you start

Read the [architecture](docs/architecture.md) for component boundaries and the
[development guide](docs/development.md) for local fixtures, commands, and
integration tests.

For substantial behavior or API changes, open an issue or discussion first so
the intended scope is clear.

## Development workflow

1. Fork the repository and create a focused branch.
2. Make the smallest complete change.
3. Add or update tests for changed behavior.
4. Update the relevant documentation and the `Unreleased` section of
   [CHANGELOG.md](CHANGELOG.md) for user-facing changes.
5. Run:

   ```sh
   make check
   go test -race ./...
   git diff --check
   ```

6. Run the relevant Kafka, PostgreSQL, Docker, or release integration checks
   described in the [development guide](docs/development.md).

## Code standards

- Prefer readable, conventional Go over compressed code.
- Run `gofmt`; `make check` verifies formatting.
- Keep exported and security-sensitive behavior documented in plain language.
- Preserve request cancellation, error handling, secret redaction, and
  accessibility behavior.
- Do not add credentials, tokens, certificates, database URLs, or realistic
  secrets to source, tests, examples, logs, or image layers.
- Use placeholders in documentation and generate test credentials at runtime.
- Keep API response envelopes, CLI output contracts, and migrations compatible
  unless the change explicitly documents a breaking change.

## Pull requests

A pull request should:

- Explain the problem and the chosen solution.
- Stay focused; unrelated cleanup belongs in another change.
- List the checks that were run.
- Include screenshots for visible UI changes.
- Call out migrations, breaking changes, security effects, and operational
  risks.
- Avoid rewriting shared branch history after review has started unless needed
  to remove sensitive material.

Report vulnerabilities privately according to [SECURITY.md](SECURITY.md), not
through a public issue or pull request.
