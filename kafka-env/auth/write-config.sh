#!/usr/bin/env bash
set -euo pipefail

mode="${1:?authentication mode is required}"
password="${2:-secret}"
ca_file="${3:-certificates/ca.pem}"
output="${4:?output path is required}"

{
  printf 'kafka:\n'
  printf '  bootstrap_servers: [localhost:9092]\n'
  printf '  connection_timeout: 5s\n'

  case "${mode}" in
    tls)
      printf '  security:\n    tls:\n      enabled: true\n      ca_file: %s\n' "${ca_file}"
      ;;
    mtls)
      printf '  security:\n    tls:\n      enabled: true\n      ca_file: %s\n' "${ca_file}"
      printf '      client_cert_file: certificates/client.pem\n'
      printf '      client_key_file: certificates/client-key.pem\n'
      ;;
    plain)
      printf '  security:\n    sasl:\n      mechanism: PLAIN\n      username: kafkaesque\n      password: %s\n' "${password}"
      ;;
    sasl_ssl_plain)
      printf '  security:\n    tls:\n      enabled: true\n      ca_file: %s\n' "${ca_file}"
      printf '    sasl:\n      mechanism: PLAIN\n      username: kafkaesque\n      password: %s\n' "${password}"
      ;;
    scram_256|scram_512)
      mechanism="SCRAM-SHA-${mode#scram_}"
      printf '  security:\n    tls:\n      enabled: true\n      ca_file: %s\n' "${ca_file}"
      printf '    sasl:\n      mechanism: %s\n      username: kafkaesque\n      password: %s\n' "${mechanism}" "${password}"
      ;;
    tcp)
      ;;
    *)
      printf 'unsupported authentication mode: %s\n' "${mode}" >&2
      exit 1
      ;;
  esac
} >"${output}"
