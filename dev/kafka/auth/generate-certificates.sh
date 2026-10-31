#!/usr/bin/env bash
set -euo pipefail

output_dir="${1:?certificate output directory is required}"
mkdir -p "${output_dir}"

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -subj "/CN=Kafkaesque Test CA" \
  -keyout "${output_dir}/ca-key.pem" -out "${output_dir}/ca.pem"

openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
  -keyout "${output_dir}/server-key.pem" -out "${output_dir}/server.csr"
openssl x509 -req -days 1 -in "${output_dir}/server.csr" \
  -CA "${output_dir}/ca.pem" -CAkey "${output_dir}/ca-key.pem" -CAcreateserial \
  -extfile <(printf 'subjectAltName=DNS:localhost,DNS:broker,IP:127.0.0.1') \
  -out "${output_dir}/server.pem"

openssl req -newkey rsa:2048 -nodes -subj "/CN=kafkaesque" \
  -keyout "${output_dir}/client-key.pem" -out "${output_dir}/client.csr"
openssl x509 -req -days 1 -in "${output_dir}/client.csr" \
  -CA "${output_dir}/ca.pem" -CAkey "${output_dir}/ca-key.pem" -CAcreateserial \
  -out "${output_dir}/client.pem"

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -subj "/CN=Untrusted Test CA" \
  -keyout "${output_dir}/invalid-ca-key.pem" -out "${output_dir}/invalid-ca.pem"

openssl pkcs12 -export -name broker \
  -in "${output_dir}/server.pem" -inkey "${output_dir}/server-key.pem" \
  -certfile "${output_dir}/ca.pem" -password pass:changeit \
  -out "${output_dir}/server.p12"
keytool -importcert -noprompt -alias kafkaesque-ca \
  -file "${output_dir}/ca.pem" -keystore "${output_dir}/truststore.p12" \
  -storetype PKCS12 -storepass changeit

chmod 0644 "${output_dir}"/*
