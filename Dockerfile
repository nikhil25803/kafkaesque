# syntax=docker/dockerfile:1

ARG GO_VERSION=1.26.5
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X github.com/nikhil25803/kafkaesque/internals.Version=${VERSION}" \
    -o /out/kafkaesque ./cmd/kafkaesque

FROM scratch

ARG VERSION=dev
ARG REVISION=unknown

LABEL org.opencontainers.image.title="Kafkaesque" \
      org.opencontainers.image.description="A lightweight, read-only Kafka cluster inspector" \
      org.opencontainers.image.source="https://github.com/nikhil25803/kafkaesque" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/kafkaesque /kafkaesque

ENV HOME=/home/nonroot \
    XDG_CONFIG_HOME=/home/nonroot/.config

USER 65532:65532
EXPOSE 8085

ENTRYPOINT ["/kafkaesque"]
CMD ["serve"]
