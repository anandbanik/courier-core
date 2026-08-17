# courier-core

A configuration-driven message-routing engine in Go. A YAML file declares
**flows**; each flow binds a **source** to a **sink**, with an optional chain of
**transforms** in between.

```
   Kafka / SQS / S3 /                                        Kafka / SQS /
   HTTP POST ─────────▶ source ──▶ transform(s) ──▶ sink ───▶ HTTP endpoint
                                          │
                                          ▼ on failure
                                   dead-letter (S3)
```

No code changes are needed to add a route — write YAML and restart. The engine
never inspects the payload, so flows can carry JSON, CSV, XML, or binary.

- **Transports**: HTTP/REST, Kafka, SQS, S3
- **Transforms**: jq expressions, Go templates, or chains of them
- **Delivery**: at-least-once, with configurable retry and S3 dead-lettering
- **Deployment**: one binary runs as a long-lived HTTP service or as an AWS
  Lambda handler, from the same config file
- **Auth**: OAuth2 client-credentials for HTTP sinks, with automatic token refresh

## Quick start

Requires Go 1.24+.

```bash
go build ./...
go test ./...
```

The `echo` flow in `configs/example.yaml` needs no brokers — it accepts an HTTP
POST and forwards it to a downstream URL. Start any listener on port 9000, then:

```bash
EIP_CONFIG=configs/example.yaml go run ./cmd/service

# in another shell
curl -XPOST localhost:8080/api/v1/echo -d '{"hello":"world"}'   # send a payload
curl localhost:8080/api/v1/echo                                  # flow status
curl localhost:8080/healthz
```

For flows that need Kafka or AWS, bring up local dependencies:

```bash
docker compose -f deployments/docker-compose.yaml up -d
```

Set `EIP_DEBUG=true` for debug-level logging. Logs are JSON via `log/slog`.

## Configuration

`configs/example.yaml` is the reference for every supported shape. Flows are
declared under two keys — `flows` and `routes` — which the engine treats
**identically**; the split is organizational convention only (`flows` for
API-sourced endpoints, `routes` for pull-source-to-sink pipes).

```yaml
server:
  addr: ":8080"          # default :8080
  apiPrefix: "/api"      # default /api
  shutdownTimeout: 15s

routes:
  - name: ordersToApi
    version: v1

    source:
      transport: sqs
      sqs:
        queueUrl: "https://sqs.us-east-1.amazonaws.com/123456789012/orders"
        region: us-east-1
        waitTimeSeconds: 20     # long poll; default 20
        maxMessages: 10         # default 10

    # `transform:` for one, `transforms:` for a chain — not both.
    transforms:
      - type: jq
        params:
          expression: "{id: .orderId, qty: .quantity}"

    sink:
      transport: api
      api:
        url: "https://downstream.example.com/orders"
        method: POST            # default POST
        timeoutSeconds: 30      # default 30
        headers:
          content-type: application/json
        oauth:                  # optional client-credentials
          tokenUrl: "https://auth.example.com/oauth2/token"
          clientId: my-client
          clientSecret: ${ORDERS_CLIENT_SECRET}

    retry:
      maxAttempts: 3            # default 1 — i.e. no retrying
      initialInterval: 100ms
      maxInterval: 5s
      multiplier: 2.0

    deadLetter:
      stopOnFailure: false
      s3:
        bucket: my-dlq-bucket
        region: us-east-1
        prefix: failures/
        dateLayout: "2006/01/02"   # optional date-partitioned keys
```

Each flow is served at `{apiPrefix}/{version}/{name}` — here,
`/api/v1/ordersToApi`. The pair `version/name` must be unique.

**Environment placeholders** are expanded over the raw file before YAML parsing,
so they work anywhere:

| Form | Behaviour |
| --- | --- |
| `${VAR}` | Required. Unset is a startup error. |
| `${VAR:-default}` | Falls back to `default`. |

An env value always beats an inline default, and *all* missing variables are
reported in a single error. Config is read **once at startup** — restart after
editing — and any validation error fails the process before it binds a port.

## Transports

| Transport | Source | Sink | Style | Notes |
| --- | :---: | :---: | --- | --- |
| `api` | ✅ | ✅ | push | Source is an HTTP endpoint; sink returns the downstream response to the caller. Optional OAuth2. |
| `kafka` | ✅ | ✅ | pull | Consumer groups; `requiredAcks` of `none`/`one`/`all`. Key and headers propagate. |
| `sqs` | ✅ | ✅ | pull | Long polling; visibility timeout honoured. The sink sends the body only. |
| `s3` | ✅ | ❌ | pull | Polls a prefix for `.json` objects, moves each to `processedPrefix` once delivered. |

**Push** sources are driven by an inbound HTTP request. **Pull** sources run a
background loop and can also be triggered on demand with a POST, which returns
`{"triggered": true, "processed": N}`.

The S3 source additionally supports `apiTrigger.enabled` for an uncapped
on-demand scan, plus `disablePollLoop` to make a flow REST-triggered only —
useful for bulk reprocessing.

## Transforms

| Type | Params | Behaviour |
| --- | --- | --- |
| `passthrough` | — | Returns the message unchanged. The default when none is configured. |
| `jq` | `expression` | Runs a [gojq](https://github.com/itchyny/gojq) query. JSON in, JSON out. |
| `template` | `template` | Renders a Go `text/template` over `{{.Body}}` and `{{.Headers}}`. |

Both real transforms compile their program at startup, so a malformed
expression fails the process at boot rather than on the first message. In a
chain, each transform's output feeds the next; the last one's output goes to the
sink.

## Reliability

Delivery is **at-least-once**. Pull sources attach finalization hooks to each
message — commit a Kafka offset, delete an SQS message, move an S3 object — and
the engine invokes them only after the sink accepts the message. A crash between
sink success and acknowledgement yields a duplicate, never a loss.

- **Retry** wraps the sink call with geometric backoff. Note `maxAttempts`
  defaults to `1`, which counts the first attempt, so **retrying is off unless
  configured**.
- **Dead-letter** writes a JSON envelope to S3 — id, flow, source, receivedAt,
  error, headers, and body — after retries are exhausted, then acknowledges the
  source so the loop continues.
- Without a `deadLetter` block, a message that cannot be delivered **stops that
  flow's background loop** (Kafka and SQS; the S3 source logs and continues).
  Configure dead-lettering for unattended flows.

## HTTP endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Liveness. |
| `GET` | `{apiPrefix}/{version}/{name}` | Flow status: source/sink kinds, `running`, `processed`, `lastProcessedAt`, `lastError`. |
| `POST` | `{apiPrefix}/{version}/{name}` | Push source: deliver a payload. Pull source: trigger a poll. |

A push flow returns `200` with the sink's response body when there is one, else
`202 Accepted`; `400` for a malformed request and `502` when the pipeline fails.

## Deployment

**Service** — a long-running HTTP server.

```bash
docker build -f Dockerfile.service -t courier-core .          # alpine
docker build -f deployments/Dockerfile -t courier-core .      # distroless, nonroot
docker run -p 8080:8080 -e EIP_CONFIG=configs/example.yaml courier-core
```

Both images are static `CGO_ENABLED=0` builds of `cmd/service` and bundle
`configs/`.

**Lambda** — `cmd/lambda` reads the same config. Set `EIP_LAMBDA_TRIGGER`:

| Value | Event | Flow selected by |
| --- | --- | --- |
| `apigw` | API Gateway v2 HTTP | request path |
| `sqs` | SQS | `lambdaTrigger.sqsQueueArn` |
| `s3` | S3 object events | `lambdaTrigger.s3Bucket` |
| `kafka` | MSK / self-managed Kafka | `lambdaTrigger.kafkaTopic` |
| `apigw+s3` | either, detected per invocation | both of the above |

Because AWS events carry no flow name, pull-triggered flows need a
`lambdaTrigger` block naming the ARN, bucket, or topic that routes to them. The
SQS handler reports partial batch failures so only failed records are retried.

## Development

```bash
go test ./internal/flow/                                # one package
go test ./internal/transform/ -run TestJQReshape -v     # one test
gofmt -l .                                              # -w to fix
go vet ./...
```

Tests use the standard library only — no mocking or assertion frameworks; the
flow and Lambda tests use hand-written mock sources and sinks.

Layout: `cmd/` holds the two entrypoints, and `internal/app` is the composition
root both share. `internal/flow` is the engine, `internal/transport/*` the
transports, `internal/transform` the transforms, and `internal/config` the YAML
model and validation.

Adding a transport means implementing the interfaces in
`internal/transport/transport.go` and registering a factory — no changes to the
engine.

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — low-level design: abstractions,
  control flow, delivery semantics, and per-transport behaviour.
- [`CLAUDE.md`](CLAUDE.md) — repository guidance for Claude Code.
