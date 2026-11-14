# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

No Makefile, no CI config, no linter beyond the Go toolchain. Standard library
`testing` only — no mocking or assertion frameworks.

```bash
go build ./...
go vet ./...
gofmt -l .                      # list unformatted files; -w to fix

go test ./...
go test ./internal/flow/                                  # one package
go test ./internal/transform/ -run TestJQReshape -v        # one test
```

Run the service (needs a config file; falls back to `configs/example.yaml`):

```bash
EIP_CONFIG=configs/example.yaml go run ./cmd/service
EIP_DEBUG=true EIP_CONFIG=configs/example.yaml go run ./cmd/service   # debug logs
```

Three environment variables exist: `EIP_CONFIG` (config path), `EIP_DEBUG`
(bool-ish), and `EIP_LAMBDA_TRIGGER` for `cmd/lambda`, which must be one of
`apigw`, `sqs`, `s3`, `kafka`, `apigw+s3`.

Local Kafka and an AWS emulator for manual testing:

```bash
docker compose -f deployments/docker-compose.yaml up -d
```

Smoke-testing a change end to end usually means pointing a flow's API sink at a
local listener on port 9000 and POSTing to the flow's path
(`{apiPrefix}/{version}/{name}`, e.g. `/api/v1/echo`). `GET` on the same path
returns flow status including `processed` and `lastError`.

## Architecture

`docs/architecture.md` is a full low-level design reference — read it before
non-trivial changes. The essentials:

A YAML file declares **flows**; each binds a **source** to a **sink** with an
optional chain of **transforms** between them. The engine never inspects the
payload (`Message.Body` is `[]byte`).

**One composition root, two binaries.** `cmd/service` (HTTP server, background
poll loops) and `cmd/lambda` (AWS event handlers) both build everything through
`internal/app.New`. Wiring belongs there, not in either `main.go`, so the two
modes cannot drift.

**`Ack`/`Nack` closures are the central design idea.** Pull sources attach
closures to `Message.Metadata` that know how to commit a Kafka offset, delete an
SQS message, or move an S3 object. `Flow.pipeline` calls them only after the
sink succeeds. That is how at-least-once delivery works without the flow engine
importing any broker types.

**All delivery paths converge on `Flow.pipeline`** (`internal/flow/flow.go`).
Three wrappers differ only in what failure *means*: `handlePost` (HTTP, returns
502), `deliverPull` (background loops), `PipelineWithDeadLetter` (Lambda, where
a `nil` error means "handled, don't retry").

**Import graph rules.** `internal/message` imports nothing internal.
`internal/config` is imported by `transform` and `transport`, never the reverse
— which is why unknown transform types are caught in `flow.BuildManager` (via
`transform.Registry.Build`) rather than `config.Validate`. Preserve this
direction; `internal/transform/custom` imports `internal/transform`, never the
reverse.

**Extending.** Both transports and transforms are registry-driven, and neither
requires core edits. For a transport, implement the interfaces in
`internal/transport/transport.go` and add a `Register` function. For a transform,
implement `transform.Transform` in `internal/transform/custom` and add a line to
that package's `Register` — see `docs/custom-transforms.md`, with
`internal/transform/custom/enrich.go` as the worked example. Both registries are
instance-based and populated explicitly in `internal/app`, not via `init()`.

## Gotchas

These are correct but surprising, and each has cost time before:

- **Retrying is off by default.** `maxAttempts` defaults to 1, and it counts the
  first attempt. Only sink calls are retried, never transforms.
- **A failing message kills a pull loop when no `deadLetter` is configured**
  (Kafka/SQS; the S3 source swallows per-object errors instead). Treat
  `deadLetter` as required for unattended flows.
- **Transforms must copy the message struct**, not mutate through the pointer:
  `out := *in; out.Body = ...; return &out, nil`. Building a fresh
  `message.Message` silently drops the `Ack`/`Nack` closures and breaks
  at-least-once delivery with no compile error. The copy is shallow, so
  `Headers` still aliases the input's map.
- **`Registry.BuildChain` does not wrap a single entry**, so it can return a
  `*jqTransform` rather than a `chain`. Type-asserting on `chain` is unsafe.
- **Registering a transform name twice panics**, deliberately, so a custom
  transform cannot silently shadow a built-in like `jq`. This is the one place
  the transform registry differs from `transport.Registry`, whose
  `RegisterSource` overwrites instead.
- **Transform errors do not `Nack`**; only sink errors do. No transport
  currently sets `Nack` at all — redelivery relies on uncommitted offsets,
  visibility timeouts, or objects left in place.
- **`flows` vs `routes` in YAML is cosmetic** — `AllFlows()` concatenates them
  and the engine treats both identically.
- **S3 is source-only**, enforced in both the registry and validation.
- **The SQS sink drops headers**; the API sink's response becomes a *new*
  `Message` with a different `ID`.
- **No hot reload** — config is read once at startup, so restart after editing
  YAML.

## Config

`configs/example.yaml` is the reference for every supported shape. Config is
loaded once, validated, and fails the process at startup on any error.

`${VAR}` and `${VAR:-default}` placeholders are expanded over the raw bytes
before YAML parsing; an env value beats an inline default, and every missing
variable is reported in one error.

`configs/onestream.yaml` is a real deployment config containing internal
endpoints and an OAuth client ID — do not copy its values into docs, tests, or
examples.
