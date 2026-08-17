# courier-core — Low-Level Design

Internal design reference for the courier-core component: its abstractions,
control flow, delivery semantics, and the behaviours that are easy to get wrong.

Audience: engineers modifying or operating this service. For a task-oriented
guide to adding transformation logic, see the source of
[`internal/transform`](../internal/transform/).

---

## Contents

1. [What it is](#1-what-it-is)
2. [Package map](#2-package-map)
3. [Domain model: Message](#3-domain-model-message)
4. [The four abstractions](#4-the-four-abstractions)
5. [Startup](#5-startup)
6. [Runtime: the pipeline](#6-runtime-the-pipeline)
7. [Delivery semantics](#7-delivery-semantics)
8. [Transport implementations](#8-transport-implementations)
9. [Transform subsystem](#9-transform-subsystem)
10. [Dead-letter subsystem](#10-dead-letter-subsystem)
11. [HTTP surface](#11-http-surface)
12. [Lambda mode](#12-lambda-mode)
13. [Configuration](#13-configuration)
14. [Concurrency model](#14-concurrency-model)
15. [Design notes and sharp edges](#15-design-notes-and-sharp-edges)

---

## 1. What it is

courier-core is a configuration-driven message-routing engine. A YAML file
declares **flows**; each flow binds a **source** to a **sink** with an optional
chain of **transforms** in between:

```
                    ┌──────────────── flow ────────────────┐
   Kafka / SQS /     │                                      │    Kafka / SQS /
   S3 / HTTP POST ──▶│  source ──▶ transform(s) ──▶ sink   │──▶ HTTP endpoint
                     │                    │                 │
                     └────────────────────┼─────────────────┘
                                          ▼ on failure
                                   dead-letter (S3)
```

The same binary runs in two deployment modes from one config file:

- **`cmd/service`** — a long-running HTTP server. Pull sources run background
  goroutines; push sources are served as REST endpoints.
- **`cmd/lambda`** — an AWS Lambda handler. AWS delivers the events; adapters
  route each to the right flow.

Both share the composition root [`internal/app`](../internal/app/app.go), so the
two modes cannot drift in how they build flows.

The design goal throughout is that adding a transport or a transform should not
require touching the flow engine.

---

## 2. Package map

```
cmd/service ─┐
             ├─▶ internal/app ──▶ internal/flow ──▶ internal/transform
cmd/lambda ──┘        │               │        └──▶ internal/transport ──▶ .../api
                      │               │        └──▶ internal/deadletter        .../kafka
                      │               │                                        .../sqs
                      ▼               ▼                                        .../s3
              internal/config   internal/message ◀── (everything)
                      ▲
              internal/httpserver, internal/lambda, internal/s3util
```

| Package | Responsibility |
| --- | --- |
| [`internal/app`](../internal/app/) | Composition root: load config, register transports, build flows, construct the server. |
| [`internal/config`](../internal/config/) | YAML shape, env-var expansion, defaults, validation. |
| [`internal/message`](../internal/message/) | The `Message` type. Depends on nothing. |
| [`internal/transport`](../internal/transport/) | `Source`/`Sink` interfaces and the transport registry. |
| [`internal/transport/{api,kafka,sqs,s3}`](../internal/transport/) | Concrete transports, each self-registering. |
| [`internal/transform`](../internal/transform/) | `Transform` interface, chain, and built-in transforms. |
| [`internal/flow`](../internal/flow/) | The engine: `Flow`, `Manager`, the pipeline, retry policy. |
| [`internal/deadletter`](../internal/deadletter/) | `Writer` interface and the S3 implementation. |
| [`internal/httpserver`](../internal/httpserver/) | Mux construction and per-flow route registration. |
| [`internal/lambda`](../internal/lambda/) | AWS event → flow adapters. |
| [`internal/s3util`](../internal/s3util/) | Key filtering/renaming shared by the S3 source and the S3 Lambda adapter. |

Two rules keep the graph acyclic and are worth preserving:

- `message` imports nothing internal. Everything may import it.
- `transform` and `transport` import `config`, never the reverse. This is why
  transform-type and transport-kind validation happens at *build* time in
  `BuildManager`, not in `config.Validate` — see
  [§15](#15-design-notes-and-sharp-edges).

---

## 3. Domain model: Message

[`internal/message/message.go`](../internal/message/message.go) — the single
unit that moves through a flow.

```go
type Message struct {
	ID       string            // random 16-byte hex, generated at ingest
	Key      []byte            // partition key (Kafka)
	Body     []byte            // the payload, opaque bytes
	Headers  map[string]string // protocol-neutral key/value metadata
	Metadata Metadata          // provenance + finalization hooks
}

type Metadata struct {
	FlowName   string
	Source     string
	ReceivedAt time.Time

	Ack  func() error     // finalize the source record (commit/delete/move)
	Nack func(err error)  // release for redelivery
}
```

`Body` is deliberately `[]byte`, not a parsed structure: the engine never
inspects the payload, so a flow can carry JSON, CSV, XML, or binary without the
core knowing.

**`Ack`/`Nack` are the crux of the delivery design.** A pull source attaches
closures that know how to commit a Kafka offset, delete an SQS message, or move
an S3 object. The flow calls them *after* the sink succeeds. This inverts the
usual coupling: the flow gets at-least-once delivery without importing a single
broker type, and a source can define whatever "finalize" means for it. Push
sources leave both `nil`.

`message.New(body)` generates the ID and initializes the header map;
constructing a `Message` literal directly risks a nil map.

---

## 4. The four abstractions

### Source

[`internal/transport/transport.go`](../internal/transport/transport.go) splits
sources by who drives the work:

```go
type Source interface {
	Kind() string
	IsPush() bool
	Close() error
}

type PushSource interface {         // request-driven; no background loop
	Source
	Build(ctx context.Context, body []byte, headers map[string]string) (*message.Message, error)
}

type PullSource interface {         // owns a loop; delivers into a callback
	Source
	Run(ctx context.Context, deliver SinkFunc) error
	Poll(ctx context.Context, max int, deliver SinkFunc) (processed int, err error)
}
```

`SinkFunc` is the seam that keeps sources ignorant of the rest of the pipeline:

```go
type SinkFunc func(ctx context.Context, msg *message.Message) (*message.Message, error)
```

The flow passes its own `deliverPull` method as this callback. A source calls it
per record and never learns that transforms, retries, or dead-lettering exist.

`Run` and `Poll` express the same work at different cadences: `Run` loops
forever (background operation), `Poll` does bounded work and returns a count
(REST-triggered operation). Every pull source implements both.

### Transform

```go
type Transform interface {
	Apply(ctx context.Context, in *message.Message) (*message.Message, error)
}
```

One method, one message in, one message out. See [§9](#9-transform-subsystem).

### Sink

```go
type Sink interface {
	Kind() string
	Send(ctx context.Context, msg *message.Message) (*message.Message, error)
	Close() error
}
```

`Send` may return a **response message** — the HTTP sink returns the downstream
response, which is what lets an API-sourced flow act as a synchronous proxy.
Kafka and SQS sinks return `nil, nil`.

### Dead-letter Writer

```go
type Writer interface {
	Write(ctx context.Context, msg *message.Message, cause error) error
}
```

Called only after all sink retries are exhausted. The caller owns `Ack`/`Nack`
timing after `Write` returns.

---

## 5. Startup

`app.New` ([`internal/app/app.go`](../internal/app/app.go)) is the whole startup
path, and it is deliberately fail-fast: any error aborts before the server binds
a port.

```
1. config.Load(path)
     ├─ os.ReadFile
     ├─ expandEnv        ${VAR} / ${VAR:-default}; missing vars → one error
     ├─ yaml.Unmarshal
     ├─ applyDefaults    server addr/prefix/timeouts
     └─ Validate         names, versions, uniqueness, endpoints, dead-letter

2. transport.NewRegistry()
     └─ api.Register / kafka.Register / sqs.Register / s3transport.Register

3. flow.BuildManager(ctx, cfg, reg, log)
     └─ for each cfg.AllFlows():
          reg.BuildSource      ─┐
          reg.BuildSink         │  any error → abort startup
          transform.BuildChain  │
          deadletter.Build     ─┘
          newRetryPolicy
          flow.New(...)

4. httpserver.New
     └─ GET /healthz + GET|POST {apiPrefix}/{version}/{name} per flow
```

**Transport registration** is a `map[string]Factory` keyed by transport kind
([`registry.go`](../internal/transport/registry.go)), populated explicitly in
step 2 rather than via `init()`. Each transport package exposes a one-line
`Register` ([example](../internal/transport/api/register.go)); the S3 package
registers only a source, which is how "S3 is source-only" is expressed in code
as well as in validation.

**Route paths** are `cfg.Server.APIPrefix + "/" + version + "/" + name`, e.g.
`/api/v1/ingestOrders`. Uniqueness is enforced on `version + "/" + name`.

Nothing is hot-reloadable: config is read exactly once, so a config change
requires a restart.

---

## 6. Runtime: the pipeline

All delivery paths converge on `Flow.pipeline`
([`internal/flow/flow.go`](../internal/flow/flow.go)):

```go
func (f *Flow) pipeline(ctx context.Context, msg *message.Message) (*message.Message, error) {
	msg.Metadata.FlowName = f.Name
	out, err := f.transform.Apply(ctx, msg)
	if err != nil {
		f.recordErr(err)
		return nil, err                    // note: no Nack
	}
	resp, err := f.sendWithRetry(ctx, out)
	if err != nil {
		f.recordErr(err)
		if msg.Metadata.Nack != nil {
			msg.Metadata.Nack(err)
		}
		return nil, err
	}
	if msg.Metadata.Ack != nil {
		if ackErr := msg.Metadata.Ack(); ackErr != nil {
			f.log.Warn("ack failed", "err", ackErr)
		}
	}
	f.processed.Add(1)
	...
	return resp, nil
}
```

Three things to note:

- `FlowName` is set **before** transforms run, so a transform may read it.
- A **transform** failure does not `Nack`; only a **sink** failure does.
- `Ack` failure is logged, not propagated — the sink already succeeded, so
  failing the message would cause a duplicate delivery on redelivery.

### Retry

`sendWithRetry` wraps only the sink call, with geometric backoff:

```go
backoff := f.retry.initial
for attempt := 1; attempt <= f.retry.maxAttempts; attempt++ {
	resp, err := f.sink.Send(ctx, msg)
	if err == nil { return resp, nil }
	if attempt == f.retry.maxAttempts { break }
	select {
	case <-ctx.Done(): return nil, ctx.Err()
	case <-time.After(backoff):
	}
	backoff = f.retry.next(backoff)   // ×multiplier, clamped to max
}
return nil, lastErr
```

Defaults from [`retry.go`](../internal/flow/retry.go): `maxAttempts=1`,
`initial=100ms`, `max=5s`, `multiplier=2.0`. `attempt` counts the first try, so
**the default of 1 means retrying is off**; each non-zero config field overrides
its default individually.

Transforms are never retried — they are assumed deterministic, so a retry would
just fail again.

### The three entry paths

| Path | Used by | Wrapper around `pipeline` |
| --- | --- | --- |
| `handlePost` | HTTP POST to a push-source flow | inline; dead-letters, returns 502 on failure |
| `deliverPull` | pull-source background loop and REST trigger | dead-letters; controls ack timing and whether the loop dies |
| `PipelineWithDeadLetter` | Lambda adapters | dead-letters; `nil` error means "handled, do not retry" |

They differ only in what a failure *means* to the caller — see next section.

---

## 7. Delivery semantics

The intended guarantee is **at-least-once**: acknowledge the source only after
the sink has accepted the message.

### `deliverPull` (service mode)

```
pipeline ok ─────────────────────────────────▶ (Ack already done in pipeline) ok
pipeline failed
   ├─ no dead-letter configured ─────────────▶ return err  ⇒ KILLS THE RUN LOOP
   ├─ dead-letter write failed
   │     ├─ stopOnFailure: true ─────────────▶ return err  ⇒ kills the loop
   │     └─ stopOnFailure: false ────────────▶ nil, nil    ⇒ unacked, broker redelivers
   └─ dead-letter write ok ──────────────────▶ Ack, then nil, nil ⇒ parked, continue
```

The first branch is the one to internalize: **for a Kafka or SQS flow with no
`deadLetter` block, a single failing message terminates that flow's background
loop.** `Manager.Start` logs `flow loop stopped` and the goroutine exits; other
flows keep running. This is deliberate fail-loud behaviour, but it makes
`deadLetter` effectively mandatory for unattended pull flows.

The S3 source is the exception — it swallows per-object delivery errors itself
(logs and continues), so it never reaches that branch.

### `PipelineWithDeadLetter` (Lambda mode)

Here a `nil` error means "handled; do not let AWS retry":

| Situation | Returns | AWS behaviour |
| --- | --- | --- |
| Sink succeeded | `resp, nil` | record consumed |
| Failed, no dead-letter | `nil, err` | AWS retries / its own DLQ takes over |
| Failed, dead-letter write failed | `nil, err` | AWS retries (not parked) |
| Failed, parked in dead-letter | `nil, nil` (after Ack) | record consumed |

### Ack/Nack by transport

| Transport | `Ack` | `Nack` | Redelivery on failure |
| --- | --- | --- | --- |
| Kafka source | `CommitMessages` | not set | offset uncommitted → refetched |
| SQS source | `DeleteMessage` | not set | visibility timeout expires → redelivered |
| S3 source | copy to `processedPrefix`, then delete | not set | object left in place → next scan |
| API source | not set | not set | none; failure returned to the HTTP caller |

No transport currently sets `Nack`; the `pipeline` call is a no-op for all of
them today. It exists for a future transport where explicit release beats
waiting for a timeout.

Both `Ack` closures that call AWS/Kafka use `context.Background()` rather than
the request context — intentional, so a cancelled request cannot lose an
acknowledgement for a message the sink already accepted.

**Duplicates are possible by design**: the sink can succeed and `Ack` then fail
(logged as `ack failed`). The S3 move is copy-then-delete for the same reason —
a crash between the two yields a duplicate, never a loss.

---

## 8. Transport implementations

### API (`internal/transport/api`)

**Source** — stateless push. `Build` wraps the request body, copies the flattened
HTTP headers onto the message, and stamps `Source`/`ReceivedAt`. Holds no
connections; `Close` is a no-op.

**Sink** — HTTP client. Defaults: method `POST`, timeout 30s. Optional OAuth2
client-credentials via `golang.org/x/oauth2/clientcredentials`, which handles
token fetch and refresh; a small `RoundTripper` logs the token endpoint's status
at debug level.

Header precedence, from [`sink.go`](../internal/transport/api/sink.go):

```go
for k, v := range msg.Headers { req.Header.Set(k, v) }   // message first
for k, v := range s.headers   { req.Header.Set(k, v) }   // config overrides
```

Any status ≥ 300 is an error carrying the status and the first 512 bytes of the
body. On success the response becomes a **new** `message.New(body)` with
`content-type` copied into headers — meaning the response message has a
*different* `ID` from the request message.

### Kafka (`internal/transport/kafka`)

**Source** — `kafka-go` `Reader`. With a `groupId`, offsets are managed by the
consumer group; without one, `StartOffset = LastOffset` (tail only). `Run` calls
`consumeOne` forever; `Poll` fetches up to `max` records with a 2s timeout each,
treating `DeadlineExceeded` as "queue empty" and breaking cleanly. `Ack` commits
the specific message.

**Sink** — `kafka-go` `Writer`, `LeastBytes` balancer, `requiredAcks` mapping
`none`/`all` with `one` as the default fallback. Propagates `Key`, `Body`, and
`Headers`.

### SQS (`internal/transport/sqs`)

**Source** — long polling; defaults `waitTimeSeconds=20`, `maxMessages=10`,
visibility timeout only sent when configured. `Poll` clamps `max` to `[1,10]`
(the SQS API limit) and uses `waitTime=0` for a prompt REST trigger. `Ack`
deletes by receipt handle.

**Sink** — `SendMessage` with the body only. **Headers are dropped**: message
attributes are not populated.

Both share [`client.go`](../internal/transport/sqs/client.go), which swaps in
static dummy credentials when `endpoint` is set, so local emulators work without
an AWS profile.

### S3 (`internal/transport/s3`)

Source-only — the registry has no S3 sink, and `config.Validate` rejects
`transport: s3` in a sink block.

Scanning uses `ListObjectsV2` with continuation-token pagination.
[`s3util.ShouldProcess`](../internal/s3util/s3util.go) filters each key: skip
folder markers (`/` suffix), skip anything not ending `.json`, skip keys already
under `processedPrefix`. That last check is what stops processed objects from
being re-ingested, since they are moved *within the same bucket*.

Per object: `GetObject` → message with `Headers["s3-key"]` → `Ack` moves it to
`processedPrefix + key`.

Failure handling is more forgiving than the other pull sources: a failed
`GetObject` logs and skips, a failed delivery logs and leaves the object in
place, and a failed scan in `Run` is logged but does not end the loop — a
transient S3 outage should not permanently kill the flow.

Defaults: `processedPrefix="processed/"`, `pollIntervalSeconds=30`,
`maxKeys=1000`.

**API-trigger mode** — `s3.apiTrigger.enabled` makes a POST perform an *uncapped*
scan (`max=0`) instead of the usual 10-message cap; adding
`disablePollLoop: true` makes `Run` a no-op so the flow is REST-triggered only.
Useful for on-demand bulk reprocessing.

---

## 9. Transform subsystem

Three built-ins, resolved by `Build`
([`transform.go`](../internal/transform/transform.go)):

| Type | Implementation | Params |
| --- | --- | --- |
| `passthrough` | returns the input pointer unchanged | — |
| `template` | Go `text/template` over `{Body, Headers}` | `template` (required) |
| `jq` | `gojq` query, JSON in → JSON out | `expression` (required) |

Both real transforms compile their program **at startup** (`template.Parse`,
`gojq.Parse`) and store the compiled artifact, so per-message work is just
execution. Config errors therefore surface at boot.

### Chaining

```go
type chain []Transform

func (c chain) Apply(ctx context.Context, in *message.Message) (*message.Message, error) {
	msg := in
	for i, t := range c {
		out, err := t.Apply(ctx, msg)
		if err != nil {
			return nil, fmt.Errorf("transform %d: %w", i, err)
		}
		msg = out
	}
	return msg, nil
}
```

`BuildChain` optimizes the shapes: zero entries → `Passthrough`; **one entry →
that transform directly, with no `chain` wrapper**; many → a `chain`. The error
wrapper adds the failing transform's index.

Config resolution order is `transforms` (list) > `transform` (singular) > nil,
via `FlowConfig.TransformChain()`. Setting both is a validation error.

### The copy convention

Every transform that modifies a message copies the struct rather than mutating
through the pointer:

```go
out := *in          // copies ID, Key, Headers, Metadata — including Ack/Nack
out.Body = body
return &out, nil
```

This is load-bearing: constructing a fresh `Message` instead would silently drop
the `Ack`/`Nack` closures and break at-least-once delivery, with no compile-time
error. `TestJQPreservesHeadersIDMetadata` guards it.

Note the copy is *shallow* — `out.Headers` aliases the input's map, so a
transform that writes headers also mutates the input's.

`Passthrough` returns the same pointer, which is safe precisely because it
changes nothing.

---

## 10. Dead-letter subsystem

S3 is the only implemented target. `deadletter.Build` returns `(nil, nil)` when
no block is configured — a nil `Writer` is the normal "no dead-letter" state and
every call site checks for it.

Each failure is written as a self-describing envelope
([`s3.go`](../internal/deadletter/s3.go)):

```json
{
  "id": "...", "flow": "...", "source": "sqs",
  "receivedAt": "...", "error": "http sink: status 500: ...",
  "headers": {...},
  "body": { }
}
```

`jsonBody` embeds the payload as raw JSON when it is valid JSON, and otherwise
wraps it as a JSON string, so the envelope always marshals cleanly regardless of
payload format.

Key layout: `{prefix}{flow}/[{date}/]{id}.json`, where the date segment appears
only if `dateLayout` is set (a Go time layout, e.g. `2006/01/02`), giving
partitioned prefixes for large volumes.

`stopOnFailure` decides whether a *failed dead-letter write* stops a pull loop —
see the [decision table](#deliverpull-service-mode).

---

## 11. HTTP surface

[`internal/httpserver`](../internal/httpserver/server.go) registers
`GET /healthz` plus `GET` and `POST` on each flow's path. `NewMux` is separate
from `New` so Lambda's API Gateway adapter can reuse the mux without an
`http.Server`.

Server defaults: `addr=:8080`, `apiPrefix=/api`, shutdown 15s, read 15s,
write 60s, idle 120s.

**GET** returns flow status — name, version, source/sink kinds, `push`,
`running`, `processed`, plus `lastProcessedAt` and `lastError` when set.

**POST** behaves differently by source type
([`flow.go`](../internal/flow/flow.go)):

| Source | Behaviour | Response |
| --- | --- | --- |
| Push (API) | read body → `Build` → `pipeline` | `200` + sink response body if the sink returned one, else `202 Accepted`; `400` on a bad request, `502` on pipeline failure |
| Pull (Kafka/SQS/S3) | trigger `Poll` — 10 messages, or uncapped for an API-trigger S3 flow | `200 {"triggered":true,"processed":N}`, `502` on error |

So POST means "send this payload" for a push flow and "drain some now" for a
pull flow — the same verb driving both, distinguished by the source's type.

Errors are JSON: `{"error": "..."}`.

---

## 12. Lambda mode

`cmd/lambda` selects a handler from `EIP_LAMBDA_TRIGGER`, sharing `app.New` with
the service:

| Value | Handler | Flow lookup |
| --- | --- | --- |
| `apigw` | replays the event through the mux | by path |
| `sqs` | `SQSEvent` | `lambdaTrigger.sqsQueueArn`, falling back to flow name = queue name from the ARN |
| `s3` | `S3Event` | `lambdaTrigger.s3Bucket` |
| `kafka` | `KafkaEvent` | `lambdaTrigger.kafkaTopic` |
| `apigw+s3` | shape-sniffs the raw JSON and dispatches | both of the above |

`LambdaTriggerConfig` exists because AWS events carry no flow name — the ARN,
bucket, or topic is the routing key. API Gateway needs no entry since the URL
path already identifies the flow.

Adapter specifics:

- **`apigw`** converts the event into an `http.Request`, serves it into an
  `httptest.ResponseRecorder`, and converts the recorder back into a Lambda
  response — so HTTP semantics live in exactly one place.
- **`sqs`** returns `BatchItemFailures`, using partial batch response so only
  failed records are retried. Unroutable records are logged and skipped.
- **`s3event`** builds its own S3 client (it must fetch the object itself) and
  reuses `s3util.ShouldProcess` / `ProcessedKey`, keeping poll-mode and
  event-mode filtering identical. Errors are logged and swallowed, leaving the
  object in place for a later run.
- **`kafka`** base64-decodes key and value with a raw-string fallback, and
  **returns on the first error**, abandoning the rest of the batch.
- **`combined`** unmarshals into a probe struct and dispatches on shape:
  `requestContext.http.method` present → API Gateway; `Records[0].eventSource ==
  "aws:s3"` → S3.

---

## 13. Configuration

Top level is `server`, `flows`, and `routes`. `AllFlows()` concatenates the last
two and the engine treats them identically — the split is **organizational
convention only** (`flows` = API-sourced endpoints, `routes` = pull-to-sink
pipes).

### Env expansion

[`env.go`](../internal/config/env.go) rewrites the raw bytes *before* YAML
parsing, so placeholders work anywhere including inside keys:

- `${VAR}` — required; unset is an error
- `${VAR:-default}` — falls back to `default`
- an env value always wins over an inline default
- **all** missing variables are collected and reported in one error, not just
  the first

### Validation

`Validate` ([`validate.go`](../internal/config/validate.go)) rejects: empty name
or version; duplicate `version/name`; both `transform` and `transforms`; a
missing or unknown transport; a sink of `transport: s3`; missing per-transport
required fields (Kafka brokers/topic, SQS queueUrl/region, S3 bucket/region, API
sink url); incomplete OAuth; negative `maxKeys`; `apiTrigger.disablePollLoop`
without `apiTrigger.enabled`; and a `deadLetter` block with no `s3` target.

It does **not** validate transform types — that would require importing
`transform` from `config`, inverting the dependency. Unknown types are caught
moments later in `BuildManager`, still at startup.

`S3Config` is reused for three roles (S3 source, dead-letter target, Lambda S3
adapter), each using a different subset of its fields.

---

## 14. Concurrency model

`Manager.Start` launches **one goroutine per flow**, tracked by a `WaitGroup`;
`context.Canceled` is filtered out of the shutdown logging.

```go
for _, f := range m.flows {
	m.wg.Add(1)
	go func(f *Flow) {
		defer m.wg.Done()
		if err := f.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			m.log.Error("flow loop stopped", "flow", f.Name, "err", err)
		}
	}(f)
}
```

`Flow.Run` returns immediately for push sources and for S3 flows with
`disablePollLoop`, so those goroutines simply exit.

Shared mutable state on `Flow` is confined to four atomics — `processed`
(`atomic.Int64`), `lastErr` and `lastAt` (`atomic.Pointer`), and `running`
(`atomic.Bool`) — because the status endpoint reads them while the pull loop
writes them. There are no mutexes in the flow engine.

Shutdown (`cmd/service`): `signal.NotifyContext` → `Server.Shutdown` with the
configured timeout → `Manager.Wait` → `Manager.Close` (closes every source and
sink).

**Concurrency caveat:** a pull-source flow can execute its pipeline on two
goroutines at once — the background `Run` loop and an HTTP-triggered `Poll` —
since nothing serializes them. Sink clients are individually goroutine-safe, and
the counters are atomic, so this is not memory-unsafe, but a REST trigger issued
while the poll loop is running can process the same backlog concurrently.
Configure `disablePollLoop` for REST-only S3 flows to avoid the overlap.

---

## 15. Design notes and sharp edges

Behaviours that are correct but surprising, worth knowing before changing
anything:

1. **Retrying is off by default.** `maxAttempts` defaults to 1, which counts the
   first attempt.
2. **A failing message kills a pull loop** when no `deadLetter` is configured
   (Kafka and SQS; not S3). Treat `deadLetter` as required for unattended flows.
3. **Transform errors do not `Nack`**, only sink errors do.
4. **No transport sets `Nack`.** Redelivery relies on uncommitted offsets,
   visibility timeouts, or objects left in place.
5. **`Ack` closures use `context.Background()`** so a cancelled request cannot
   lose an acknowledgement.
6. **The SQS sink drops headers** — body only.
7. **The API sink's response is a new `Message`** with a new `ID`.
8. **A single-entry transform chain is not wrapped**, so `BuildChain` can return
   a `*jqTransform` rather than a `chain`. Type-asserting on `chain` is unsafe.
9. **`Passthrough` returns the input pointer**, unlike every other transform.
10. **The struct copy in transforms is shallow** — `Headers` is shared with the
    input.
11. **`flows` vs `routes` is cosmetic**; both are concatenated by `AllFlows()`.
12. **S3 is source-only**, enforced in both the registry and validation.
13. **Transform types are validated in `BuildManager`, not `config.Validate`**,
    to keep the `config ← transform` dependency one-directional.
14. **No hot reload.** Config is read once at startup.
15. **Adding a transform type means editing a `switch`** in
    [`transform.go`](../internal/transform/transform.go) — unlike transports,
    which are registry-driven and extensible without touching core files.

### Testing

Tests are standard library `testing`, table-driven where it helps, colocated
with the code. `internal/flow` and `internal/lambda` use hand-written mock
sources and sinks rather than a mocking framework — `flow.NewWithRetryConfig`
exists specifically so tests outside the package can build a `*Flow` from mocks
without going through `BuildManager`.

`deployments/docker-compose.yaml` provides local Kafka and an AWS emulator for
manual end-to-end testing.
