# Writing a Custom Transform

A guide for adding your own payload-transformation code to courier-core.

This assumes you can read Go but may not have written much of it. Every Go
concept the task needs is explained inline. Nothing here requires knowing the
rest of the codebase.

---

## Contents

1. [What a transform is](#1-what-a-transform-is)
2. [The two pieces you write](#2-the-two-pieces-you-write)
3. [Tutorial: build one from scratch](#3-tutorial-build-one-from-scratch)
4. [The message you are handed](#4-the-message-you-are-handed)
5. [The three rules](#5-the-three-rules)
6. [Returning errors](#6-returning-errors)
7. [Testing](#7-testing)
8. [More examples](#8-more-examples)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. What a transform is

A **flow** moves a message from a **source** to a **sink**. In between, it can
pass through one or more **transforms**, each reshaping the payload:

```
source ──▶ transform 1 ──▶ transform 2 ──▶ sink
(Kafka,      (jq)          (your code)     (HTTP,
 SQS, S3,                                   Kafka,
 HTTP POST)                                 SQS)
```

Each transform's output becomes the next one's input. The last one's output
goes to the sink. **You do not write the chaining** — the engine does it. Your
job is only: *take one message, return one message.*

Three transforms ship built in:

| Type | What it does |
| --- | --- |
| `passthrough` | Returns the message unchanged. The default when no transform is configured. |
| `template` | Renders the body through a Go `text/template`. |
| `jq` | Runs a [jq](https://jqlang.github.io/jq/) expression over a JSON body. |

Reach for a custom transform when those cannot express what you need — for
example, logic with conditionals, a lookup table, a checksum, or anything
needing real code.

Custom transforms are **compiled Go inside this repository**. There is no
scripting engine, which means full type safety and ordinary `go test` — but
also that changing one requires rebuilding and redeploying the binary.

---

## 2. The two pieces you write

### Piece 1: the transform itself

A Go **interface** is a list of methods. Any type that has those methods
satisfies the interface automatically — there is no `implements` keyword. The
interface here has exactly one method
([`transform.go`](../internal/transform/transform.go)):

```go
type Transform interface {
    Apply(ctx context.Context, in *message.Message) (*message.Message, error)
}
```

So anything with an `Apply` method of that exact shape *is* a `Transform`.

Reading the signature piece by piece:

- `ctx context.Context` — Go's standard cancellation/deadline value. Pure
  transformation logic ignores it. Pass it along if you call something that
  takes a context (an HTTP request, a database query).
- `in *message.Message` — the incoming message. The `*` means **pointer**: you
  receive the address of the message, not a copy of it. This matters, and
  [rule 1](#rule-1-copy-the-message-never-modify-the-input) explains why.
- returns `(*message.Message, error)` — Go functions can return several values.
  The convention: return the result and `nil` on success, or `nil` and an error
  on failure.

### Piece 2: the factory

A **factory** is a function that builds your transform from its YAML config:

```go
type Factory func(params map[string]string) (Transform, error)
```

`params` is the `params:` block from the flow's YAML. The factory runs **once at
startup**, `Apply` runs **once per message**. That split is the whole point:
validate and parse config in the factory so a bad config crashes the service
immediately, instead of failing on message number 4,000 at 3am.

---

## 3. Tutorial: build one from scratch

We will build `addTimestamp`, which adds the current time as a field to a JSON
object body:

```json
{"orderId": "A1"}   ──▶   {"orderId": "A1", "processedAt": "2026-08-17T12:00:00Z"}
```

The field name will be configurable, defaulting to `processedAt`.

### Step 1 — create the file

All custom transforms live in
[`internal/transform/custom/`](../internal/transform/custom/). Create
`internal/transform/custom/addtimestamp.go`, starting with the package name and
the imports:

```go
package custom

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"courier-core/internal/message"
	"courier-core/internal/transform"
)
```

Every file in a directory shares one package name — here, `custom`. Go
**fails to compile on an unused import**, which surprises newcomers; if you see
`imported and not used`, delete the line. Standard-library imports go in the
first group, project imports in the second.

### Step 2 — define the struct

A **struct** is a bundle of fields. Yours holds everything the factory worked
out at startup, so `Apply` does not redo it per message:

```go
type addTimestampTransform struct {
	field string
	now   func() time.Time
}
```

`field` is the JSON key to write. `now` is a *function value* — a variable
holding a function. Instead of calling `time.Now()` directly in `Apply`, we call
`t.now()`. In production it holds `time.Now`; a test can swap in a function
returning a fixed time, which makes the output predictable enough to assert on.
That one indirection is what makes this transform testable.

Names starting with a lowercase letter are private to the package. Keep your
type and factory lowercase — nothing outside needs them.

### Step 3 — write the factory

```go
func newAddTimestamp(params map[string]string) (transform.Transform, error) {
	field := params["field"]
	if field == "" {
		field = "processedAt"
	}
	return &addTimestampTransform{field: field, now: time.Now}, nil
}
```

Reading a missing key from a Go map returns the zero value rather than an
error — for `map[string]string` that is `""`. So one `if` covers both "key
absent" and "key present but empty", which is what you want for an optional
setting with a default.

`&addTimestampTransform{...}` creates a value and yields a pointer to it. Note
`time.Now` has no parentheses: we are storing the function, not calling it.

Returning `nil` as the second value means success.

**If a param were required**, this is where you would reject it — as `jq` does
in [`jq.go`](../internal/transform/jq.go):

```go
expr, ok := params["expression"]
if !ok || expr == "" {
	return nil, fmt.Errorf("jq transform requires params.expression")
}
```

The two-value form `v, ok := m[k]` distinguishes "absent" from "present but
empty"; `ok` is a bool.

### Step 4 — write `Apply`

```go
func (t *addTimestampTransform) Apply(_ context.Context, in *message.Message) (*message.Message, error) {
	var body map[string]any
	if err := json.Unmarshal(in.Body, &body); err != nil {
		return nil, fmt.Errorf("addTimestamp transform: body is not a JSON object: %w", err)
	}
	if body == nil {
		return nil, fmt.Errorf("addTimestamp transform: body is not a JSON object")
	}

	body[t.field] = t.now().UTC().Format(time.RFC3339)

	newBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("addTimestamp transform: %w", err)
	}

	out := *in
	out.Body = newBody
	return &out, nil
}
```

Unpacking the unfamiliar parts:

- `func (t *addTimestampTransform) Apply(...)` — the `(t *addTimestampTransform)`
  before the name is the **receiver**, Go's way of attaching a method to a type.
  Inside, `t` is your struct. Because this method exists with this exact
  signature, `*addTimestampTransform` now satisfies `Transform`.
- `_ context.Context` — `_` discards a parameter you must accept but do not use.
- `map[string]any` — a JSON object decodes into this. `any` means "any type",
  since JSON values can be strings, numbers, booleans, arrays, or nested
  objects.
- `json.Unmarshal(in.Body, &body)` — decodes bytes into `body`. The `&` passes
  the variable's address so the function can fill it in.
- `if err := ...; err != nil` — Go idiom that runs a statement and tests its
  result in one line, scoping `err` to the `if`.
- `%w` in `fmt.Errorf` wraps the underlying error so the original text is
  preserved in the final message.
- The `body == nil` check catches a real edge case: the JSON literal `null` is
  valid JSON and unmarshals into a `nil` map without error. Writing to a nil map
  panics, so reject it explicitly.
- `out := *in` — the important line. `*in` **dereferences** the pointer, copying
  the struct. `&out` returns a pointer to the copy. See
  [rule 1](#rule-1-copy-the-message-never-modify-the-input).

### Step 5 — register it

Your transform is invisible until it has a name. Open
[`custom.go`](../internal/transform/custom/custom.go) and add one line:

```go
func Register(r *transform.Registry) {
	r.Register("enrich", newEnrich)
	r.Register("addTimestamp", newAddTimestamp)   // <── added
}
```

The string is what flows use as their config `type`. It does not have to match
the Go names.

Registering a name that already exists **panics at startup** — deliberately, so
a custom transform can never silently shadow a built-in like `jq`.

You do not need to touch anything else. `Register` is already wired into
[`internal/app`](../internal/app/app.go), the shared startup path for both
`cmd/service` and `cmd/lambda`, so registering once covers both.

### Step 6 — use it in a flow

In your config (e.g. [`configs/example.yaml`](../configs/example.yaml)):

```yaml
- name: timestampedOrders
  version: v1
  source:
    transport: api
  transforms:
    - type: jq
      params:
        expression: "{id: .orderId, qty: .quantity}"
    - type: addTimestamp
      params:
        field: processedAt
  sink:
    transport: api
    api:
      url: "http://localhost:9000/downstream"
      method: POST
```

Built-in and custom transforms mix freely, in any order. Each flow constructs
its **own instances** from its **own params**, so two flows using
`addTimestamp` with different `field` values do not interfere.

For a single transform you may use `transform:` (singular) instead of a
`transforms:` list. Setting both is rejected at startup.

### Step 7 — verify

```bash
go build ./...
go test ./...
```

Then run it end to end. In one terminal, start something to receive the output:

```bash
python3 -m http.server 9000
```

In another:

```bash
EIP_CONFIG=configs/example.yaml go run ./cmd/service
curl -XPOST localhost:8080/api/v1/timestampedOrders \
     -d '{"orderId":"A1","quantity":3}'
```

You should get back the reshaped, timestamped payload. `GET` the same URL for
flow status, including a `processed` count and the last error if any.

---

## 4. The message you are handed

From [`message.go`](../internal/message/message.go):

```go
type Message struct {
	ID       string
	Key      []byte
	Body     []byte
	Headers  map[string]string
	Metadata Metadata
}
```

| Field | What it is | Should you change it? |
| --- | --- | --- |
| `ID` | Random hex id generated at ingest. | No — it ties log lines together. |
| `Key` | Partition key. Becomes the Kafka message key at a Kafka sink. | Only if you deliberately want to re-partition. |
| `Body` | The payload, as raw bytes. | **Yes — this is the field you are here to change.** |
| `Headers` | String key/value pairs. Become HTTP request headers at an HTTP sink and Kafka headers at a Kafka sink. | Sometimes — useful for routing hints or content type. |
| `Metadata` | Provenance and delivery hooks. | No — read only. |

`Metadata` carries `FlowName`, `Source`, `ReceivedAt`, and two function fields,
`Ack` and `Nack`. Those finalize the source record — acknowledging a Kafka
offset or deleting an SQS message once the sink succeeds. **Never call or clear
them.** The engine owns their timing; that is what gives at-least-once delivery.
Copying the struct ([rule 1](#rule-1-copy-the-message-never-modify-the-input))
carries them along automatically.

`Metadata.FlowName` is set *before* transforms run, so you can safely read it —
[`enrich.go`](../internal/transform/custom/enrich.go) does exactly that.

Two details worth knowing about `Body`: it is `[]byte`, not a string, so JSON is
not parsed for you — decode it yourself, as the tutorial does. And nothing
requires it to be JSON. A transform is free to read or emit CSV, XML, or
anything else; just document what yours expects.

One header nuance: at an HTTP sink, message headers are applied to the request
first and *then* headers from the flow's `api.headers:` config, so a configured
header wins over one you set in a transform.

---

## 5. The three rules

### Rule 1: copy the message, never modify the input

Always end with the copy-then-return pattern:

```go
out := *in          // copy the struct
out.Body = newBody  // change only the copy
return &out, nil    // return a pointer to the copy
```

**Why copying is necessary.** You receive a *pointer*. Writing `in.Body = ...`
would reach through it and modify the caller's message — a side effect that
outlives your function and can corrupt retry and dead-letter behaviour, which
rely on the original message still being intact.

**Why copying is sufficient.** `out := *in` copies every field, so `ID`,
`Headers`, and `Metadata` (including the `Ack`/`Nack` hooks) come along without
you listing them. You then overwrite just `Body`. Get this backwards — build a
fresh `message.Message` from scratch — and you silently drop the delivery hooks,
breaking at-least-once delivery in a way no compiler will catch.

One sharp edge: the copy is **shallow**. `Headers` is a map, and maps are
reference types, so `out.Headers` and `in.Headers` point at the *same* map.
Writing `out.Headers["x"] = "y"` also mutates the input. If you need to change
headers without touching the original, copy the map first:

```go
out.Headers = make(map[string]string, len(in.Headers))
for k, v := range in.Headers {
	out.Headers[k] = v
}
out.Headers["x-processed"] = "true"
```

The same applies to `Body` if you modify the byte slice in place rather than
replacing it — replacing it, as the tutorial does, is always safe.

### Rule 2: validate config in the factory, not in `Apply`

The factory runs once at startup; `Apply` runs on every message. Anything you
can check or precompute up front — required params, number parsing, compiling a
regex or template — belongs in the factory. A config mistake then stops the
service at boot, loudly, instead of failing per message in production.

### Rule 3: prefix your errors

Start every error with your transform's name:

```go
return nil, fmt.Errorf("addTimestamp transform: body is not a JSON object: %w", err)
```

The chain adds only a position (`transform 2: ...`). In a flow with several
transforms, the name is what tells an on-call engineer which one failed.

---

## 6. Returning errors

When your `Apply` returns a non-nil error, the engine:

1. records it as the flow's `lastError` (visible on the flow's `GET` endpoint);
2. **never calls the sink** — the message is not delivered;
3. writes the message to the dead-letter target if the flow configures one;
4. for HTTP-sourced flows, responds `502 Bad Gateway` with the error text.

So an error means "this message cannot go on". Note there is no way to *skip* a
message: returning `nil, nil` is not a supported "drop it" signal. If a message
should be ignored rather than failed, filter it upstream with a `jq` transform,
or pass it through unchanged.

Prefer errors that name the offending input. `body is not a JSON object` beats
`unmarshal failed` when you are reading it out of a log at 3am.

---

## 7. Testing

Tests live beside the code, in a file ending `_test.go`. Run them with:

```bash
go test ./internal/transform/custom/          # this package
go test ./...                                 # everything
go test ./internal/transform/custom/ -run AddTimestamp -v   # one test, verbose
```

A test is a function starting with `Test` that takes `*testing.T`. Call
`t.Fatalf` to fail and stop, or `t.Errorf` to record a failure and continue.

```go
package custom

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"courier-core/internal/message"
)

func TestAddTimestampDefaultField(t *testing.T) {
	tr, err := newAddTimestamp(nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	// Swap in a fixed clock so the output is predictable.
	fixed := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	tr.(*addTimestampTransform).now = func() time.Time { return fixed }

	in := message.New([]byte(`{"orderId":"A1"}`))
	in.Headers["k"] = "v"

	out, err := tr.Apply(context.Background(), in)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(out.Body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["processedAt"] != "2026-08-17T12:00:00Z" {
		t.Errorf("processedAt = %v", got["processedAt"])
	}
	if got["orderId"] != "A1" {
		t.Errorf("original field lost: %v", got)
	}

	// Rule 1, asserted:
	if out.ID != in.ID || out.Headers["k"] != "v" {
		t.Error("ID or Headers not preserved")
	}
	if string(in.Body) != `{"orderId":"A1"}` {
		t.Errorf("input mutated: %s", in.Body)
	}
}
```

`tr.(*addTimestampTransform)` is a **type assertion**: `newAddTimestamp` returns
the general `transform.Transform` interface, and this recovers the concrete type
so the test can reach the `now` field. `message.New` builds a message with a
generated ID and an initialized header map.

Worth covering for any transform:

- the happy path, with output asserted field by field;
- **`ID` and `Headers` survive, and the input is not mutated** — the regression
  test for rule 1;
- malformed input (not JSON, `null`, an array, a bare number);
- the factory rejecting bad config, and applying defaults for absent config.

[`enrich_test.go`](../internal/transform/custom/enrich_test.go) is a complete
worked example, including a check that the `Ack` hook survives the copy.

---

## 8. More examples

**Read a header and branch.**

```go
if in.Headers["x-tenant"] == "acme" {
	body["region"] = "us-east-1"
}
```

**Set a header for the sink.** Copy the map first — see rule 1's sharp edge.

**Handle a JSON array body.** Unmarshal into a slice instead of a map:

```go
var items []map[string]any
if err := json.Unmarshal(in.Body, &items); err != nil {
	return nil, fmt.Errorf("myTransform: body is not a JSON array: %w", err)
}
for _, item := range items {
	item["seen"] = true      // maps are references, so this edits in place
}
```

**Precompute in the factory.** A transform needing a compiled regex or a parsed
number should do that work once in the factory and store the result on the
struct — the pattern `jq` uses, parsing its expression at startup and keeping
the compiled query.

**Merge config fields into a body.** Already written:
[`enrich.go`](../internal/transform/custom/enrich.go).

**Keep output ordering stable.** Iterating a Go map yields keys in *random*
order, which makes output and tests flaky. `enrich.go` sorts its params once in
the factory for exactly this reason.

---

## 9. Troubleshooting

| Symptom | Cause |
| --- | --- |
| `unknown transform type "x"` at startup | The `type:` in YAML has no matching `r.Register("x", ...)` in `custom.go`. Check for a typo — the name is case-sensitive. |
| `panic: transform: duplicate registration for type "x"` | That name is registered twice, or collides with `passthrough`, `template`, or `jq`. Rename yours. |
| `imported and not used` | Go rejects unused imports. Delete the line. |
| `cannot use ... as transform.Transform value` | Your `Apply` signature does not match exactly, or you returned a value where a pointer is needed (`&addTimestampTransform{}`, not `addTimestampTransform{}`, when the method has a pointer receiver). |
| `panic: assignment to entry in nil map` | Writing to a map that was never initialized — usually a body that was the JSON literal `null`. Add the `body == nil` check. |
| Downstream lost the `ID`, or messages get redelivered forever | You built a new `message.Message` instead of copying with `out := *in`, dropping the `Ack`/`Nack` hooks. |
| Changing headers affected the original message | The struct copy is shallow. Copy the map explicitly. |
| Output field order changes between runs | You are iterating a map. Sort the keys first. |
| Config change had no effect | The service reads config only at startup. Restart it. |

### Known limitation

`params` is `map[string]string`, so a transform needing nested or typed
configuration must encode it into a string — for example a JSON blob in a single
param, decoded in the factory:

```yaml
params:
  mapping: '{"orderId":"id","quantity":"qty"}'
```

---

## Reference

| File | Contents |
| --- | --- |
| [`internal/transform/transform.go`](../internal/transform/transform.go) | The `Transform` interface, `Passthrough`, and the chain. |
| [`internal/transform/registry.go`](../internal/transform/registry.go) | `Factory`, `Registry`, `Register`, `Build`, `BuildChain`. |
| [`internal/transform/custom/custom.go`](../internal/transform/custom/custom.go) | Where you register your transform. |
| [`internal/transform/custom/enrich.go`](../internal/transform/custom/enrich.go) | Reference implementation. |
| [`internal/transform/jq.go`](../internal/transform/jq.go) | A built-in with a required param and startup precompilation. |
| [`internal/app/app.go`](../internal/app/app.go) | Startup wiring, shared by both entrypoints. |
