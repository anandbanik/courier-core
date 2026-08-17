package message

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Message is the unit that moves through a flow.
type Message struct {
	ID       string
	Key      []byte
	Body     []byte
	Headers  map[string]string
	Metadata Metadata
}

// Metadata carries provenance and optional finalization hooks. A pull source
// (Kafka/SQS) sets Ack/Nack so the underlying record is only committed or
// deleted after the sink succeeds, giving at-least-once delivery without
// leaking broker types into the flow layer. Push sources leave them nil.
type Metadata struct {
	FlowName   string
	Source     string
	ReceivedAt time.Time

	// Ack finalizes the source record after a successful sink. Nack releases
	// it for redelivery. Both must be safe to call at most once.
	Ack  func() error
	Nack func(err error)
}

// New builds a Message with a generated ID and an initialized header map.
func New(body []byte) *Message {
	return &Message{
		ID:      newID(),
		Body:    body,
		Headers: map[string]string{},
	}
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
