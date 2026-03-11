package protonats

import "time"

// Acker provides JetStream acknowledgment controls.
type Acker interface {
	Ack() error
	Nak() error
	NakWithDelay(delay time.Duration) error
	Term() error
	InProgress() error
}
