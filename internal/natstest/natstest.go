// Package natstest connects tests to a NATS server.
package natstest

import (
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// requireEnv makes a missing server fatal instead of a skip. CI sets it, so an
// unreachable NATS can never be mistaken for a passing suite; leaving it unset
// keeps the tests runnable locally without a server.
const requireEnv = "PROTONATS_REQUIRE_NATS"

// Connect dials the default NATS URL, closing the connection when the test ends.
func Connect(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(nats.DefaultURL, nats.Timeout(2*time.Second))
	if err != nil {
		Unavailable(t, "NATS not available at %s: %v", nats.DefaultURL, err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// Unavailable reports missing infrastructure: fatal when the suite is required
// to run against a real server, a skip otherwise.
func Unavailable(t *testing.T, format string, args ...any) {
	t.Helper()
	if os.Getenv(requireEnv) != "" {
		t.Fatalf(format, args...)
	}
	t.Skipf(format, args...)
}
