package protonats

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultRetryPolicy(t *testing.T) {
	cases := map[string]struct {
		err  error
		want Disposition
	}{
		"plain error":       {errors.New("boom"), Retry},
		"server error":      {Errorf(500, "upstream down"), Retry},
		"client error":      {Errorf(404, "no such order"), Retry},
		"terminate":         {ErrTerminate, Terminate},
		"wrapped terminate": {fmt.Errorf("declined: %w", ErrTerminate), Terminate},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, DefaultRetryPolicy(tc.err))
		})
	}
}

func TestTerminateOnClientError(t *testing.T) {
	cases := map[string]struct {
		err  error
		want Disposition
	}{
		"400 malformed":      {Errorf(400, "bad payload"), Terminate},
		"404 unknown":        {Errorf(404, "no such order"), Terminate},
		"499 upper bound":    {Errorf(499, "edge"), Terminate},
		"500 server":         {Errorf(500, "upstream down"), Retry},
		"399 below range":    {Errorf(399, "odd"), Retry},
		"plain error":        {errors.New("boom"), Retry},
		"wrapped 4xx":        {fmt.Errorf("charging: %w", Errorf(402, "no funds")), Terminate},
		"terminate sentinel": {ErrTerminate, Terminate},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, TerminateOnClientError(tc.err))
		})
	}
}

// The delay tracks the attempt count and holds at the last entry, so a long
// retry budget does not run off the end of a short backoff list.
func TestBackoffFor(t *testing.T) {
	backoff := []time.Duration{time.Second, 5 * time.Second, 30 * time.Second}
	cases := map[uint64]time.Duration{
		0: time.Second, // no metadata yet; treat as the first attempt
		1: time.Second,
		2: 5 * time.Second,
		3: 30 * time.Second,
		4: 30 * time.Second,
		9: 30 * time.Second,
	}
	for delivered, want := range cases {
		assert.Equal(t, want, backoffFor(backoff, delivered), "after %d deliveries", delivered)
	}
}

func TestRollbackNaming(t *testing.T) {
	assert.Equal(t, "orders.Reserve.rollback", RollbackSubjectFor("orders.Reserve"))
	assert.Equal(t, "orders.*.rollback", RollbackSubjectFor("orders.*"))
	assert.Equal(t, "reserver-rollback", RollbackConsumerFor("reserver"))

	// An ephemeral task has no durable rollback consumer to derive.
	assert.Equal(t, "", RollbackConsumerFor(""))
}

func TestHandlerOptions_RetryPolicyDefaults(t *testing.T) {
	assert.Equal(t, Retry, ApplyHandlerOptions(nil).retryPolicyOr()(Errorf(404, "x")))

	ho := ApplyHandlerOptions([]HandlerOption{WithRetryPolicy(TerminateOnClientError)})
	assert.Equal(t, Terminate, ho.retryPolicyOr()(Errorf(404, "x")))
}
