package protonats

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallOptions_Defaults(t *testing.T) {
	co := applyCallOptions(nil)
	assert.Nil(t, co.timeout)
	assert.Nil(t, co.headers)
	assert.Empty(t, co.subject)
}

func TestCallOptions_WithTimeout(t *testing.T) {
	co := applyCallOptions([]CallOption{WithTimeout(3 * time.Second)})
	require.NotNil(t, co.timeout)
	assert.Equal(t, 3*time.Second, *co.timeout)
}

func TestCallOptions_WithHeaders(t *testing.T) {
	co := applyCallOptions([]CallOption{WithHeaders(nats.Header{"X-Id": []string{"123"}})})
	assert.Equal(t, "123", co.headers.Get("X-Id"))
}

func TestCallOptions_WithSubject(t *testing.T) {
	co := applyCallOptions([]CallOption{WithSubject("override")})
	assert.Equal(t, "override", co.subject)
}

func TestCallOptions_Compose(t *testing.T) {
	co := applyCallOptions([]CallOption{
		WithTimeout(1 * time.Second),
		WithSubject("test"),
	})
	require.NotNil(t, co.timeout)
	assert.Equal(t, 1*time.Second, *co.timeout)
	assert.Equal(t, "test", co.subject)
}

// Edge cases

func TestCallOptions_ZeroTimeout(t *testing.T) {
	co := applyCallOptions([]CallOption{WithTimeout(0)})
	require.NotNil(t, co.timeout)
	assert.Equal(t, time.Duration(0), *co.timeout)
}

func TestCallOptions_NegativeTimeout(t *testing.T) {
	co := applyCallOptions([]CallOption{WithTimeout(-1 * time.Second)})
	require.NotNil(t, co.timeout)
	assert.Equal(t, -1*time.Second, *co.timeout)
}

func TestCallOptions_NilHeaders(t *testing.T) {
	co := applyCallOptions([]CallOption{WithHeaders(nil)})
	assert.Nil(t, co.headers)
}

func TestCallOptions_EmptyHeaders(t *testing.T) {
	co := applyCallOptions([]CallOption{WithHeaders(nats.Header{})})
	assert.NotNil(t, co.headers)
	assert.Empty(t, co.headers)
}

func TestCallOptions_MultipleHeadersCalls(t *testing.T) {
	co := applyCallOptions([]CallOption{
		WithHeaders(nats.Header{"X-First": []string{"1"}}),
		WithHeaders(nats.Header{"X-Second": []string{"2"}}),
	})
	// Last one wins.
	assert.Equal(t, "", co.headers.Get("X-First"))
	assert.Equal(t, "2", co.headers.Get("X-Second"))
}

func TestCallOptions_EmptySubject(t *testing.T) {
	co := applyCallOptions([]CallOption{WithSubject("")})
	assert.Equal(t, "", co.subject) // empty doesn't override
}

func TestCallOptions_WildcardSubject(t *testing.T) {
	co := applyCallOptions([]CallOption{WithSubject("orders.>")})
	assert.Equal(t, "orders.>", co.subject)
}

func TestCallOptions_AllCombined(t *testing.T) {
	co := applyCallOptions([]CallOption{
		WithTimeout(5 * time.Second),
		WithHeaders(nats.Header{"X-Id": []string{"abc"}}),
		WithSubject("custom.subj"),
	})
	require.NotNil(t, co.timeout)
	assert.Equal(t, 5*time.Second, *co.timeout)
	assert.Equal(t, "abc", co.headers.Get("X-Id"))
	assert.Equal(t, "custom.subj", co.subject)
}

func TestCallOptions_TimeoutOverride(t *testing.T) {
	// Second timeout overrides first.
	co := applyCallOptions([]CallOption{
		WithTimeout(1 * time.Second),
		WithTimeout(10 * time.Second),
	})
	require.NotNil(t, co.timeout)
	assert.Equal(t, 10*time.Second, *co.timeout)
}

func TestCallOptions_SubjectOverride(t *testing.T) {
	co := applyCallOptions([]CallOption{
		WithSubject("first"),
		WithSubject("second"),
	})
	assert.Equal(t, "second", co.subject)
}

func TestCallOptions_WithMethodName(t *testing.T) {
	co := applyCallOptions([]CallOption{WithMethodName("pkg.Svc.Do")})
	assert.Equal(t, "pkg.Svc.Do", co.method)
	assert.Equal(t, "pkg.Svc.Do", co.methodOr("subject"))
}

func TestCallOptions_MethodDefaultsToSubject(t *testing.T) {
	var co callOptions
	assert.Equal(t, "the.subject", co.methodOr("the.subject"))
}
