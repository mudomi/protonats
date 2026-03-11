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
