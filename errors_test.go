package protonats

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorf(t *testing.T) {
	err := Errorf(404, "order %s not found", "abc123")
	assert.Equal(t, 404, err.Code)
	assert.Equal(t, "order abc123 not found", err.Message)
	assert.Contains(t, err.Error(), "404")
	assert.Contains(t, err.Error(), "order abc123 not found")
}

func TestErrorHeaders_Structured(t *testing.T) {
	h := nats.Header{}
	setErrorHeaders(h, Errorf(404, "not found"))

	assert.Equal(t, "not found", h.Get(headerError))
	assert.Equal(t, "404", h.Get(headerErrorCode))

	err := errorFromHeaders(h)
	require.NotNil(t, err)
	var pnErr *Error
	require.ErrorAs(t, err, &pnErr)
	assert.Equal(t, 404, pnErr.Code)
	assert.Equal(t, "not found", pnErr.Message)
}

func TestErrorHeaders_PlainError(t *testing.T) {
	h := nats.Header{}
	setErrorHeaders(h, assert.AnError)
	assert.Equal(t, "500", h.Get(headerErrorCode))
}

func TestErrorFromHeaders_Empty(t *testing.T) {
	assert.Nil(t, errorFromHeaders(nats.Header{}))
}

func TestErrorFromHeaders_InvalidCode(t *testing.T) {
	h := nats.Header{}
	h.Set(headerError, "bad")
	h.Set(headerErrorCode, "xyz")

	var pnErr *Error
	require.ErrorAs(t, errorFromHeaders(h), &pnErr)
	assert.Equal(t, 500, pnErr.Code) // defaults to 500
}
