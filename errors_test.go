package protonats

import (
	"fmt"
	"strings"
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


// Edge cases

func TestErrorf_ZeroCode(t *testing.T) {
	err := Errorf(0, "zero code")
	assert.Equal(t, 0, err.Code)
	assert.Equal(t, "zero code", err.Message)
}

func TestErrorf_NegativeCode(t *testing.T) {
	err := Errorf(-1, "negative")
	assert.Equal(t, -1, err.Code)
}

func TestErrorf_LargeCode(t *testing.T) {
	err := Errorf(999999, "large code")
	assert.Equal(t, 999999, err.Code)
}

func TestErrorf_EmptyMessage(t *testing.T) {
	err := Errorf(500, "")
	assert.Equal(t, "", err.Message)
	assert.Contains(t, err.Error(), "500")
}

func TestErrorf_UnicodeMessage(t *testing.T) {
	err := Errorf(400, "ungültige Anfrage: %s", "日本語")
	assert.Equal(t, "ungültige Anfrage: 日本語", err.Message)
}

func TestErrorf_MessageWithNewlines(t *testing.T) {
	err := Errorf(500, "line1\nline2\ttab")
	assert.Equal(t, "line1\nline2\ttab", err.Message)
}

func TestSetErrorHeaders_ZeroCode(t *testing.T) {
	h := nats.Header{}
	setErrorHeaders(h, Errorf(0, "zero"))
	assert.Equal(t, "0", h.Get(headerErrorCode))
	assert.Equal(t, "zero", h.Get(headerError))
}

func TestSetErrorHeaders_NegativeCode(t *testing.T) {
	h := nats.Header{}
	setErrorHeaders(h, Errorf(-1, "negative"))
	assert.Equal(t, "-1", h.Get(headerErrorCode))
}

func TestSetErrorHeaders_WrappedError(t *testing.T) {
	inner := Errorf(403, "forbidden")
	wrapped := fmt.Errorf("outer: %w", inner)
	h := nats.Header{}
	setErrorHeaders(h, wrapped)
	// errors.As should unwrap and find the *Error.
	assert.Equal(t, "403", h.Get(headerErrorCode))
	assert.Equal(t, "forbidden", h.Get(headerError))
}

func TestErrorFromHeaders_NegativeCode(t *testing.T) {
	h := nats.Header{}
	h.Set(headerError, "bad")
	h.Set(headerErrorCode, "-1")
	var pnErr *Error
	require.ErrorAs(t, errorFromHeaders(h), &pnErr)
	assert.Equal(t, -1, pnErr.Code)
}

func TestErrorFromHeaders_ZeroCode(t *testing.T) {
	h := nats.Header{}
	h.Set(headerError, "zero")
	h.Set(headerErrorCode, "0")
	var pnErr *Error
	require.ErrorAs(t, errorFromHeaders(h), &pnErr)
	assert.Equal(t, 0, pnErr.Code)
}

func TestErrorFromHeaders_OverflowCode(t *testing.T) {
	h := nats.Header{}
	h.Set(headerError, "overflow")
	h.Set(headerErrorCode, "99999999999999999999")
	var pnErr *Error
	require.ErrorAs(t, errorFromHeaders(h), &pnErr)
	assert.Equal(t, 500, pnErr.Code) // strconv.Atoi fails → default 500
}

func TestErrorFromHeaders_FloatCode(t *testing.T) {
	h := nats.Header{}
	h.Set(headerError, "float")
	h.Set(headerErrorCode, "3.14")
	var pnErr *Error
	require.ErrorAs(t, errorFromHeaders(h), &pnErr)
	assert.Equal(t, 500, pnErr.Code) // invalid → default 500
}

func TestErrorFromHeaders_MissingCodeHeader(t *testing.T) {
	h := nats.Header{}
	h.Set(headerError, "error without code")
	// No code header set.
	var pnErr *Error
	require.ErrorAs(t, errorFromHeaders(h), &pnErr)
	assert.Equal(t, 500, pnErr.Code)
	assert.Equal(t, "error without code", pnErr.Message)
}

func TestErrorFromHeaders_EmptyMessageHeader(t *testing.T) {
	h := nats.Header{}
	h.Set(headerError, "")
	h.Set(headerErrorCode, "404")
	// Empty error message → returns nil (no error).
	assert.Nil(t, errorFromHeaders(h))
}

func TestErrorHeaders_LongMessage(t *testing.T) {
	long := strings.Repeat("x", 10000)
	h := nats.Header{}
	setErrorHeaders(h, Errorf(500, long))
	var pnErr *Error
	require.ErrorAs(t, errorFromHeaders(h), &pnErr)
	assert.Equal(t, long, pnErr.Message)
}

func TestError_ErrorInterface(t *testing.T) {
	var err error = Errorf(418, "I'm a teapot")
	assert.Equal(t, "protonats: 418: I'm a teapot", err.Error())
}

func TestErrTerminate_IsDistinct(t *testing.T) {
	// ErrTerminate should not be treated as *Error.
	var pnErr *Error
	assert.False(t, assert.ObjectsAreEqual(pnErr, ErrTerminate))
}
