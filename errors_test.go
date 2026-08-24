package protonats

import (
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorf(t *testing.T) {
	cases := []struct {
		name string
		err  *Error
		want string
	}{
		{"formatted", Errorf(404, "order %s not found", "abc"), "protonats: 404: order abc not found"},
		{"zero code", Errorf(0, "zero"), "protonats: 0: zero"},
		{"negative code", Errorf(-1, "neg"), "protonats: -1: neg"},
		{"empty message", Errorf(500, ""), "protonats: 500: "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.err.Error())
		})
	}
}

func TestErrorHeaders_Roundtrip(t *testing.T) {
	h := nats.Header{}
	setErrorHeaders(h, Errorf(404, "not found"))

	err := errorFromHeaders(h)
	var pnErr *Error
	require.ErrorAs(t, err, &pnErr)
	assert.Equal(t, 404, pnErr.Code)
	assert.Equal(t, "not found", pnErr.Message)
}

func TestErrorHeaders_PlainErrorBecomes500(t *testing.T) {
	h := nats.Header{}
	setErrorHeaders(h, errors.New("boom"))

	assert.Equal(t, "boom", h.Get("Nats-Service-Error"))
	assert.Equal(t, "500", h.Get("Nats-Service-Error-Code"))
}

func TestErrorHeaders_WrappedStructuredError(t *testing.T) {
	h := nats.Header{}
	setErrorHeaders(h, fmt.Errorf("context: %w", Errorf(403, "denied")))

	assert.Equal(t, "denied", h.Get("Nats-Service-Error"))
	assert.Equal(t, "403", h.Get("Nats-Service-Error-Code"))
}

func TestErrorFromHeaders_Absent(t *testing.T) {
	assert.NoError(t, errorFromHeaders(nats.Header{}))
}

// An error carrying no message must still read as an error. Keying off the
// header's value instead of its presence silently turned such a failure into a
// successful, zero-valued response.
func TestErrorFromHeaders_EmptyMessageIsStillAnError(t *testing.T) {
	h := nats.Header{}
	setErrorHeaders(h, Errorf(404, ""))

	err := errorFromHeaders(h)
	var pnErr *Error
	require.ErrorAs(t, err, &pnErr)
	assert.Equal(t, 404, pnErr.Code)
	assert.Empty(t, pnErr.Message)
}

func TestErrorFromHeaders_BadCodeDefaultsTo500(t *testing.T) {
	cases := map[string]string{
		"non-numeric": "abc",
		"float":       "40.4",
		"empty":       "",
	}
	for name, code := range cases {
		t.Run(name, func(t *testing.T) {
			h := nats.Header{}
			h.Set("Nats-Service-Error", "oops")
			if code != "" {
				h.Set("Nats-Service-Error-Code", code)
			}

			err := errorFromHeaders(h)
			var pnErr *Error
			require.ErrorAs(t, err, &pnErr)
			assert.Equal(t, 500, pnErr.Code)
		})
	}
}

func TestErrTerminate_IsDistinct(t *testing.T) {
	assert.False(t, errors.Is(ErrTerminate, Errorf(500, "x")))
	assert.True(t, errors.Is(fmt.Errorf("wrap: %w", ErrTerminate), ErrTerminate))
}
