package protonats

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
)

const (
	headerError     = "Nats-Service-Error"
	headerErrorCode = "Nats-Service-Error-Code"
)

// Error is a structured error with a numeric code, compatible with the NATS Service API.
type Error struct {
	Code    int
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("protonats: %d: %s", e.Code, e.Message)
}

// Errorf creates a structured error with code and formatted message.
func Errorf(code int, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// ErrTerminate signals that a JetStream message should be terminated (no redelivery).
var ErrTerminate = fmt.Errorf("protonats: terminate message")

func setErrorHeaders(h nats.Header, err error) {
	var pnErr *Error
	if errors.As(err, &pnErr) {
		h.Set(headerError, pnErr.Message)
		h.Set(headerErrorCode, strconv.Itoa(pnErr.Code))
	} else {
		h.Set(headerError, err.Error())
		h.Set(headerErrorCode, "500")
	}
}

func errorFromHeaders(h nats.Header) error {
	// Presence of the header, not its value, marks a failure: a handler may
	// return an error whose message is empty, and testing the value would
	// hand the caller a zero-valued response and a nil error instead.
	values, failed := h[headerError]
	if !failed {
		return nil
	}
	var msg string
	if len(values) > 0 {
		msg = values[0]
	}

	code := 500
	if c := h.Get(headerErrorCode); c != "" {
		if parsed, err := strconv.Atoi(c); err == nil {
			code = parsed
		}
	}
	return &Error{Code: code, Message: msg}
}
