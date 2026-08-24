// Command interop is the Go half of the cross-language wire-compatibility
// test driven by ts/runtime/src/interop.test.ts. It is a test fixture, not a
// user-facing binary.
//
// It serves protonats handlers for the TypeScript client to call, and on
// request acts as a protonats client against the TypeScript handlers, so one
// process covers both directions. google.protobuf.StringValue is the payload
// because both runtimes ship it as a well-known type, which keeps the fixture
// free of a generated-code build step.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	protonats "github.com/mudomi/protonats"
)

// Subjects served by this process, and those it calls on the TypeScript side.
const (
	subjectEcho       = "interop.go.echo"
	subjectFail       = "interop.go.fail"
	subjectEmptyError = "interop.go.emptyerror"

	subjectTSEcho = "interop.ts.echo"
	subjectTSFail = "interop.ts.fail"
)

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	pn, err := protonats.New(nc)
	if err != nil {
		log.Fatalf("protonats: %v", err)
	}

	if err := serve(pn); err != nil {
		log.Fatalf("serve: %v", err)
	}

	// Flush before announcing readiness: the TypeScript side starts calling as
	// soon as it sees this line, and an unflushed subscription would miss.
	if err := nc.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}
	fmt.Println("READY")

	// One command per line, so the TypeScript test can register its own
	// handlers before asking this process to call them.
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		switch scanner.Text() {
		case "CALL":
			callTypeScript(pn)
		case "QUIT":
			return
		}
	}
}

func serve(pn *protonats.Conn) error {
	newString := func() proto.Message { return &wrapperspb.StringValue{} }

	handlers := []struct {
		subject string
		handle  protonats.HandlerInvoker
	}{
		{subjectEcho, func(_ context.Context, req proto.Message) (proto.Message, error) {
			return wrapperspb.String(req.(*wrapperspb.StringValue).Value + " (from go)"), nil
		}},
		{subjectFail, func(_ context.Context, _ proto.Message) (proto.Message, error) {
			return nil, protonats.Errorf(404, "gone from go")
		}},
		// A structured error carrying no message: the case that used to reach
		// the other runtime as a success.
		{subjectEmptyError, func(_ context.Context, _ proto.Message) (proto.Message, error) {
			return nil, protonats.Errorf(418, "")
		}},
	}

	for _, h := range handlers {
		if _, err := pn.Subscribe(h.subject, h.subject, protonats.HandlerOptions{}, newString, h.handle); err != nil {
			return fmt.Errorf("subscribe %s: %w", h.subject, err)
		}
	}
	return nil
}

// callTypeScript exercises the Go client against the TypeScript handlers,
// reporting each result on stdout for the test to assert on.
func callTypeScript(pn *protonats.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := &wrapperspb.StringValue{}
	if err := pn.Request(ctx, subjectTSEcho, wrapperspb.String("hello"), resp); err != nil {
		fmt.Printf("ECHO ERR %v\n", err)
	} else {
		fmt.Printf("ECHO OK %s\n", resp.Value)
	}

	err := pn.Request(ctx, subjectTSFail, wrapperspb.String("x"), &wrapperspb.StringValue{})
	var pnErr *protonats.Error
	switch {
	case err == nil:
		fmt.Println("FAIL ERR expected an error, got success")
	case !errors.As(err, &pnErr):
		fmt.Printf("FAIL ERR not a protonats error: %v\n", err)
	default:
		fmt.Printf("FAIL OK %d %s\n", pnErr.Code, pnErr.Message)
	}
}
