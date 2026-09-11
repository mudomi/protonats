// Command rollback demonstrates a JETSTREAM_TASK: a unit of work declared once
// in the proto and generated as two roles, a worker that does it and a rollback
// that undoes it when it fails for good.
//
// Run a NATS server with JetStream, then run this:
//
//	docker run --rm -p 4222:4222 nats:latest -js
//	go run ./examples/rollback
//
// It plays three orders through the same task — one that succeeds, one that is
// declined outright, and one whose gateway keeps timing out — and prints what
// the runtime does with each.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	protonats "github.com/mudomi/protonats"
	shopgen "github.com/mudomi/protonats/examples/rollback/gen"
)

// The gateway declines this order outright, and times out on this one. Two
// failures that deserve opposite treatment: one is final, the other is worth
// retrying.
const (
	declinedOrder = "ord-declined"
	flakyOrder    = "ord-flaky"

	maxAttempts = 3
	retryWait   = 300 * time.Millisecond
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	if err != nil {
		return fmt.Errorf("connect to %s (is NATS running with -js?): %w", url, err)
	}
	defer nc.Drain()

	// Logging is off: this example prints its own trace, and the runtime's
	// log lines would interleave with it.
	pn, err := protonats.New(nc, protonats.WithLogger(nil))
	if err != nil {
		return err
	}

	if err := ensureStream(pn); err != nil {
		return err
	}

	ledger := &ledger{}

	// ── The payments service: it performs the charge. ──
	//
	// TerminateOnClientError gives up immediately on a 4xx, because a declined
	// card fails identically however many times it is retried. Everything else
	// is retried up to MaxDeliver, and the last failed attempt rolls back.
	worker, err := shopgen.RegisterPaymentsWorker(pn, &paymentsWorker{ledger: ledger},
		protonats.WithRetryPolicy(protonats.TerminateOnClientError),
		protonats.WithConsumerConfig("ChargeCard", jetstream.ConsumerConfig{
			Durable:    "payments",
			MaxDeliver: maxAttempts,
			BackOff:    []time.Duration{retryWait, retryWait},
		}),
		protonats.WithOnTerminate(func(_ context.Context, _, _ string, _ proto.Message, cause error) {
			trace("give up", "rolling back", cause.Error())
		}),
	)
	if err != nil {
		return err
	}
	defer worker.Drain()

	// ── The order service: it owns the undo. ──
	//
	// In a real deployment this runs in a different process; the point is that
	// neither side had to agree on a subject by hand.
	rollback, err := shopgen.RegisterPaymentsRollback(pn, &paymentsRollback{ledger: ledger})
	if err != nil {
		return err
	}
	defer rollback.Drain()

	client := shopgen.NewPaymentsClient(pn)
	for _, order := range []struct {
		id     string
		amount int64
	}{
		{"ord-paid", 2500},
		{declinedOrder, 9900},
		{flakyOrder, 1200},
	} {
		fmt.Println()
		trace("trigger", order.id, fmt.Sprintf("%s to shop.ChargeCard", money(order.amount)))
		if _, err := client.ChargeCard(context.Background(), &shopgen.Charge{
			OrderId:     order.id,
			AmountCents: order.amount,
		}); err != nil {
			return err
		}
		// Long enough for the retries and the rollback to play out.
		time.Sleep(2 * time.Second)
	}

	fmt.Println()
	ledger.report()
	return nil
}

// ── Worker: charges the card ──

type paymentsWorker struct {
	ledger   *ledger
	attempts sync.Map // order id -> *atomic.Int32
}

func (w *paymentsWorker) ChargeCard(_ context.Context, req *shopgen.Charge, _ protonats.Acker) error {
	attempt := w.attempt(req.OrderId)

	switch req.OrderId {
	case declinedOrder:
		// A 4xx: the card is bad, and it will still be bad on every retry.
		// TerminateOnClientError turns this into an immediate rollback — note
		// there is no attempt 2.
		trace("work", req.OrderId, fmt.Sprintf("attempt %d — card declined", attempt))
		return protonats.Errorf(402, "card declined")

	case flakyOrder:
		// The money moves, then confirming it times out. This is the case that
		// makes rollbacks worth having: the task failed, but it already
		// changed state that somebody has to undo.
		//
		// Charging is keyed by order id, so a retry re-charging is harmless —
		// the same property the rollback needs on the other side.
		w.ledger.charge(req.OrderId, req.AmountCents)
		trace("work", req.OrderId, fmt.Sprintf("attempt %d/%d — charged, gateway confirmation timed out", attempt, maxAttempts))
		return fmt.Errorf("payment gateway timeout")

	default:
		w.ledger.charge(req.OrderId, req.AmountCents)
		trace("work", req.OrderId, fmt.Sprintf("attempt %d — charged %s", attempt, money(req.AmountCents)))
		return nil
	}
}

func (w *paymentsWorker) attempt(orderID string) int32 {
	counter, _ := w.attempts.LoadOrStore(orderID, &atomic.Int32{})
	return counter.(*atomic.Int32).Add(1)
}

// ── Rollback: refunds the charge ──

type paymentsRollback struct{ ledger *ledger }

// The rollback receives the message that failed and the error that ended it,
// so it can tell a declined card from a broken gateway.
//
// Rollbacks are delivered at least once, so this must be idempotent — here
// that is the ledger refusing to refund what it never charged.
func (r *paymentsRollback) ChargeCard(_ context.Context, req *shopgen.Charge, cause *protonats.Error, _ protonats.Acker) error {
	refunded := r.ledger.refund(req.OrderId, req.AmountCents)

	detail := fmt.Sprintf("cause %d %s", cause.Code, cause.Message)
	if !refunded {
		detail += " — nothing was charged, so nothing to refund"
	}
	trace("rollback", req.OrderId, detail)
	return nil
}

// ── Ledger: the state a rollback has to undo ──

type ledger struct {
	mu      sync.Mutex
	charged map[string]int64
	events  []string
}

// charge is keyed by order id, so a retried attempt re-charging the same order
// is a no-op rather than a second debit.
func (l *ledger) charge(orderID string, cents int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.charged == nil {
		l.charged = map[string]int64{}
	}
	if _, already := l.charged[orderID]; already {
		return
	}
	l.charged[orderID] = cents
	l.events = append(l.events, fmt.Sprintf("charged  %s %s", orderID, money(cents)))
}

// refund reports whether there was anything to refund, which is what makes a
// repeated rollback harmless.
func (l *ledger) refund(orderID string, cents int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.charged[orderID]; !ok {
		return false
	}
	delete(l.charged, orderID)
	l.events = append(l.events, fmt.Sprintf("refunded %s %s", orderID, money(cents)))
	return true
}

func (l *ledger) report() {
	l.mu.Lock()
	defer l.mu.Unlock()

	fmt.Println("ledger")
	for _, e := range l.events {
		fmt.Printf("  %s\n", e)
	}
	fmt.Printf("  %d order(s) left charged\n", len(l.charged))
}

// ── Output ──

func trace(stage, subject, detail string) {
	fmt.Printf("  %-9s %-14s %s\n", stage, subject, detail)
}

func money(cents int64) string {
	return fmt.Sprintf("$%d.%02d", cents/100, cents%100)
}

// ensureStream creates the stream the task and its rollback both live on.
// protonats creates consumers but never streams, and the rollback subject is
// derived — so the stream has to capture the whole subtree, not just the task
// subject itself.
func ensureStream(pn *protonats.Conn) error {
	js, err := pn.JetStream()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Deleted first so repeated runs start from a clean ledger.
	_ = js.DeleteStream(ctx, "SHOP")
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "SHOP",
		Subjects: []string{"shop.>"},
	})
	if err != nil {
		return fmt.Errorf("create SHOP stream (is JetStream enabled?): %w", err)
	}

	fmt.Println(strings.Repeat("─", 60))
	fmt.Println("  task      shop.ChargeCard")
	fmt.Println("  rollback  " + protonats.RollbackSubjectFor("shop.ChargeCard") + "   (derived)")
	fmt.Println(strings.Repeat("─", 60))
	return nil
}
