package gateway_test

import (
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/anwexhaa/murmur/api/gen/murmur/events/v1"
	"github.com/anwexhaa/murmur/internal/gateway"
	"github.com/anwexhaa/murmur/internal/platform/bus"
)

// These tests run against a real NATS, because what is being tested is the
// routing: that a publish with no knowledge of where anyone is connected
// reaches the right subscriber, on any replica. A fake bus would only confirm
// that this file agrees with itself.
var testNATSURL string

func TestMain(m *testing.M) {
	flag.Parse()

	testNATSURL = os.Getenv("MURMUR_TEST_NATS_URL")
	if testNATSURL == "" && !testing.Short() {
		log.Println("set MURMUR_TEST_NATS_URL to run the hub tests")
	}

	// goleak at package scope catches a subscription that leaves a goroutine
	// behind on close — which is the failure mode of every hub that forgets to
	// unsubscribe, and is invisible until a process has served a few thousand
	// connections.
	goleak.VerifyTestMain(m,
		// NATS keeps its own connection goroutines alive until Close, and
		// several tests share one connection.
		goleak.IgnoreTopFunction("github.com/nats-io/nats%2ego.(*Conn).waitForMsgs"),
		goleak.IgnoreTopFunction("github.com/nats-io/nats%2ego.(*Conn).readLoop"),
		goleak.IgnoreTopFunction("github.com/nats-io/nats%2ego.(*Conn).flusher"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	)
}

func requireNATS(t *testing.T) *nats.Conn {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test: -short")
	}
	if testNATSURL == "" {
		t.Skip("skipping: MURMUR_TEST_NATS_URL is not set")
	}

	nc, err := nats.Connect(testNATSURL)
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newHub(t *testing.T, nc *nats.Conn, buffer int, maxDrops int64) *gateway.Hub {
	t.Helper()
	return gateway.NewHub(nc, gateway.HubOptions{
		BufferSize: buffer,
		MaxDrops:   maxDrops,
		Log:        quietLogger(),
	})
}

func publish(t *testing.T, nc *nats.Conn, userID, postID string) {
	t.Helper()

	payload, err := proto.Marshal(&eventsv1.PostCreated{
		PostId:    postID,
		AuthorId:  "author",
		CreatedAt: timestamppb.Now(),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := nc.Publish(bus.TimelineSubject(userID), payload); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func receive(t *testing.T, sub *gateway.Subscription, within time.Duration) (gateway.Update, bool) {
	t.Helper()
	select {
	case update := <-sub.Updates:
		return update, true
	case <-time.After(within):
		return gateway.Update{}, false
	}
}

func TestSubscriptionReceivesItsOwnUpdates(t *testing.T) {
	nc := requireNATS(t)
	hub := newHub(t, nc, 16, 100)

	sub, err := hub.Subscribe("alice")
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	defer sub.Close()

	publish(t, nc, "alice", "01POST")

	update, ok := receive(t, sub, 3*time.Second)
	if !ok {
		t.Fatal("no update arrived")
	}
	if update.PostID != "01POST" {
		t.Errorf("PostID = %q, want 01POST", update.PostID)
	}
	if update.Gap {
		t.Error("Gap = true on the first update")
	}
}

// The property that makes the design multi-replica: a hub only hears about the
// users connected to it.
func TestSubscriptionIgnoresOtherUsers(t *testing.T) {
	nc := requireNATS(t)
	hub := newHub(t, nc, 16, 100)

	sub, err := hub.Subscribe("alice")
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	defer sub.Close()

	publish(t, nc, "bob", "01BOBPOST")

	if _, ok := receive(t, sub, 300*time.Millisecond); ok {
		t.Error("alice's subscription received bob's update")
	}
}

// The cross-replica claim, in the only form that can actually be tested: two
// hubs on separate NATS connections, standing in for two gateway processes.
// A publish that knows nothing about either reaches the one with the
// subscriber.
func TestUpdatesCrossReplicas(t *testing.T) {
	publisher := requireNATS(t)

	replicaA := newHub(t, requireNATS(t), 16, 100)
	replicaC := newHub(t, requireNATS(t), 16, 100)

	// Alice is connected to replica C. Nothing anywhere records that fact.
	subC, err := replicaC.Subscribe("alice")
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	defer subC.Close()

	// Bob is connected to replica A, so A has a subscription too and the test
	// is not trivially passing because only one hub exists.
	subA, err := replicaA.Subscribe("bob")
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	defer subA.Close()

	publish(t, publisher, "alice", "01CROSSPOD")

	update, ok := receive(t, subC, 3*time.Second)
	if !ok {
		t.Fatal("the update did not cross to the replica holding the subscription")
	}
	if update.PostID != "01CROSSPOD" {
		t.Errorf("PostID = %q, want 01CROSSPOD", update.PostID)
	}

	if _, ok := receive(t, subA, 200*time.Millisecond); ok {
		t.Error("the update was also delivered to the replica that does not hold alice")
	}
}

// The backpressure policy. A client that stops reading must not block the
// publisher, and must be told it missed something when it starts reading again.
func TestSlowClientIsGappedNotBlocked(t *testing.T) {
	nc := requireNATS(t)

	const buffer = 4
	hub := newHub(t, nc, buffer, 1000)

	sub, err := hub.Subscribe("slow")
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	defer sub.Close()

	// Overfill the buffer without reading. If the handler blocked, this would
	// hang rather than fail.
	for i := range buffer * 5 {
		publish(t, nc, "slow", fmt.Sprintf("01POST%03d", i))
	}
	time.Sleep(200 * time.Millisecond)

	if sub.Dropped() == 0 {
		t.Fatal("nothing was dropped; the buffer should have filled")
	}

	// Drain what fits.
	drained := 0
	for {
		update, ok := receive(t, sub, 200*time.Millisecond)
		if !ok {
			break
		}
		drained++
		_ = update
	}
	if drained > buffer {
		t.Errorf("drained %d updates from a buffer of %d", drained, buffer)
	}

	// The next update that gets through must carry the gap, so the client
	// knows its view is incomplete and refetches.
	publish(t, nc, "slow", "01AFTERGAP")

	update, ok := receive(t, sub, 3*time.Second)
	if !ok {
		t.Fatal("no update after the client resumed reading")
	}
	if !update.Gap {
		t.Error("Gap = false after updates were dropped; the client has no way to know it missed anything")
	}
}

// One slow client must not affect anyone else. This is the reason the NATS
// handler never blocks.
func TestOneSlowClientDoesNotStallAnother(t *testing.T) {
	nc := requireNATS(t)
	hub := newHub(t, nc, 2, 100_000)

	slow, err := hub.Subscribe("slow")
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	defer slow.Close()

	fast, err := hub.Subscribe("fast")
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	defer fast.Close()

	// Bury the slow client, never reading it.
	for i := range 200 {
		publish(t, nc, "slow", fmt.Sprintf("01SLOW%03d", i))
	}

	// The fast client must still be served promptly.
	start := time.Now()
	publish(t, nc, "fast", "01FAST")

	update, ok := receive(t, fast, 3*time.Second)
	if !ok {
		t.Fatal("the fast client received nothing while a slow client was backed up")
	}
	if update.PostID != "01FAST" {
		t.Errorf("PostID = %q, want 01FAST", update.PostID)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("the fast client waited %v behind a slow one", elapsed)
	}
}

// A client that never drains would otherwise hold a buffer and a subscription
// forever while receiving nothing. Past the threshold it is closed, because a
// disconnect is a signal the client can act on and a stalled stream is not.
func TestHopelesslySlowClientIsEvicted(t *testing.T) {
	nc := requireNATS(t)

	const maxDrops = 10
	hub := newHub(t, nc, 2, maxDrops)

	sub, err := hub.Subscribe("hopeless")
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	defer sub.Close()

	for i := range 100 {
		publish(t, nc, "hopeless", fmt.Sprintf("01POST%03d", i))
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !sub.Closed() {
		time.Sleep(20 * time.Millisecond)
	}

	if !sub.Closed() {
		t.Fatalf("a client that dropped %d updates was not evicted", sub.Dropped())
	}
	select {
	case <-sub.Done():
	default:
		t.Error("Done() is not closed on an evicted subscription")
	}
}

func TestConnectionsAreCounted(t *testing.T) {
	nc := requireNATS(t)
	hub := newHub(t, nc, 16, 100)

	if got := hub.Connections(); got != 0 {
		t.Fatalf("Connections() = %d on a new hub, want 0", got)
	}

	subs := make([]*gateway.Subscription, 5)
	for i := range subs {
		sub, err := hub.Subscribe(fmt.Sprintf("user-%d", i))
		if err != nil {
			t.Fatalf("Subscribe() = %v", err)
		}
		subs[i] = sub
	}
	if got := hub.Connections(); got != 5 {
		t.Errorf("Connections() = %d, want 5", got)
	}

	for _, sub := range subs {
		sub.Close()
	}
	if got := hub.Connections(); got != 0 {
		t.Errorf("Connections() = %d after closing every subscription, want 0", got)
	}
}

// Close is reached from both the consumer's defer and the eviction path inside
// a NATS handler, so it has to be safe to call more than once and from more
// than one goroutine.
func TestCloseIsIdempotentAndConcurrencySafe(t *testing.T) {
	nc := requireNATS(t)
	hub := newHub(t, nc, 16, 100)

	sub, err := hub.Subscribe("alice")
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub.Close()
		}()
	}
	wg.Wait()

	if got := hub.Connections(); got != 0 {
		t.Errorf("Connections() = %d after 16 concurrent closes, want 0", got)
	}
}

// Closing while messages are in flight must not panic. This is the case that
// makes the hub deliberately never close the channel it sends on: a NATS
// delivery goroutine may be inside the send at the moment Close runs.
func TestCloseDuringDeliveryDoesNotPanic(t *testing.T) {
	nc := requireNATS(t)
	hub := newHub(t, nc, 1, 100_000)

	sub, err := hub.Subscribe("racy")
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 500 {
			publish(t, nc, "racy", fmt.Sprintf("01POST%03d", i))
		}
	}()

	time.Sleep(20 * time.Millisecond)
	sub.Close()
	<-done
}

func TestSubscribeAfterNATSClosedFails(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test: -short")
	}
	if testNATSURL == "" {
		t.Skip("skipping: MURMUR_TEST_NATS_URL is not set")
	}

	nc, err := nats.Connect(testNATSURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	nc.Close()

	hub := newHub(t, nc, 16, 100)
	if _, err := hub.Subscribe("alice"); err == nil {
		t.Error("Subscribe() = nil on a closed connection, want an error")
	}
}
