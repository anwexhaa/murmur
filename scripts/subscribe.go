//go:build ignore

// A minimal graphql-transport-ws client, for verifying subscriptions by hand
// and from the phase 6 scripts.
//
// It exists because there is no way to drive a WebSocket subscription with
// curl, and the thing worth verifying — that a post published through one
// gateway replica arrives on a socket held by a different one — cannot be
// checked any other way.
//
//	go run scripts/subscribe.go -url ws://host:8080/query -viewer <id> [-slow]
//
// With -expect it exits non-zero unless that many updates arrive, and it prints
// a latency summary measured from each post's own createdAt, which is what
// makes it usable as an assertion in scripts/realtime.sh rather than only by
// eye.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"time"

	"github.com/coder/websocket"

	"github.com/anwexhaa/murmur/internal/auth"
)

type message struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// payload is the shape of a subscription update, read only for the fields this
// client measures on.
type payload struct {
	Data struct {
		TimelineUpdates struct {
			Gap  bool `json:"gap"`
			Post struct {
				ID        string    `json:"id"`
				CreatedAt time.Time `json:"createdAt"`
			} `json:"post"`
		} `json:"timelineUpdates"`
	} `json:"data"`
}

func main() {
	url := flag.String("url", "ws://localhost:8080/query", "gateway websocket endpoint")
	viewer := flag.String("viewer", "", "user id to subscribe as")
	signingKey := flag.String("signing-key", os.Getenv("AUTH_SIGNING_KEY"), "base64 Ed25519 seed, to mint a token for -viewer")
	after := flag.String("after", "", "last post id already seen, to replay from")
	expect := flag.Int("expect", 0, "exit successfully after this many updates (0 = run until interrupted)")
	slow := flag.Bool("slow", false, "read very slowly, to exercise the backpressure path")
	quiet := flag.Bool("quiet", false, "print progress rather than every update, for high-volume runs")
	timeout := flag.Duration("timeout", 60*time.Second, "give up after this long")
	flag.Parse()

	if *viewer == "" {
		log.Fatal("-viewer is required")
	}

	// The seeded accounts have no passwords, so there is nothing to log in
	// with. See scripts/mint.go.
	token, err := mintToken(*signingKey, *viewer)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	ctx, cancelTimeout := context.WithTimeout(ctx, *timeout)
	defer cancelTimeout()

	conn, _, err := websocket.Dial(ctx, *url, &websocket.DialOptions{
		Subprotocols: []string{"graphql-transport-ws"},
	})
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()

	// A subscription can outlive the default read limit many times over once
	// updates carry post bodies, and a client that silently dies on a large
	// frame looks exactly like a server that stopped delivering.
	conn.SetReadLimit(1 << 20)

	// The token travels in the init payload rather than a header: a browser
	// cannot set headers on a WebSocket upgrade, so anything the connection
	// needs to authenticate with has to go here. The gateway verifies it with
	// the same verifier and the same audience it uses for an HTTP request.
	send(ctx, conn, message{
		Type:    "connection_init",
		Payload: mustJSON(map[string]string{"Authorization": "Bearer " + token}),
	})
	if got := receiveType(ctx, conn); got != "connection_ack" {
		log.Fatalf("expected connection_ack, got %q", got)
	}

	query := `subscription Live($after: String) {
      timelineUpdates(after: $after) {
        gap
        post { id body createdAt author { handle } }
      }
    }`

	variables := map[string]any{}
	if *after != "" {
		variables["after"] = *after
	}

	send(ctx, conn, message{
		ID:   "1",
		Type: "subscribe",
		Payload: mustJSON(map[string]any{
			"query":     query,
			"variables": variables,
		}),
	})

	fmt.Println("subscribed")

	received := 0
	gaps := 0
	var latencies []time.Duration

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			summarise(received, gaps, latencies)
			if *expect > 0 && received >= *expect {
				return
			}
			log.Fatalf("read: %v after %d updates", err, received)
		}

		var msg message
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case "next":
			received++

			// Measured against the post's own createdAt, so the number covers
			// the whole path — outbox relay, JetStream, fanout, NATS, socket —
			// rather than only the part inside the gateway.
			var parsed payload
			if err := json.Unmarshal(msg.Payload, &parsed); err == nil {
				if created := parsed.Data.TimelineUpdates.Post.CreatedAt; !created.IsZero() {
					latencies = append(latencies, time.Since(created))
				}
				if parsed.Data.TimelineUpdates.Gap {
					gaps++
				}
			}

			if *quiet {
				if received%100 == 0 {
					fmt.Printf("received %d\n", received)
				}
			} else {
				fmt.Printf("update %d: %s\n", received, compact(msg.Payload))
			}

			if *slow {
				// Read far more slowly than updates arrive, which is what makes
				// the server's buffer fill and its backpressure policy engage.
				time.Sleep(2 * time.Second)
			}
			if *expect > 0 && received >= *expect {
				summarise(received, gaps, latencies)
				return
			}
		case "error":
			log.Fatalf("subscription error: %s", compact(msg.Payload))
		case "complete":
			summarise(received, gaps, latencies)
			fmt.Printf("stream completed after %d updates\n", received)
			if *expect > 0 && received < *expect {
				os.Exit(1)
			}
			return
		case "ping":
			send(ctx, conn, message{Type: "pong"})
		}
	}
}

// summarise prints what the run measured. Percentiles rather than a mean,
// because the interesting question about a live stream is its tail.
func summarise(received, gaps int, latencies []time.Duration) {
	fmt.Printf("received %d updates, %d carrying a gap\n", received, gaps)
	if len(latencies) == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	fmt.Printf("delivery ms: p50=%.1f p95=%.1f p99=%.1f max=%.1f\n",
		milli(percentile(latencies, 0.50)),
		milli(percentile(latencies, 0.95)),
		milli(percentile(latencies, 0.99)),
		milli(latencies[len(latencies)-1]))
}

func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*q)]
}

func milli(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func send(ctx context.Context, conn *websocket.Conn, msg message) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		log.Fatalf("write %s: %v", msg.Type, err)
	}
}

func receiveType(ctx context.Context, conn *websocket.Conn) string {
	_, data, err := conn.Read(ctx)
	if err != nil {
		log.Fatalf("read: %v", err)
	}
	var msg message
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Fatalf("unmarshal: %v", err)
	}
	return msg.Type
}

func mustJSON(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}
	return data
}

func compact(raw json.RawMessage) string {
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return string(raw)
	}
	data, err := json.Marshal(out)
	if err != nil {
		return string(raw)
	}
	return string(data)
}

// mintToken signs an access token for a seeded account.
func mintToken(signingKey, viewer string) (string, error) {
	if signingKey == "" {
		return "", errors.New("no signing key: set AUTH_SIGNING_KEY or pass -signing-key")
	}
	private, err := auth.DecodeSeed(signingKey)
	if err != nil {
		return "", fmt.Errorf("signing key: %w", err)
	}
	signer, err := auth.NewSigner(private, auth.ServiceGateway)
	if err != nil {
		return "", err
	}
	return signer.Sign(viewer, "", auth.AudienceClient, time.Hour)
}
