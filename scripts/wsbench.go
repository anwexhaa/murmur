//go:build ignore

// wsbench opens many subscriptions at once, to find out what a single gateway
// replica costs per connected client and whether delivery holds up under load.
//
// The question it answers is not "how fast is one update" — subscribe.go
// measures that — but "what happens to that number when a thousand sockets are
// open", which is the only version of the question a capacity plan cares about.
//
//	go run scripts/wsbench.go -url ws://host:8080/query -viewers users.txt \
//	    -connections 1000 -hold 60s
//
// Viewers are used round-robin, so N connections over M users means N/M sockets
// per user and each published post is delivered N/M times. That is deliberate:
// it is the multi-device case, and it keeps NATS delivering to many local
// subscriptions rather than one.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/anwexhaa/murmur/internal/auth"
)

type message struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type payload struct {
	Data struct {
		TimelineUpdates struct {
			Gap  bool `json:"gap"`
			Post struct {
				CreatedAt time.Time `json:"createdAt"`
			} `json:"post"`
		} `json:"timelineUpdates"`
	} `json:"data"`
}

const subscribeQuery = `subscription Live {
  timelineUpdates {
    gap
    post { id createdAt }
  }
}`

func main() {
	url := flag.String("url", "ws://localhost:8080/query", "gateway websocket endpoint")
	viewersPath := flag.String("viewers", "", "file of user ids, one per line")
	connections := flag.Int("connections", 100, "sockets to open")
	rampRate := flag.Int("ramp", 200, "connections to open per second")
	hold := flag.Duration("hold", 30*time.Second, "how long to hold the connections open")
	signingKey := flag.String("signing-key", os.Getenv("AUTH_SIGNING_KEY"), "base64 Ed25519 seed, to mint tokens for the viewers")
	flag.Parse()

	viewers, err := readLines(*viewersPath)
	if err != nil {
		log.Fatalf("viewers: %v", err)
	}
	if len(viewers) == 0 {
		log.Fatal("-viewers must name a file with at least one user id")
	}

	// One signer, thousands of tokens: the seeded accounts have no passwords
	// to log in with. See scripts/mint.go.
	if *signingKey == "" {
		log.Fatal("no signing key: set AUTH_SIGNING_KEY or pass -signing-key")
	}
	private, err := auth.DecodeSeed(*signingKey)
	if err != nil {
		log.Fatalf("signing key: %v", err)
	}
	signer, err := auth.NewSigner(private, auth.ServiceGateway)
	if err != nil {
		log.Fatalf("signer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		opened   atomic.Int64
		failed   atomic.Int64
		updates  atomic.Int64
		mu       sync.Mutex
		latency  []time.Duration
		wg       sync.WaitGroup
		firstErr atomic.Value
	)

	// Ramping rather than opening everything at once: a thundering herd
	// measures the accept queue, not the steady state, and the steady state is
	// the thing a capacity number is about.
	ticker := time.NewTicker(time.Second / time.Duration(max(1, *rampRate)))
	defer ticker.Stop()

	start := time.Now()
	for i := 0; i < *connections; i++ {
		<-ticker.C
		viewer := viewers[i%len(viewers)]

		wg.Add(1)
		go func() {
			defer wg.Done()

			token, err := signer.Sign(viewer, "", auth.AudienceClient, time.Hour)
			if err != nil {
				failed.Add(1)
				firstErr.CompareAndSwap(nil, err)
				return
			}

			conn, err := open(ctx, *url, token)
			if err != nil {
				failed.Add(1)
				firstErr.CompareAndSwap(nil, err)
				return
			}
			defer conn.CloseNow()
			opened.Add(1)

			local := make([]time.Duration, 0, 8)
			for {
				_, data, err := conn.Read(ctx)
				if err != nil {
					break
				}
				var msg message
				if json.Unmarshal(data, &msg) != nil {
					continue
				}
				switch msg.Type {
				case "next":
					updates.Add(1)
					var parsed payload
					if json.Unmarshal(msg.Payload, &parsed) == nil {
						if created := parsed.Data.TimelineUpdates.Post.CreatedAt; !created.IsZero() {
							local = append(local, time.Since(created))
						}
					}
				case "ping":
					_ = write(ctx, conn, message{Type: "pong"})
				}
			}

			mu.Lock()
			latency = append(latency, local...)
			mu.Unlock()
		}()
	}

	fmt.Printf("ramped %d connections in %s\n", *connections, time.Since(start).Round(time.Millisecond))
	fmt.Printf("connected=%d failed=%d\n", opened.Load(), failed.Load())
	if err, ok := firstErr.Load().(error); ok && err != nil {
		fmt.Printf("first failure: %v\n", err)
	}

	time.Sleep(*hold)
	fmt.Printf("held %s, updates=%d\n", *hold, updates.Load())

	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(latency) == 0 {
		return
	}
	sort.Slice(latency, func(i, j int) bool { return latency[i] < latency[j] })
	fmt.Printf("delivery ms over %d updates: p50=%.1f p95=%.1f p99=%.1f max=%.1f\n",
		len(latency),
		milli(percentile(latency, 0.50)),
		milli(percentile(latency, 0.95)),
		milli(percentile(latency, 0.99)),
		milli(latency[len(latency)-1]))
}

func open(ctx context.Context, url, token string) (*websocket.Conn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(dialCtx, url, &websocket.DialOptions{
		Subprotocols: []string{"graphql-transport-ws"},
	})
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(1 << 20)

	if err := write(dialCtx, conn, message{
		Type:    "connection_init",
		Payload: mustJSON(map[string]string{"Authorization": "Bearer " + token}),
	}); err != nil {
		conn.CloseNow()
		return nil, err
	}
	if _, _, err := conn.Read(dialCtx); err != nil {
		conn.CloseNow()
		return nil, err
	}

	if err := write(dialCtx, conn, message{
		ID:      "1",
		Type:    "subscribe",
		Payload: mustJSON(map[string]any{"query": subscribeQuery}),
	}); err != nil {
		conn.CloseNow()
		return nil, err
	}
	return conn, nil
}

func write(ctx context.Context, conn *websocket.Conn, msg message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if line := scanner.Text(); line != "" {
			lines = append(lines, line)
		}
	}
	return lines, scanner.Err()
}

func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*q)]
}

func milli(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func mustJSON(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}
	return data
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
