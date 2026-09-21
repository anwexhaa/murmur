// Command seed builds a realistic social graph.
//
// Realistic means power-law, not uniform. A graph where everyone has thirty
// followers would make phase 4 meaningless: the whole question there is where
// push fanout stops paying off, and that question only exists because follower
// counts span five orders of magnitude. So this generates a Zipf-distributed
// in-degree, plus an explicit "whale" account large enough to break the naive
// design on purpose.
//
// Phase 4's dataset:
//
//	seed -users 600000 -whale-followers 500000 -avg-following 10
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/anwexhaa/murmur/api/gen/murmur/events/v1"

	"github.com/anwexhaa/murmur/internal/domain"
	"github.com/anwexhaa/murmur/internal/platform/config"
	"github.com/anwexhaa/murmur/internal/platform/db"
	"github.com/anwexhaa/murmur/internal/platform/logging"
	"github.com/anwexhaa/murmur/internal/social"
)

type options struct {
	users          int
	whales         int
	whaleFollowers int
	avgFollowing   int
	postsPerUser   int
	batchSize      int
	writers        int
	reset          bool
	skipEvents     bool
	zipfSkew       float64
	progressEvery  time.Duration
}

func main() {
	if err := run(); err != nil {
		slog.Error("seed failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var opt options
	flag.IntVar(&opt.users, "users", 100_000, "number of users to create")
	flag.IntVar(&opt.whales, "whales", 1, "number of accounts given an explicit large follower count")
	flag.IntVar(&opt.whaleFollowers, "whale-followers", 0, "followers to give each whale (0 disables)")
	flag.IntVar(&opt.avgFollowing, "avg-following", 10, "organic follow edges per user")
	flag.IntVar(&opt.postsPerUser, "posts-per-user", 0, "posts to create per user")
	flag.IntVar(&opt.batchSize, "batch", 20_000, "rows per COPY batch")
	flag.IntVar(&opt.writers, "writers", 4, "concurrent COPY workers")
	flag.BoolVar(&opt.reset, "reset", false, "truncate every table first")
	flag.BoolVar(&opt.skipEvents, "skip-events", false, "do not write outbox events for seeded posts (they will never reach a timeline)")
	flag.Float64Var(&opt.zipfSkew, "skew", 1.2, "Zipf exponent; higher concentrates followers on fewer accounts")
	flag.DurationVar(&opt.progressEvery, "progress", 5*time.Second, "how often to log progress")
	flag.Parse()

	if opt.whaleFollowers >= opt.users {
		return fmt.Errorf("-whale-followers (%d) must be below -users (%d): a whale needs other accounts to follow it",
			opt.whaleFollowers, opt.users)
	}

	cfg, err := config.Load("seed", ":8099")
	if err != nil {
		return err
	}
	log := logging.New(cfg)
	ctx := context.Background()

	pool, closePool, err := db.Open(ctx, cfg.Postgres, log)
	if err != nil {
		return err
	}
	defer closePool()

	store := social.NewStore(pool)

	if opt.reset {
		log.Info("truncating every table")
		if err := store.TruncateAll(ctx); err != nil {
			return err
		}
	}

	started := time.Now()

	users, err := seedUsers(ctx, log, store, opt)
	if err != nil {
		return err
	}
	edges, err := seedFollows(ctx, log, store, opt, users)
	if err != nil {
		return err
	}
	posts, err := seedPosts(ctx, log, store, opt, users)
	if err != nil {
		return err
	}

	log.Info("seed complete",
		"users", len(users),
		"follow_edges", edges,
		"posts", posts,
		"duration", time.Since(started).Round(time.Millisecond),
	)

	// Report the shape that matters for phase 4, not just the totals: a run
	// that produced the right number of edges but a flat distribution has not
	// produced a useful dataset.
	return reportDistribution(ctx, log, store)
}

// ---------------------------------------------------------------- users

func seedUsers(ctx context.Context, log *slog.Logger, store *social.Store, opt options) ([]uuid.UUID, error) {
	log.Info("creating users", "count", opt.users)
	started := time.Now()

	ids := make([]uuid.UUID, opt.users)
	batch := make([]domain.User, 0, opt.batchSize)
	now := time.Now().UTC()

	for i := range opt.users {
		user := domain.User{
			ID:          uuid.New(),
			Handle:      fmt.Sprintf("seed_%08d", i),
			DisplayName: fmt.Sprintf("Seed User %d", i),
			// Spread creation over the past year so ordering by age is
			// meaningful rather than every row sharing one timestamp.
			CreatedAt: now.Add(-time.Duration(opt.users-i) * time.Minute),
		}
		ids[i] = user.ID
		batch = append(batch, user)

		if len(batch) == opt.batchSize {
			if _, err := store.CopyUsers(ctx, batch); err != nil {
				return nil, err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if _, err := store.CopyUsers(ctx, batch); err != nil {
			return nil, err
		}
	}

	log.Info("users created", "count", opt.users, "duration", time.Since(started).Round(time.Millisecond))
	return ids, nil
}

// --------------------------------------------------------- follow graph

func seedFollows(ctx context.Context, log *slog.Logger, store *social.Store, opt options, users []uuid.UUID) (int64, error) {
	log.Info("creating follow edges",
		"whales", opt.whales, "whale_followers", opt.whaleFollowers,
		"avg_following", opt.avgFollowing, "writers", opt.writers)
	started := time.Now()

	batches := make(chan []social.FollowEdge, opt.writers*2)
	group, groupCtx := errgroup.WithContext(ctx)

	var written atomic64
	for range opt.writers {
		group.Go(func() error {
			for batch := range batches {
				n, err := store.CopyFollows(groupCtx, batch)
				if err != nil {
					return err
				}
				written.add(n)
			}
			return nil
		})
	}

	group.Go(func() error {
		defer close(batches)
		return generateEdges(groupCtx, log, opt, users, batches, &written)
	})

	if err := group.Wait(); err != nil {
		return written.load(), err
	}

	total := written.load()
	elapsed := time.Since(started)
	log.Info("follow edges created",
		"count", total,
		"duration", elapsed.Round(time.Millisecond),
		"rows_per_sec", int64(float64(total)/elapsed.Seconds()),
	)
	return total, nil
}

// generateEdges streams batches into the channel rather than building the
// whole edge list first. Phase 4's dataset is six million edges; materialising
// them all would cost hundreds of megabytes for no reason.
func generateEdges(
	ctx context.Context,
	log *slog.Logger,
	opt options,
	users []uuid.UUID,
	batches chan<- []social.FollowEdge,
	written *atomic64,
) error {
	random := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0x6D75726D7572))
	batch := make([]social.FollowEdge, 0, opt.batchSize)

	emit := func() error {
		if len(batch) == 0 {
			return nil
		}
		out := make([]social.FollowEdge, len(batch))
		copy(out, batch)
		batch = batch[:0]
		select {
		case batches <- out:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	add := func(follower, followee uuid.UUID) error {
		batch = append(batch, social.FollowEdge{FollowerID: follower, FolloweeID: followee})
		if len(batch) == opt.batchSize {
			return emit()
		}
		return nil
	}

	// Whales first. Their followers are drawn from the tail of the user list,
	// and organic edges below never target a whale, so no (follower, followee)
	// pair can be produced twice — which matters because a duplicate would
	// abort the entire COPY batch, not just the offending row.
	for whale := range opt.whales {
		if opt.whaleFollowers == 0 {
			break
		}
		followee := users[whale]
		for i := range opt.whaleFollowers {
			follower := users[len(users)-1-i]
			if follower == followee {
				continue
			}
			if err := add(follower, followee); err != nil {
				return err
			}
		}
		log.Info("whale wired", "index", whale, "followers", opt.whaleFollowers)
	}

	// Organic edges. Zipf over the non-whale range gives a power-law
	// in-degree: a few accounts collect thousands of followers, most collect a
	// handful, which is the distribution the phase 4 benchmark needs.
	organicStart := opt.whales
	organicCount := len(users) - organicStart
	if organicCount <= 1 || opt.avgFollowing == 0 {
		return emit()
	}

	zipf := rand.NewZipf(random, opt.zipfSkew, 1, uint64(organicCount-1))
	lastLog := time.Now()

	for i, follower := range users {
		chosen := make(map[int]struct{}, opt.avgFollowing)
		for range opt.avgFollowing {
			target := organicStart + int(zipf.Uint64())
			if target == i {
				continue
			}
			if _, duplicate := chosen[target]; duplicate {
				continue
			}
			chosen[target] = struct{}{}
			if err := add(follower, users[target]); err != nil {
				return err
			}
		}

		if opt.progressEvery > 0 && time.Since(lastLog) >= opt.progressEvery {
			log.Info("generating edges", "follower", i, "of", len(users), "written", written.load())
			lastLog = time.Now()
		}
	}
	return emit()
}

// ---------------------------------------------------------------- posts

func seedPosts(ctx context.Context, log *slog.Logger, store *social.Store, opt options, users []uuid.UUID) (int64, error) {
	if opt.postsPerUser == 0 {
		return 0, nil
	}

	total := len(users) * opt.postsPerUser
	log.Info("creating posts", "count", total)
	started := time.Now()

	ids := domain.NewIDGenerator()
	batch := make([]domain.Post, 0, opt.batchSize)
	var written int64

	// Walk backwards in time so ULIDs come out ascending as we go forwards,
	// which keeps the generated IDs consistent with created_at.
	instant := time.Now().UTC().Add(-time.Duration(total) * time.Millisecond)

	// Seeded posts carry outbox events like any other post, so the relay
	// publishes them and the fanout worker materialises the timelines. Without
	// this, a seeded dataset looks complete in Postgres and every feed is empty.
	events := make([]social.OutboxRecord, 0, opt.batchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := store.CopyPosts(ctx, batch)
		written += n
		if err != nil {
			batch, events = batch[:0], events[:0]
			return err
		}
		if !opt.skipEvents {
			if _, err := store.CopyOutbox(ctx, events); err != nil {
				batch, events = batch[:0], events[:0]
				return err
			}
		}
		batch, events = batch[:0], events[:0]
		return nil
	}

	for _, author := range users {
		for p := range opt.postsPerUser {
			instant = instant.Add(time.Millisecond)
			id, err := ids.New(instant)
			if err != nil {
				return written, err
			}
			batch = append(batch, domain.Post{
				ID:        id,
				AuthorID:  author,
				Body:      fmt.Sprintf("seeded post %d %s", p, strings.Repeat("banter ", 3)),
				CreatedAt: instant,
			})

			if !opt.skipEvents {
				payload, err := proto.Marshal(&eventsv1.PostCreated{
					PostId:    id,
					AuthorId:  author.String(),
					CreatedAt: timestamppb.New(instant),
				})
				if err != nil {
					return written, err
				}
				events = append(events, social.OutboxRecord{
					AggregateID: id,
					Type:        social.PostCreatedType,
					Payload:     payload,
				})
			}
			if len(batch) == opt.batchSize {
				if err := flush(); err != nil {
					return written, err
				}
			}
		}
	}
	if err := flush(); err != nil {
		return written, err
	}

	log.Info("posts created", "count", written, "duration", time.Since(started).Round(time.Millisecond))
	return written, nil
}

// --------------------------------------------------------------- report

// reportDistribution prints the follower-count percentiles.
//
// Totals alone cannot tell you whether a seed run produced a usable dataset: a
// graph with the right number of edges but a flat distribution would make
// phase 4's benchmark measure nothing. The spread is the deliverable, so the
// seeder prints it rather than leaving it to a query someone has to remember.
func reportDistribution(ctx context.Context, log *slog.Logger, store *social.Store) error {
	dist, err := store.FollowerDistribution(ctx)
	if err != nil {
		return err
	}
	log.Info("follower distribution",
		"accounts_with_followers", dist.Accounts,
		"max", dist.Max,
		"p50", dist.P50,
		"p90", dist.P90,
		"p99", dist.P99,
	)
	return nil
}

// atomic64 lets the COPY workers total their rows without contending on a
// mutex for every batch.
type atomic64 struct{ v atomic.Int64 }

func (a *atomic64) add(n int64) { a.v.Add(n) }
func (a *atomic64) load() int64 { return a.v.Load() }
