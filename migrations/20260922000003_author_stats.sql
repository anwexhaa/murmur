-- +goose Up
-- +goose StatementBegin

-- Follower counts, maintained incrementally.
--
-- The fanout worker needs an author's follower count on every single post, to
-- decide whether to push to followers or leave the work to read time. Counting
-- the follows table each time would mean an aggregate over up to half a million
-- rows on the hot write path, to answer a question whose answer barely changes.
--
-- So the count is maintained: incremented and decremented in the same
-- transaction as the edge itself, which keeps it exact rather than eventually
-- right. Bulk loads bypass that path and recompute in one pass instead — see
-- RecomputeAuthorStats.
CREATE TABLE author_stats (
    user_id        uuid        PRIMARY KEY REFERENCES users (id) ON DELETE CASCADE,
    follower_count bigint      NOT NULL DEFAULT 0 CHECK (follower_count >= 0),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

-- Answers "which accounts are above the threshold", which the read path asks
-- every few seconds and the write path asks per post.
--
-- A plain index rather than a partial one: the threshold is configuration, not
-- schema. Baking it into a WHERE clause here would mean a migration every time
-- the benchmark says to move it, and the whole point of phase 4 is that the
-- number is measured and therefore subject to change.
CREATE INDEX author_stats_by_followers ON author_stats (follower_count DESC, user_id);

-- Posts an author wrote, most recent first, materialised in Redis rather than
-- here — this table only tracks who is big enough to need it.

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS author_stats;
-- +goose StatementEnd
