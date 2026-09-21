-- +goose Up
-- +goose StatementBegin

-- citext gives case-insensitive handles without a lower() index on every
-- lookup, which matters because handle resolution is on the hot read path.
CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE users (
    id           uuid        PRIMARY KEY,
    handle       citext      NOT NULL UNIQUE,
    display_name text        NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT users_handle_shape CHECK (handle ~ '^[A-Za-z0-9_]{2,30}$')
);

CREATE TABLE follows (
    follower_id uuid        NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    followee_id uuid        NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (follower_id, followee_id),
    CONSTRAINT follows_no_self CHECK (follower_id <> followee_id)
);

-- The primary key answers "who does X follow". Fanout asks the opposite
-- question — "who follows X" — several thousand rows at a time, so it needs
-- its own index. Including follower_id keeps that scan index-only.
CREATE INDEX follows_by_followee ON follows (followee_id, follower_id);

CREATE TABLE posts (
    -- ULIDs, not UUIDs: they sort lexicographically by creation time, so the
    -- Redis timeline score falls out of the ID, cursor pagination needs no
    -- second column, and merging two post streams is a string comparison.
    id         text        PRIMARY KEY,
    author_id  uuid        NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    body       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz,
    CONSTRAINT posts_id_is_ulid CHECK (id ~ '^[0-9A-HJKMNP-TV-Z]{26}$'),
    CONSTRAINT posts_body_length CHECK (char_length(body) BETWEEN 1 AND 2000)
);

-- DESC on id, not created_at: the ID already carries the ordering, and one
-- column is cheaper to keep sorted than two.
CREATE INDEX posts_by_author ON posts (author_id, id DESC);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS posts;
DROP TABLE IF EXISTS follows;
DROP TABLE IF EXISTS users;
-- +goose StatementEnd
