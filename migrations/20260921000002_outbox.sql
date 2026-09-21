-- +goose Up
-- +goose StatementBegin

-- The transactional outbox.
--
-- Writing a post and publishing post.created are two different systems. Do
-- them separately and there is a window where the post exists but no timeline
-- ever hears about it: the commit succeeded, the publish failed, and nothing
-- anywhere records that the fanout is owed. That bug is silent, unreproducible
-- and permanent.
--
-- Writing the event into the same transaction as the post closes the window.
-- A relay then moves rows from here to the bus, which converts the problem
-- from "might be lost" into "might be delivered twice" — and duplicate
-- delivery is something the fanout worker is built to tolerate, because ZADD
-- of the same member is a no-op.
CREATE TABLE outbox (
    id           bigserial   PRIMARY KEY,
    aggregate_id text        NOT NULL,
    type         text        NOT NULL,
    -- bytea, not jsonb: the payload is the encoded protobuf message, stored
    -- byte for byte as it will be published. Keeping it in its wire form means
    -- there is no transformation between what was committed and what was
    -- delivered, and therefore no way for the two to drift. jsonb would be
    -- nicer to read in psql and would cost exactly that guarantee — the `type`
    -- column is enough to know how to decode a row when it matters.
    payload      bytea       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    attempts     int         NOT NULL DEFAULT 0
);

-- A partial index on exactly the rows the relay claims.
--
-- The outbox is almost entirely published rows — it is a log, and the
-- unpublished tail is a handful of entries at any moment. Indexing only that
-- tail keeps the index the size of the backlog rather than the size of
-- history, so the relay's claim query stays fast no matter how many millions
-- of events have already been delivered.
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS outbox;
-- +goose StatementEnd
