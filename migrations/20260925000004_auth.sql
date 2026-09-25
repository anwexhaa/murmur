-- +goose Up
-- +goose StatementBegin

-- Credentials live apart from users on purpose.
--
-- The users table is on the hot read path: BatchGetUsers reads it for every
-- author on every timeline render, tens of rows at a time. A password hash has
-- no business travelling with that, and a separate table means it cannot be
-- swept up by a careless SELECT * or serialised into a cache entry by
-- accident. The cost is one join on the login path, which happens once per
-- session rather than once per page.
CREATE TABLE user_credentials (
    user_id       uuid        PRIMARY KEY REFERENCES users (id) ON DELETE CASCADE,
    -- The full PHC string: algorithm, parameters, salt and hash together.
    -- Storing the parameters beside the hash is what makes them changeable --
    -- an older hash stays verifiable, and the login path can notice it was
    -- made with weaker settings and rewrite it.
    password_hash text        NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Refresh tokens, stored as a hash and grouped into families.
--
-- A family is one rotation chain: log in once, and every refresh that follows
-- replaces the previous token while keeping the family. That grouping is the
-- detection mechanism, not bookkeeping. A refresh token is meant to be used
-- exactly once, so a second use of one that is already spent means a copy
-- exists somewhere it should not -- either the legitimate client is replaying
-- after a failed response, or someone stole it. Both cases are indistinguishable
-- from here, and both are handled the same way: revoke the whole family and
-- make everyone log in again.
CREATE TABLE refresh_tokens (
    id         uuid        PRIMARY KEY,
    user_id    uuid        NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    -- SHA-256 of the token, hex encoded. Deliberately a fast hash, and the
    -- reasoning is the opposite of the password column above: a refresh token
    -- is 256 bits of CSPRNG output, so there is no dictionary to attack and
    -- no guessing to slow down. Argon2 here would spend 64MB and 50ms on
    -- every refresh to defend against an attack that cannot be mounted.
    token_hash text        NOT NULL UNIQUE,
    family_id  uuid        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    -- Set when the token is exchanged. A row is kept after use rather than
    -- deleted, because a deleted token and a never-issued token look the same
    -- on replay, and only one of them means a breach.
    used_at    timestamptz,
    -- Set when the family is revoked, by reuse detection or by logout.
    revoked_at timestamptz
);

-- Rotation reads by hash on every refresh; this is that lookup.
CREATE UNIQUE INDEX refresh_tokens_by_hash ON refresh_tokens (token_hash);

-- Revoking a family touches every row in it.
CREATE INDEX refresh_tokens_by_family ON refresh_tokens (family_id);

-- Logout-everywhere and the expiry sweep both walk a user's tokens.
CREATE INDEX refresh_tokens_by_user ON refresh_tokens (user_id, expires_at);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS refresh_tokens;
DROP TABLE IF EXISTS user_credentials;
-- +goose StatementEnd
