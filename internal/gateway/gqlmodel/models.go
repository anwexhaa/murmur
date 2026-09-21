// Package gqlmodel holds the GraphQL types.
//
// Post and User are hand-written rather than generated because they carry
// fields the schema does not expose. A Post resolved from the social service
// knows its author's ID; the schema exposes an author *object*, resolved
// separately. Without somewhere to keep that ID, the author resolver would
// have nothing to look up.
//
// Everything else in this package is generated into models_gen.go.
package gqlmodel

import "time"

// Post is a post. AuthorID is deliberately not in the GraphQL schema: clients
// ask for `author { ... }` and get an object, which is what makes the author
// lookup a separate round trip — the N+1 this phase exists to measure.
type Post struct {
	ID        string    `json:"id"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"createdAt"`

	AuthorID string `json:"-"`
}

// User is an account.
type User struct {
	ID          string    `json:"id"`
	Handle      string    `json:"handle"`
	DisplayName string    `json:"displayName"`
	CreatedAt   time.Time `json:"createdAt"`
}
