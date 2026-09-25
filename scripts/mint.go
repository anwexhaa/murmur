//go:build ignore

// mint issues access tokens from the deployment's signing key.
//
// It exists because the seeded graph has no passwords. cmd/seed writes six
// hundred thousand users with COPY; giving each one a credential would mean
// six hundred thousand argon2 hashes, which is the entire point of argon2
// working against the one place it is not wanted. So the load tests and the
// phase 6 scripts, which drive seeded accounts, need a token they cannot log
// in to obtain.
//
// This is an operator tool holding the signing key, and it can therefore
// impersonate anybody. That is exactly as dangerous as it sounds, which is why
// the key it reads is the development one and why nothing in the deployed
// system does this.
//
//	go run scripts/mint.go -viewer <user-id>
//	go run scripts/mint.go -users users.txt -out tokens.json
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/anwexhaa/murmur/internal/auth"
)

func main() {
	viewer := flag.String("viewer", "", "user id to mint a token for")
	usersPath := flag.String("users", "", "file of user ids, one per line")
	out := flag.String("out", "", "write a JSON object of id -> token here")
	key := flag.String("signing-key", os.Getenv("AUTH_SIGNING_KEY"), "base64 Ed25519 seed")
	ttl := flag.Duration("ttl", time.Hour, "token lifetime")
	flag.Parse()

	if *key == "" {
		log.Fatal("no signing key: set AUTH_SIGNING_KEY or pass -signing-key")
	}
	private, err := auth.DecodeSeed(*key)
	if err != nil {
		log.Fatalf("signing key: %v", err)
	}
	signer, err := auth.NewSigner(private, auth.ServiceGateway)
	if err != nil {
		log.Fatalf("signer: %v", err)
	}

	// A longer lifetime than the gateway issues, on purpose: a load test that
	// runs for twenty minutes should not spend its own runtime refreshing, and
	// nothing here is a credential anybody keeps.
	mint := func(id string) string {
		token, err := signer.Sign(id, "", auth.AudienceClient, *ttl)
		if err != nil {
			log.Fatalf("sign for %s: %v", id, err)
		}
		return token
	}

	switch {
	case *viewer != "":
		fmt.Println(mint(*viewer))

	case *usersPath != "":
		file, err := os.Open(*usersPath)
		if err != nil {
			log.Fatalf("users: %v", err)
		}
		defer file.Close()

		tokens := map[string]string{}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			if id := scanner.Text(); id != "" {
				tokens[id] = mint(id)
			}
		}
		if err := scanner.Err(); err != nil {
			log.Fatalf("read users: %v", err)
		}

		encoded, err := json.Marshal(tokens)
		if err != nil {
			log.Fatalf("encode: %v", err)
		}
		if *out == "" {
			fmt.Println(string(encoded))
			return
		}
		if err := os.WriteFile(*out, encoded, 0o600); err != nil {
			log.Fatalf("write %s: %v", *out, err)
		}
		fmt.Fprintf(os.Stderr, "wrote %d tokens to %s\n", len(tokens), *out)

	default:
		log.Fatal("pass -viewer or -users")
	}
}
