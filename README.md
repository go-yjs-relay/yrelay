<p align="center"><img src="https://raw.githubusercontent.com/go-yjs-relay/brand/main/social/go-yjs-relay-yrelay.png" alt="go-yjs-relay/yrelay" width="720"></p>

# yrelay — go-yjs-relay

[![Docs](https://img.shields.io/badge/docs-mkdocs--material-14B8A6)](https://go-yjs-relay.github.io/docs/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](https://go.dev/dl/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-1a7f37)](#tests--coverage)

**A pure-Go (no cgo) server-side [Yjs](https://github.com/yjs/yjs)
[y-websocket](https://github.com/yjs/y-websocket) relay hub.** It multiplexes
collaborative-editing connections into rooms and fans each client's binary
frames out to the rest of the room — the exact relay-only deployment mode the
upstream y-websocket server uses.

It is **transport-agnostic**: the package imports no WebSocket library. The
caller owns the socket and pushes/pulls binary frames through a `Membership`
handle, so `yrelay` drops into any Go WebSocket stack (`nhooyr.io/websocket`,
`gorilla/websocket`, `net/http`'s own upgrader, or an in-memory pipe in tests).

> **A relay, not a server-side CRDT.** `yrelay` does **not** decode Yjs updates
> server-side. The clients reconcile among themselves via Yjs's update +
> sync-step algebra; the server only shuttles frames. That is what makes it
> **forward-compatible** with new y-protocols versions for free and keeps it a
> few hundred lines instead of a several-thousand-line Go Yjs implementation.

## Why a relay (and not a server-side CRDT)

- The y-protocols spec is binary + versioned; clients evolve and a relay is
  forward-compatible with new protocol versions at no cost.
- Server-side state would need a full Go Yjs implementation — a large project on
  its own.
- Relay-only is the deployment mode the upstream y-websocket server uses too;
  persistence is bolted on downstream by serialising the last update from any one
  client to a backing store.

## Features

- **Room multiplexing** — one `Room` per document, keyed by room ID; `Hub.Join`
  returns a `Membership` handle.
- **Fan-out broadcast** — every frame a member `Send`s is delivered to every
  *other* member's `Recv` channel (echo-suppressed; a client never sees its own
  frame).
- **Slow-peer drop** — a member whose receive buffer is full has the frame
  dropped rather than blocking the hub; Yjs re-syncs it via the next sync-step
  handshake. One straggler never stalls the room.
- **Automatic room GC** — the room is reclaimed when its last member leaves.
- **Context-driven leave** — `Membership.LeaveOnContextDone(ctx)` unregisters the
  connection when its request context cancels, saving the caller a goroutine.
- **Idempotent `Leave`** — safe to call from both a deferred cleanup and a
  context watcher.

Concurrency-safe by design (one goroutine per connection reads the socket and
`Send`s; the fan-out runs under a per-room lock), CGO-free, dependency-free
(stdlib `context` + `sync` only), **100% test coverage** under `-race`, `gofmt` +
`go vet` clean, and green across the six 64-bit Go targets (amd64, arm64,
riscv64, loong64, ppc64le, s390x).

## Install

```sh
go get github.com/go-yjs-relay/yrelay
```

## Usage

```go
package main

import (
	"fmt"

	"github.com/go-yjs-relay/yrelay"
)

func main() {
	hub := yrelay.NewHub()

	alice := hub.Join("doc-42", "alice")
	bob := hub.Join("doc-42", "bob")
	defer alice.Leave()
	defer bob.Leave()

	// Alice fans a Yjs update out to the room; Bob reads it. Alice never
	// receives her own frame.
	alice.Send([]byte("y-update"))
	fmt.Printf("%s\n", <-bob.Recv()) // y-update
}
```

Wiring a `Membership` to a real WebSocket is two goroutines — a reader that
`Send`s inbound socket frames into the room, and a writer that ranges over
`Recv()` and writes peer frames back out:

```go
func serve(hub *yrelay.Hub, roomID string, conn Socket) {
	m := hub.Join(roomID, yrelay.ConnID(conn.RemoteAddr()))
	m.LeaveOnContextDone(conn.Context()) // auto-leave when the request ends
	defer m.Leave()

	// writer: peer frames -> this socket
	go func() {
		for frame := range m.Recv() {
			_ = conn.WriteBinary(frame)
		}
	}()

	// reader: this socket -> the room
	for {
		frame, err := conn.ReadBinary()
		if err != nil {
			return
		}
		m.Send(frame)
	}
}
```

## API

```go
// Hub multiplexes connections by room ID. One Hub per server instance.
func NewHub() *Hub
func (h *Hub) Join(roomID string, conn ConnID) *Membership
func (h *Hub) MembersCount(roomID string) int

// ConnID is the opaque connection identifier the caller passes in.
type ConnID string

// Membership is one client's view into a Room. Returned by Hub.Join.
func (m *Membership) Recv() <-chan []byte              // peer frames (closed on leave)
func (m *Membership) Send(payload []byte)              // fan out to the rest of the room
func (m *Membership) Leave()                           // unregister (idempotent)
func (m *Membership) LeaveOnContextDone(ctx context.Context)
```

## Tests & coverage

Pure concurrent Go, tested with the race detector and gated at 100% coverage
including every error/edge branch — double-`Leave` idempotency, room GC on the
last leave, drop-on-full-buffer, and both arms of `LeaveOnContextDone`:

```sh
COVERPKG=$(go list ./... | paste -sd, -)
go test -race -coverpkg="$COVERPKG" -coverprofile=cover.out ./...
go tool cover -func=cover.out | tail -1   # 100.0%
```

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright the go-yjs-relay/yrelay authors.
