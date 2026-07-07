// Package yrelay implements the server side of the Yjs y-websocket
// protocol as a RELAY. We don't decode CRDT updates server-side — the
// clients sort it out among themselves via Yjs's update + sync-step
// algebra. Our job :
//
//   - track rooms (one per project document)
//   - when a client joins, broadcast their incoming binary frames to
//     every OTHER client in the same room
//   - when a client emits a "sync-step-1" handshake, the existing
//     clients reply with their current state ; we don't intercept
//   - on the last client leaving, the room is GC'd (state is gone —
//     V0.1 has no persistence ; V0.2 will snapshot to a backing store)
//
// Why a relay (and not a server-side CRDT) :
//
//   - the y-protocols spec is binary + versioned ; clients evolve and
//     a relay is forward-compatible with new protocol versions for
//     free
//   - server-side state needs a Go Yjs implementation, which is a
//     several-thousand-line project on its own
//   - relay-only is the deployment mode the upstream y-websocket
//     server uses too ; persistence is bolted on by serialising the
//     last update from any one client to a backing store
//
// Transport-agnostic : this package imports no WebSocket library. The
// caller owns the socket and pushes/pulls binary frames through the
// [Membership] handle — [Membership.Send] to fan a frame out to the
// room, [Membership.Recv] to read the frames peers send. That keeps
// yrelay usable with any Go WebSocket stack (nhooyr/websocket,
// gorilla, net/http's own upgrader, an in-memory pipe in tests…). See
// the package example for a concrete transport wiring.
//
// Concurrency : one goroutine per connection reads from the socket
// and pushes onto the hub's broadcast channel ; the hub fan-out
// goroutine pushes to every peer's send channel. Slow peers get
// dropped (we never block the hub on one straggler).
package yrelay

import (
	"context"
	"sync"
)

// Hub multiplexes connections by RoomID. One Hub per server instance.
type Hub struct {
	mu    sync.Mutex
	rooms map[string]*Room
}

// NewHub builds an empty Hub. Long-lived ; no Shutdown method — the
// process exit takes everything with it. Per-room cleanup happens on
// the last-client-leaves path.
func NewHub() *Hub {
	return &Hub{rooms: map[string]*Room{}}
}

// Join inserts the connection into the named room (creating the room
// if needed) and returns a handle the caller uses to read + write +
// leave. The handle's Recv channel delivers every binary frame from
// peers ; the caller writes their outbound frames via Send. Leave
// unregisters.
func (h *Hub) Join(roomID string, conn ConnID) *Membership {
	h.mu.Lock()
	r, ok := h.rooms[roomID]
	if !ok {
		r = newRoom(roomID, h)
		h.rooms[roomID] = r
	}
	h.mu.Unlock()
	return r.add(conn)
}

// MembersCount returns the number of connections currently in the
// named room (0 when the room doesn't exist). Used by the SSE log /
// the observability layer to surface joining + leaving peers — the
// count IS the bidirectional-sync precondition (need ≥ 2 members for
// any cross-client frame to flow).
func (h *Hub) MembersCount(roomID string) int {
	h.mu.Lock()
	r, ok := h.rooms[roomID]
	h.mu.Unlock()
	if !ok {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.members)
}

// gcRoom removes a room when its last member leaves. Called by Room
// itself with the room lock held internally — the Hub lock is what
// we take here to serialise the map mutation.
func (h *Hub) gcRoom(roomID string) {
	h.mu.Lock()
	delete(h.rooms, roomID)
	h.mu.Unlock()
}

// roomCount returns the number of live rooms. Goes through h.mu so
// tests and future Prometheus exporters don't race against
// concurrent Join/gcRoom.
func (h *Hub) roomCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.rooms)
}

// ConnID is the opaque connection identifier the caller passes in.
// Typically the remote-addr ; only used for log lines + tie-breaking
// in awareness.
type ConnID string

// Room is one collaborative document. All members see the same
// broadcast feed.
type Room struct {
	id  string
	hub *Hub

	mu      sync.Mutex
	members map[*Membership]struct{}
}

func newRoom(id string, h *Hub) *Room {
	return &Room{
		id:      id,
		hub:     h,
		members: map[*Membership]struct{}{},
	}
}

func (r *Room) add(conn ConnID) *Membership {
	m := &Membership{
		room:   r,
		conn:   conn,
		recv:   make(chan []byte, 32),
		closed: make(chan struct{}),
	}
	r.mu.Lock()
	r.members[m] = struct{}{}
	r.mu.Unlock()
	return m
}

func (r *Room) remove(m *Membership) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[m]; !ok {
		// Already removed ; idempotent.
		return
	}
	delete(r.members, m)
	// Close recv + closed UNDER the lock so any concurrent broadcast
	// (which also takes r.mu) is fully serialised — no chance of a
	// "send on closed channel" panic.
	close(m.recv)
	close(m.closed)
	if len(r.members) == 0 {
		// gcRoom takes the hub lock but never the room lock, so the
		// lock ordering is fine.
		r.hub.gcRoom(r.id)
	}
}

// broadcast pushes payload to every member EXCEPT sender. Slow
// receivers (full send buffer) get the frame dropped — Yjs's
// protocol recovers via the sync-step handshake on the next message.
//
// The whole iteration runs under r.mu so a concurrent Leave can't
// close m.recv mid-send. Sends are non-blocking (default arm) so
// the critical section stays tight — microseconds even at 100s of
// members.
func (r *Room) broadcast(payload []byte, sender *Membership) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for m := range r.members {
		if m == sender {
			continue
		}
		select {
		case m.recv <- payload:
		default:
			// Slow peer ; drop. They'll re-sync via the protocol's
			// state-vector exchange.
		}
	}
}

// Membership is one client's view into a Room. Returned by Hub.Join.
type Membership struct {
	room *Room
	conn ConnID
	// recv is the inbound feed : frames from OTHER members in the
	// room. The caller's writer goroutine ranges over this channel
	// and pushes to the WebSocket.
	recv   chan []byte
	closed chan struct{}
}

// Recv returns the channel of peer frames. Closed when the
// Membership leaves.
func (m *Membership) Recv() <-chan []byte { return m.recv }

// Send broadcasts the binary frame to the rest of the room. Safe
// concurrent with Recv on the same Membership.
func (m *Membership) Send(payload []byte) {
	m.room.broadcast(payload, m)
}

// Leave removes the membership from its room and closes Recv.
// Idempotent ; second call is a no-op.
func (m *Membership) Leave() {
	select {
	case <-m.closed:
		return
	default:
		m.room.remove(m)
	}
}

// LeaveOnContextDone wires the Membership to ctx — when ctx cancels,
// the membership leaves automatically. Saves the caller a goroutine.
func (m *Membership) LeaveOnContextDone(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			m.Leave()
		case <-m.closed:
		}
	}()
}
