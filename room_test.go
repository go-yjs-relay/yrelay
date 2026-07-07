package yrelay

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestHub_TwoClientsRelayBetween(t *testing.T) {
	h := NewHub()
	a := h.Join("room1", "client-a")
	b := h.Join("room1", "client-b")
	defer a.Leave()
	defer b.Leave()

	// a sends ; b should receive ; a should NOT receive own message
	a.Send([]byte("hello"))

	select {
	case got := <-b.Recv():
		if string(got) != "hello" {
			t.Errorf("b got %q ; want hello", got)
		}
	case <-time.After(time.Second):
		t.Fatal("b did not receive a's message in 1s")
	}

	// Verify a does NOT receive its own message (echo-suppression).
	select {
	case echo := <-a.Recv():
		t.Errorf("a received its own echo : %q", echo)
	case <-time.After(50 * time.Millisecond):
		// good : no echo
	}
}

func TestHub_DifferentRoomsIsolated(t *testing.T) {
	h := NewHub()
	r1 := h.Join("room1", "c1")
	r2 := h.Join("room2", "c2")
	defer r1.Leave()
	defer r2.Leave()

	r1.Send([]byte("private"))

	select {
	case leak := <-r2.Recv():
		t.Errorf("cross-room leak : r2 received %q from r1", leak)
	case <-time.After(50 * time.Millisecond):
		// good
	}
}

func TestHub_GCEmptyRooms(t *testing.T) {
	h := NewHub()
	m := h.Join("room1", "c1")
	if h.roomCount() != 1 {
		t.Errorf("want 1 room after join ; got %d", h.roomCount())
	}
	m.Leave()
	// After last member leaves, the room should be GC'd.
	if h.roomCount() != 0 {
		t.Errorf("want 0 rooms after last leave ; got %d", h.roomCount())
	}
}

func TestHub_MembersCount(t *testing.T) {
	h := NewHub()

	// Unknown room reports 0 (the "room doesn't exist" arm).
	if got := h.MembersCount("ghost"); got != 0 {
		t.Errorf("MembersCount(ghost) = %d ; want 0", got)
	}

	a := h.Join("room", "a")
	if got := h.MembersCount("room"); got != 1 {
		t.Errorf("MembersCount after 1 join = %d ; want 1", got)
	}
	b := h.Join("room", "b")
	if got := h.MembersCount("room"); got != 2 {
		t.Errorf("MembersCount after 2 joins = %d ; want 2", got)
	}

	a.Leave()
	if got := h.MembersCount("room"); got != 1 {
		t.Errorf("MembersCount after 1 leave = %d ; want 1", got)
	}
	b.Leave()
	// Room is GC'd on the last leave, so it reports 0 again.
	if got := h.MembersCount("room"); got != 0 {
		t.Errorf("MembersCount after last leave = %d ; want 0", got)
	}
}

func TestHub_SlowPeerDoesNotBlockHub(t *testing.T) {
	// Slow peer = never reads its Recv channel. The fast peer's
	// Send must not block on the slow peer's full buffer.
	h := NewHub()
	slow := h.Join("room", "slow")
	fast := h.Join("room", "fast")
	defer slow.Leave()
	defer fast.Leave()

	// Send 100 messages from fast ; without back-pressure on slow,
	// this must complete promptly even if slow never reads.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			fast.Send([]byte("x"))
		}
		close(done)
	}()
	select {
	case <-done:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("slow peer blocked hub for >2s")
	}
}

func TestHub_LeaveIdempotent(t *testing.T) {
	h := NewHub()
	m := h.Join("room", "c")
	m.Leave()
	m.Leave() // second call must not panic
}

// TestRoom_RemoveIdempotent drives Room.remove directly a second time,
// after the membership has already left. Membership.Leave short-circuits
// on m.closed, so this white-box call is what exercises the
// "already removed" guard inside remove.
func TestRoom_RemoveIdempotent(t *testing.T) {
	h := NewHub()
	m := h.Join("room", "c")
	m.Leave()
	// m is gone from the room; calling remove again must hit the
	// idempotent early-return and not double-close m.recv / m.closed.
	m.room.remove(m)
}

// TestMembership_LeaveOnContextDone_Cancel covers the ctx.Done arm:
// cancelling the context makes the membership leave, which closes Recv.
func TestMembership_LeaveOnContextDone_Cancel(t *testing.T) {
	h := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	m := h.Join("room", "c")
	m.LeaveOnContextDone(ctx)

	cancel()

	// The leave closes m.recv; a receive on a closed channel returns
	// ok == false. That deterministically signals the leave happened.
	select {
	case _, ok := <-m.Recv():
		if ok {
			t.Fatal("Recv delivered a frame ; expected a close after ctx cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("membership did not leave within 2s of ctx cancel")
	}
	if h.roomCount() != 0 {
		t.Errorf("room not GC'd after context-driven leave : %d", h.roomCount())
	}
}

// TestMembership_LeaveOnContextDone_MembershipLeaves covers the
// <-m.closed arm: when the membership leaves on its own (ctx never
// cancels), the watcher goroutine observes m.closed and exits. We wait
// for the goroutine count to drop back so the branch is deterministically
// executed (no fixed-sleep flake).
func TestMembership_LeaveOnContextDone_MembershipLeaves(t *testing.T) {
	h := NewHub()
	before := runtime.NumGoroutine()

	m := h.Join("room", "c")
	m.LeaveOnContextDone(context.Background()) // never cancels
	m.Leave()                                  // closes m.closed → watcher exits

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatal("LeaveOnContextDone watcher goroutine did not exit")
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
}

func TestHub_ConcurrentJoinSendLeave(t *testing.T) {
	// 50 goroutines each join, send 10 messages, leave. The hub
	// must not race or panic.
	h := NewHub()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m := h.Join("shared", ConnID(string(rune('a'+i%26))))
			for j := 0; j < 10; j++ {
				m.Send([]byte{byte(j)})
			}
			m.Leave()
		}(i)
	}
	wg.Wait()
	if h.roomCount() != 0 {
		t.Errorf("rooms leaked after burst : %d", h.roomCount())
	}
}
