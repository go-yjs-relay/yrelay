package yrelay_test

import (
	"bytes"
	"fmt"

	"github.com/go-yjs-relay/yrelay"
)

// Example shows the whole relay in miniature: two clients join the same
// document room and one broadcasts a binary frame to the other. In a
// real server each Membership is fed by a WebSocket goroutine; the
// package imports no WS library, so the same handle plugs into any
// transport.
func Example() {
	hub := yrelay.NewHub()

	alice := hub.Join("doc-42", "alice")
	bob := hub.Join("doc-42", "bob")
	defer alice.Leave()
	defer bob.Leave()

	// Alice fans a Yjs update out to the rest of the room; Bob reads it.
	// Alice never receives her own frame (echo suppression).
	alice.Send([]byte("y-update"))
	fmt.Printf("%s\n", <-bob.Recv())

	// Output: y-update
}

// ExampleMembership_Recv wires a Membership to a generic io-style
// transport: a pump goroutine ranges over Recv() and copies every peer
// frame out to an io.Writer (here a bytes.Buffer; in a server it would
// be the client's socket). The range ends when the membership leaves and
// Recv is closed — draining any frames still buffered first.
func ExampleMembership_Recv() {
	hub := yrelay.NewHub()
	writer := hub.Join("room", "writer")
	reader := hub.Join("room", "reader")
	defer writer.Leave()

	var out bytes.Buffer
	done := make(chan struct{})
	go func() {
		for frame := range reader.Recv() { // io.Writer transport pump
			out.Write(frame)
			out.WriteByte('\n')
		}
		close(done)
	}()

	writer.Send([]byte("hello"))
	writer.Send([]byte("world"))
	reader.Leave() // closes Recv after its buffered frames drain
	<-done

	fmt.Print(out.String())

	// Output:
	// hello
	// world
}
