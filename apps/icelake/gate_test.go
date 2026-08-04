package main

import (
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// gate is a TCP forwarder in front of the object store that a test can close and
// open again.
//
// It is not a mock and stands in for nothing: the store on the other side is the
// same real MinIO every other test writes to, and every byte passes through
// untouched. What it controls is the network, which is the input a test needs
// when the claim under test is "the daemon holds stdin while the bucket is
// unreachable, and resumes when it comes back". That claim is unobservable
// against a substrate that always answers.
//
// Signed requests survive it because the signature covers the Host header the
// client sent and this forwards that header unchanged, so the store never has to
// believe it is being addressed by its own name.
type gate struct {
	// Endpoint is the base URL to configure the daemon with.
	Endpoint string

	listener net.Listener
	target   string
	reject   atomic.Bool

	wg   sync.WaitGroup
	done chan struct{}
	once sync.Once

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

// startGate puts a gate in front of an endpoint and stops it when the test ends.
func startGate(tb testing.TB, endpoint string) *gate {
	tb.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("starting the gate: %v", err)
	}

	g := &gate{
		Endpoint: "http://" + listener.Addr().String(),
		listener: listener,
		target:   strings.TrimPrefix(endpoint, "http://"),
		done:     make(chan struct{}),
		conns:    make(map[net.Conn]struct{}),
	}

	g.wg.Add(1)
	go g.accept()

	tb.Cleanup(func() {
		g.once.Do(func() { close(g.done) })
		_ = listener.Close()
		// Live connections are closed rather than waited on: an HTTP client
		// keeps its connections idle for its own keep-alive window, so a gate
		// that only stopped accepting would sit here long after the test.
		g.hangUp()
		g.wg.Wait()
	})

	return g
}

// SetReject closes the gate or opens it again.
//
// Closing it severs every connection currently open, which is not optional: the
// client pools its connections, so a gate applied only at accept time would be
// skipped entirely by a request travelling on one opened before it closed.
// Without that, "from here on nothing reaches the store" would be true only when
// no warm connection happened to be lying around, which is an accident of timing
// rather than a property a test can rest on.
func (g *gate) SetReject(reject bool) {
	g.reject.Store(reject)
	if reject {
		g.hangUp()
	}
}

// hangUp closes every connection the gate currently holds, in both directions.
func (g *gate) hangUp() {
	g.mu.Lock()
	defer g.mu.Unlock()

	for conn := range g.conns {
		_ = conn.Close()
	}
}

// accept serves connections until the listener closes.
func (g *gate) accept() {
	defer g.wg.Done()

	for {
		conn, err := g.listener.Accept()
		if err != nil {
			return
		}
		g.wg.Add(1)
		go g.forward(conn)
	}
}

// track registers a connection so a close can reach it, and returns the function
// that unregisters and closes it.
func (g *gate) track(conn net.Conn) func() {
	g.mu.Lock()
	g.conns[conn] = struct{}{}
	g.mu.Unlock()

	return func() {
		g.mu.Lock()
		delete(g.conns, conn)
		g.mu.Unlock()
		_ = conn.Close()
	}
}

// forward pipes one connection to the store, or hangs up if the gate is closed.
func (g *gate) forward(client net.Conn) {
	defer g.wg.Done()
	defer g.track(client)()

	if g.reject.Load() {
		return
	}

	upstream, err := net.Dial("tcp", g.target)
	if err != nil {
		return
	}
	defer g.track(upstream)()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(upstream, client)
		if c, ok := upstream.(*net.TCPConn); ok {
			_ = c.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(client, upstream)
		if c, ok := client.(*net.TCPConn); ok {
			_ = c.CloseWrite()
		}
	}()
	wg.Wait()
}
