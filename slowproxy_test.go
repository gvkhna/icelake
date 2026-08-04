package icelake_test

import (
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowProxy is a TCP forwarder that delays every request it carries to the
// object store.
//
// It is not a mock and does not stand in for anything: the substrate on the
// other side is the same real MinIO every other test writes to, and every byte
// is passed through untouched. What it controls is the network, which is the
// input a test needs when the claim under test is "a flush does not block an
// insert" or "a shutdown that runs out of time leaves its records safe". Both
// are unobservable against a substrate that answers instantly.
//
// Signed requests survive it because the signature covers the Host header the
// client sent, and this proxy forwards that header unchanged; the store never
// has to believe it is being addressed by its own name.
type slowProxy struct {
	// Endpoint is the base URL to configure a writer with.
	Endpoint string

	listener net.Listener
	target   string
	delay    atomic.Int64

	reject atomic.Bool

	wg   sync.WaitGroup
	done chan struct{}
	once sync.Once

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

// startSlowProxy starts a proxy in front of an endpoint and stops it when the
// test ends.
func startSlowProxy(tb testing.TB, endpoint string, delay time.Duration) *slowProxy {
	tb.Helper()

	target := strings.TrimPrefix(endpoint, "http://")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("starting the proxy: %v", err)
	}

	p := &slowProxy{
		Endpoint: "http://" + listener.Addr().String(),
		listener: listener,
		target:   target,
		done:     make(chan struct{}),
		conns:    make(map[net.Conn]struct{}),
	}
	p.delay.Store(int64(delay))

	p.wg.Add(1)
	go p.accept()

	tb.Cleanup(func() {
		p.once.Do(func() { close(p.done) })
		if err := listener.Close(); err != nil {
			tb.Errorf("closing the proxy: %v", err)
		}
		// Every live connection is closed rather than waited on. An HTTP client
		// keeps its connections idle for its own keep-alive window, so a proxy
		// that only stopped accepting would sit here for a minute and a half
		// after a test that was otherwise finished.
		p.mu.Lock()
		for conn := range p.conns {
			_ = conn.Close()
		}
		p.mu.Unlock()
		p.wg.Wait()
	})

	return p
}

// SetReject makes the proxy hang up on every connection instead of forwarding
// it, which is what an endpoint that is down looks like to a client: a fast,
// unambiguous failure rather than a stall. Clearing it lets the same table
// heal, with no restart and no reconfiguration.
//
// Turning it on severs the connections that already exist, for the same reason
// [slowProxy.SetDelay] does: the client under test pools its connections, so a
// gate applied only at accept time would be skipped entirely by a request
// travelling on a connection opened before the gate closed. Without this, "from
// here on nothing reaches the store" would be true only when no warm connection
// happened to be lying around — which is an accident of timing, not a property
// a test can rest on.
func (p *slowProxy) SetReject(reject bool) {
	p.reject.Store(reject)
	if reject {
		p.hangUp()
	}
}

// SetDelay changes the delay applied from now on, and — when it is raising the
// delay — hangs up every connection currently open through the proxy.
//
// Both halves are needed for a test to be able to say "from here on, nothing
// reaches the store". An HTTP client pools its connections, so a delay applied
// only at accept time would be skipped entirely by a request travelling on a
// connection that already exists. But holding back each chunk is not enough on
// its own either: a request whose bytes had already been forwarded when the
// delay landed is past the gate, and its response comes back at full speed.
// Cutting the existing connections closes that window, and the client's own
// re-dial then meets the gate. Lowering the delay leaves connections alone,
// because letting a stalled table heal must not also disturb it.
func (p *slowProxy) SetDelay(d time.Duration) {
	previous := p.delay.Swap(int64(d))
	if d > 0 && d > time.Duration(previous) {
		p.hangUp()
	}
}

// hangUp closes every connection the proxy currently holds, in both directions.
func (p *slowProxy) hangUp() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for conn := range p.conns {
		_ = conn.Close()
	}
}

// accept serves connections until the listener closes.
func (p *slowProxy) accept() {
	defer p.wg.Done()

	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.wg.Add(1)
		go p.forward(conn)
	}
}

// track registers a connection so shutdown can close it, and returns the
// function that unregisters it.
func (p *slowProxy) track(conn net.Conn) func() {
	p.mu.Lock()
	p.conns[conn] = struct{}{}
	p.mu.Unlock()

	return func() {
		p.mu.Lock()
		delete(p.conns, conn)
		p.mu.Unlock()
		_ = conn.Close()
	}
}

// hold waits out the current delay, reporting false if the proxy shut down
// while it waited.
func (p *slowProxy) hold() bool {
	d := time.Duration(p.delay.Load())
	if d <= 0 {
		select {
		case <-p.done:
			return false
		default:
			return true
		}
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-p.done:
		return false
	}
}

// forward pipes one connection to the target, holding back each chunk the
// client sends by the configured delay.
func (p *slowProxy) forward(client net.Conn) {
	defer p.wg.Done()
	defer p.track(client)()

	if p.reject.Load() {
		return
	}
	// Gated before the connection to the store is made at all, so a delay that
	// was already set holds a new connection back rather than letting its first
	// request through and stalling the second.
	if !p.hold() {
		return
	}

	upstream, err := net.Dial("tcp", p.target)
	if err != nil {
		return
	}
	defer p.track(upstream)()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		p.pipe(upstream, client, true)
	}()
	go func() {
		defer wg.Done()
		p.pipe(client, upstream, false)
	}()
	wg.Wait()
}

// pipe copies one direction, pausing before each chunk when asked.
//
// Only the outbound direction is delayed, which is enough: a request that is
// held back has not reached the store, so nothing comes back either. The pause
// is interruptible, so a test that shuts the proxy down does not have to wait
// out a stall it deliberately made long.
func (p *slowProxy) pipe(dst, src net.Conn, delayed bool) {
	buf := make([]byte, 32*1024)
	for {
		if delayed && !p.hold() {
			return
		}

		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				return
			}
			// Half-closing tells the other side the stream ended, which is what
			// keeps a keep-alive connection from hanging until a timeout.
			if c, ok := dst.(*net.TCPConn); ok {
				_ = c.CloseWrite()
			}

			return
		}
	}
}
