package hub

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/pyrex41/zmqcat/internal/mailbox"
	"github.com/pyrex41/zmqcat/internal/wire"
)

func testHub(t *testing.T) *Hub {
	t.Helper()
	h := New(mailbox.New())
	h.Heartbeat = 0
	h.Logf = func(string, ...any) {}
	t.Cleanup(h.Close)
	return h
}

func TestPutTakeOverPipe(t *testing.T) {
	h := testHub(t)
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go h.ServeConn(context.Background(), b)

	mustWrite(t, a, wire.Frame{Op: wire.OpHello, From: "t"})
	hello := mustRead(t, a)
	if hello.Op != wire.OpOK {
		t.Fatalf("hello %+v", hello)
	}

	mustWrite(t, a, wire.Frame{Op: wire.OpPut, Name: "inbox", Text: "ping"})
	ok := mustRead(t, a)
	if ok.Op != wire.OpOK {
		t.Fatalf("put %+v", ok)
	}

	mustWrite(t, a, wire.Frame{Op: wire.OpTake, Name: "inbox"})
	msg := mustRead(t, a)
	if msg.Op != wire.OpMsg || string(msg.Payload()) != "ping" {
		t.Fatalf("take %+v", msg)
	}
}

func TestPubSubOverPipe(t *testing.T) {
	h := testHub(t)
	pub, pubS := net.Pipe()
	sub, subS := net.Pipe()
	defer pub.Close()
	defer sub.Close()
	go h.ServeConn(context.Background(), pubS)
	go h.ServeConn(context.Background(), subS)

	mustWrite(t, sub, wire.Frame{Op: wire.OpSub, Name: "ev."})
	if mustRead(t, sub).Op != wire.OpOK {
		t.Fatal("sub")
	}
	mustWrite(t, pub, wire.Frame{Op: wire.OpPub, Name: "ev.1", Text: "x"})
	if mustRead(t, pub).Op != wire.OpOK {
		t.Fatal("pub")
	}
	msg := mustRead(t, sub)
	if msg.Op != wire.OpMsg || string(msg.Payload()) != "x" {
		t.Fatalf("got %+v", msg)
	}
}

func TestPutRejectsWhenFull(t *testing.T) {
	bus := mailbox.NewWithLimits(1, 1024)
	h := New(bus)
	h.Heartbeat = 0
	h.Logf = func(string, ...any) {}
	defer h.Close()
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go h.ServeConn(context.Background(), b)

	mustWrite(t, a, wire.Frame{Op: wire.OpPut, Name: "q", Text: "1"})
	if mustRead(t, a).Op != wire.OpOK {
		t.Fatal("first put")
	}
	mustWrite(t, a, wire.Frame{Op: wire.OpPut, Name: "q", Text: "2"})
	errf := mustRead(t, a)
	if errf.Op != wire.OpErr || !strings.Contains(errf.Error, "queue full") {
		t.Fatalf("want reject, got %+v", errf)
	}
	if bus.Depth("q") != 1 {
		t.Fatalf("depth %d", bus.Depth("q"))
	}
}

func TestLastValueCacheOverPipe(t *testing.T) {
	h := testHub(t)
	pub, pubS := net.Pipe()
	sub, subS := net.Pipe()
	defer pub.Close()
	defer sub.Close()
	go h.ServeConn(context.Background(), pubS)
	go h.ServeConn(context.Background(), subS)

	mustWrite(t, pub, wire.Frame{Op: wire.OpPub, Name: "ev.1", Text: "cached"})
	if mustRead(t, pub).Op != wire.OpOK {
		t.Fatal("pub")
	}
	mustWrite(t, sub, wire.Frame{Op: wire.OpSub, Name: "ev."})
	if mustRead(t, sub).Op != wire.OpOK {
		t.Fatal("sub")
	}
	msg := mustRead(t, sub)
	if msg.Op != wire.OpMsg || string(msg.Payload()) != "cached" {
		t.Fatalf("lvc %+v", msg)
	}
}

func TestReserveNackOnDisconnect(t *testing.T) {
	h := testHub(t)
	w, wS := net.Pipe()
	go h.ServeConn(context.Background(), wS)
	mustWrite(t, w, wire.Frame{Op: wire.OpPut, Name: "jobs", Text: "work"})
	if mustRead(t, w).Op != wire.OpOK {
		t.Fatal("put")
	}
	mustWrite(t, w, wire.Frame{Op: wire.OpReserve, Name: "jobs", Lease: 60})
	got := mustRead(t, w)
	if got.Op != wire.OpMsg || got.Delivery == "" {
		t.Fatalf("reserve %+v", got)
	}
	w.Close()

	r, rS := net.Pipe()
	defer r.Close()
	go h.ServeConn(context.Background(), rS)
	deadline := time.Now().Add(2 * time.Second)
	for {
		mustWrite(t, r, wire.Frame{Op: wire.OpTake, Name: "jobs"})
		msg := mustRead(t, r)
		if msg.Op == wire.OpMsg && string(msg.Payload()) == "work" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("not requeued after disconnect: %+v", msg)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestReadyCompetingConsumer(t *testing.T) {
	h := testHub(t)
	w1, s1 := net.Pipe()
	w2, s2 := net.Pipe()
	c, cs := net.Pipe()
	defer w1.Close()
	defer w2.Close()
	defer c.Close()
	go h.ServeConn(context.Background(), s1)
	go h.ServeConn(context.Background(), s2)
	go h.ServeConn(context.Background(), cs)

	mustWrite(t, w1, wire.Frame{Op: wire.OpReady, Name: "svc"})
	mustWrite(t, w2, wire.Frame{Op: wire.OpReady, Name: "svc"})
	time.Sleep(20 * time.Millisecond)
	mustWrite(t, c, wire.Frame{Op: wire.OpPut, Name: "svc", Text: "one"})
	if mustRead(t, c).Op != wire.OpOK {
		t.Fatal("put")
	}

	got := 0
	deadline := time.After(2 * time.Second)
	for got < 1 {
		select {
		case <-deadline:
			t.Fatal("no worker received job")
		default:
		}
		_ = w1.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		if f, err := wire.Read(w1); err == nil && f.Op == wire.OpMsg {
			got++
			break
		}
		_ = w2.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		if f, err := wire.Read(w2); err == nil && f.Op == wire.OpMsg {
			got++
			break
		}
	}
}

func TestReqRep(t *testing.T) {
	h := testHub(t)
	w, wS := net.Pipe()
	c, cS := net.Pipe()
	defer w.Close()
	defer c.Close()
	go h.ServeConn(context.Background(), wS)
	go h.ServeConn(context.Background(), cS)

	mustWrite(t, w, wire.Frame{Op: wire.OpReady, Name: "echo"})
	mustWrite(t, c, wire.Frame{Op: wire.OpReq, ID: "c1", Name: "echo", Text: "hi"})
	job := mustRead(t, w)
	if job.Op != wire.OpMsg || string(job.Payload()) != "hi" {
		t.Fatalf("job %+v", job)
	}
	mustWrite(t, w, wire.Frame{Op: wire.OpRep, ID: job.ID, Name: "echo", Text: "there"})
	if mustRead(t, w).Op != wire.OpOK {
		t.Fatal("rep ack")
	}
	rep := mustRead(t, c)
	if rep.Op != wire.OpRep || string(rep.Payload()) != "there" || rep.ID != "c1" {
		t.Fatalf("rep %+v", rep)
	}
}

func TestServerHeartbeatPing(t *testing.T) {
	h := New(mailbox.New())
	h.Heartbeat = 25 * time.Millisecond
	h.Liveness = 3
	h.Logf = func(string, ...any) {}
	t.Cleanup(h.Close)
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go h.ServeConn(context.Background(), b)

	mustWrite(t, a, wire.Frame{Op: wire.OpHello, From: "hb"})
	if mustRead(t, a).Op != wire.OpOK {
		t.Fatal("hello")
	}
	_ = a.SetReadDeadline(time.Now().Add(time.Second))
	ping := mustReadRaw(t, a)
	if ping.Op != wire.OpPing {
		t.Fatalf("want server ping, got %+v", ping)
	}
	mustWrite(t, a, wire.Frame{Op: wire.OpPong, ID: ping.ID})
	mustWrite(t, a, wire.Frame{Op: wire.OpPing})
	if mustRead(t, a).Op != wire.OpPong {
		t.Fatal("session died after pong")
	}
}

func TestHeartbeatTimeoutNacksReserve(t *testing.T) {
	h := New(mailbox.New())
	h.Heartbeat = 20 * time.Millisecond
	h.Liveness = 3
	h.Logf = func(string, ...any) {}
	t.Cleanup(h.Close)
	a, b := net.Pipe()
	defer a.Close()
	go h.ServeConn(context.Background(), b)

	mustWrite(t, a, wire.Frame{Op: wire.OpPut, Name: "jobs", Text: "leased"})
	if mustRead(t, a).Op != wire.OpOK {
		t.Fatal("put")
	}
	mustWrite(t, a, wire.Frame{Op: wire.OpReserve, Name: "jobs", Lease: 60})
	got := mustRead(t, a)
	if got.Op != wire.OpMsg || got.Delivery == "" {
		t.Fatalf("reserve %+v", got)
	}

	// Drain server pings without ponging so lastSeen stays stale.
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		_ = a.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
		f, err := wire.Read(a)
		if err != nil {
			break
		}
		if f.Op != wire.OpPing {
			t.Fatalf("unexpected %+v", f)
		}
	}

	r, rS := net.Pipe()
	defer r.Close()
	go h.ServeConn(context.Background(), rS)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mustWrite(t, r, wire.Frame{Op: wire.OpTake, Name: "jobs"})
		msg := mustRead(t, r)
		if msg.Op == wire.OpMsg && string(msg.Payload()) == "leased" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("reserved job not nacked after missed heartbeats")
}

func TestHeartbeatBlockedWriteNacksTake(t *testing.T) {
	h := New(mailbox.New())
	h.Heartbeat = 25 * time.Millisecond
	h.Liveness = 3
	h.Logf = func(string, ...any) {}
	t.Cleanup(h.Close)
	a, b := net.Pipe()
	defer a.Close()
	go h.ServeConn(context.Background(), b)

	mustWrite(t, a, wire.Frame{Op: wire.OpPut, Name: "jobs", Text: "taken"})
	if mustRead(t, a).Op != wire.OpOK {
		t.Fatal("put")
	}
	mustWrite(t, a, wire.Frame{Op: wire.OpTake, Name: "jobs"})
	// Do not read the Take reply or heartbeats: write deadline must unblock
	// close/Nack instead of stalling on s.mu.
	time.Sleep(150 * time.Millisecond)

	r, rS := net.Pipe()
	defer r.Close()
	go h.ServeConn(context.Background(), rS)
	mustWrite(t, r, wire.Frame{Op: wire.OpTake, Name: "jobs"})
	msg := mustRead(t, r)
	if msg.Op != wire.OpMsg || string(msg.Payload()) != "taken" {
		t.Fatalf("take not requeued after blocked write: %+v", msg)
	}
}

func TestTraceHook(t *testing.T) {
	h := testHub(t)
	var n int
	h.Trace = func(session, dir string, f wire.Frame) { n++ }
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go h.ServeConn(context.Background(), b)
	mustWrite(t, a, wire.Frame{Op: wire.OpPing})
	if mustRead(t, a).Op != wire.OpPong {
		t.Fatal("pong")
	}
	if n < 2 {
		t.Fatalf("trace calls %d", n)
	}
}

func mustWrite(t *testing.T, w io.Writer, f wire.Frame) {
	t.Helper()
	if err := wire.Write(w, f); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, r io.Reader) wire.Frame {
	t.Helper()
	for {
		f := mustReadRaw(t, r)
		if f.Op == wire.OpPing {
			if ww, ok := r.(io.Writer); ok {
				_ = wire.Write(ww, wire.Frame{Op: wire.OpPong, ID: f.ID})
			}
			continue
		}
		return f
	}
}

func mustReadRaw(t *testing.T, r io.Reader) wire.Frame {
	t.Helper()
	deadline, ok := r.(interface{ SetReadDeadline(time.Time) error })
	if ok {
		_ = deadline.SetReadDeadline(time.Now().Add(2 * time.Second))
	}
	f, err := wire.Read(r)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// Two clients that independently pick the same correlation id must not
// deduplicate each other's jobs.
func TestPutIDsAreScopedPerSession(t *testing.T) {
	h := testHub(t)
	a, aS := net.Pipe()
	b, bS := net.Pipe()
	defer a.Close()
	defer b.Close()
	go h.ServeConn(context.Background(), aS)
	go h.ServeConn(context.Background(), bS)

	mustWrite(t, a, wire.Frame{Op: wire.OpPut, ID: "r-2", Name: "jobs", Text: "from-a"})
	if got := mustRead(t, a); got.Op != wire.OpOK {
		t.Fatalf("put a: %+v", got)
	}
	mustWrite(t, b, wire.Frame{Op: wire.OpPut, ID: "r-2", Name: "jobs", Text: "from-b"})
	if got := mustRead(t, b); got.Op != wire.OpOK {
		t.Fatalf("put b: %+v", got)
	}

	r, rS := net.Pipe()
	defer r.Close()
	go h.ServeConn(context.Background(), rS)
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		mustWrite(t, r, wire.Frame{Op: wire.OpTake, Name: "jobs"})
		msg := mustRead(t, r)
		if msg.Op != wire.OpMsg {
			t.Fatalf("take %d: %+v", i, msg)
		}
		seen[string(msg.Payload())] = true
	}
	if !seen["from-a"] || !seen["from-b"] {
		t.Fatalf("a job was swallowed as a duplicate: %v", seen)
	}
}

// A retried req must re-enqueue when the worker holding it died, otherwise
// the Lazy Pirate loop can never make progress.
func TestReqRetryRecoversFromDeadWorker(t *testing.T) {
	h := testHub(t)
	c, cS := net.Pipe()
	defer c.Close()
	go h.ServeConn(context.Background(), cS)

	w1, w1S := net.Pipe()
	go h.ServeConn(context.Background(), w1S)
	mustWrite(t, w1, wire.Frame{Op: wire.OpReady, Name: "echo"})
	mustWrite(t, c, wire.Frame{Op: wire.OpReq, ID: "c1", Name: "echo", Text: "hi"})
	if job := mustRead(t, w1); job.Op != wire.OpMsg {
		t.Fatalf("worker1 job: %+v", job)
	}
	w1.Close() // worker dies holding the request, never replies

	// The client retries with the same correlation id.
	mustWrite(t, c, wire.Frame{Op: wire.OpReq, ID: "c1", Name: "echo", Text: "hi"})

	w2, w2S := net.Pipe()
	defer w2.Close()
	go h.ServeConn(context.Background(), w2S)
	mustWrite(t, w2, wire.Frame{Op: wire.OpReady, Name: "echo"})
	job := mustRead(t, w2)
	if job.Op != wire.OpMsg || string(job.Payload()) != "hi" {
		t.Fatalf("retry did not re-enqueue: %+v", job)
	}
	mustWrite(t, w2, wire.Frame{Op: wire.OpRep, ID: job.ID, Name: "echo", Text: "there"})
	if got := mustRead(t, w2); got.Op != wire.OpOK {
		t.Fatalf("rep ack: %+v", got)
	}
	rep := mustRead(t, c)
	if rep.Op != wire.OpRep || string(rep.Payload()) != "there" || rep.ID != "c1" {
		t.Fatalf("rep %+v", rep)
	}
}

// A retry while the worker is merely slow must not duplicate the job.
func TestReqRetryDoesNotDuplicate(t *testing.T) {
	h := testHub(t)
	c, cS := net.Pipe()
	defer c.Close()
	go h.ServeConn(context.Background(), cS)

	mustWrite(t, c, wire.Frame{Op: wire.OpReq, ID: "c1", Name: "echo", Text: "hi"})
	mustWrite(t, c, wire.Frame{Op: wire.OpReq, ID: "c1", Name: "echo", Text: "hi"})
	time.Sleep(50 * time.Millisecond)
	if got := h.Bus.Depth("echo"); got != 1 {
		t.Fatalf("retry duplicated the job: depth %d", got)
	}
}

func TestPendingExpires(t *testing.T) {
	h := testHub(t)
	h.PendingTTL = 10 * time.Millisecond
	c, cS := net.Pipe()
	defer c.Close()
	go h.ServeConn(context.Background(), cS)

	mustWrite(t, c, wire.Frame{Op: wire.OpReq, ID: "old", Name: "svc", Text: "x"})
	time.Sleep(30 * time.Millisecond)
	mustWrite(t, c, wire.Frame{Op: wire.OpReq, ID: "new", Name: "svc", Text: "y"})
	time.Sleep(30 * time.Millisecond)
	h.mu.Lock()
	n := len(h.pending)
	h.mu.Unlock()
	if n != 1 {
		t.Fatalf("pending entries %d, want the expired one dropped", n)
	}
}

// A competing consumer that dies without replying must give the job back.
func TestReadyWorkerDeathRequeues(t *testing.T) {
	h := testHub(t)
	c, cS := net.Pipe()
	defer c.Close()
	go h.ServeConn(context.Background(), cS)
	mustWrite(t, c, wire.Frame{Op: wire.OpPut, Name: "svc", Text: "work"})
	if got := mustRead(t, c); got.Op != wire.OpOK {
		t.Fatalf("put: %+v", got)
	}

	w1, w1S := net.Pipe()
	go h.ServeConn(context.Background(), w1S)
	mustWrite(t, w1, wire.Frame{Op: wire.OpReady, Name: "svc"})
	job := mustRead(t, w1)
	if job.Op != wire.OpMsg || job.Delivery == "" {
		t.Fatalf("ready delivery must be leased: %+v", job)
	}
	w1.Close()

	w2, w2S := net.Pipe()
	defer w2.Close()
	go h.ServeConn(context.Background(), w2S)
	mustWrite(t, w2, wire.Frame{Op: wire.OpReady, Name: "svc"})
	again := mustRead(t, w2)
	if again.Op != wire.OpMsg || string(again.Payload()) != "work" {
		t.Fatalf("job not requeued after worker death: %+v", again)
	}
}

// rep acknowledges the delivery, so the job is not redelivered afterwards.
func TestRepAcknowledgesDelivery(t *testing.T) {
	h := testHub(t)
	c, cS := net.Pipe()
	defer c.Close()
	go h.ServeConn(context.Background(), cS)
	w, wS := net.Pipe()
	defer w.Close()
	go h.ServeConn(context.Background(), wS)

	mustWrite(t, w, wire.Frame{Op: wire.OpReady, Name: "echo"})
	mustWrite(t, c, wire.Frame{Op: wire.OpReq, ID: "c1", Name: "echo", Text: "hi"})
	job := mustRead(t, w)
	mustWrite(t, w, wire.Frame{Op: wire.OpRep, ID: job.ID, Name: "echo", Text: "there"})
	if got := mustRead(t, w); got.Op != wire.OpOK {
		t.Fatalf("rep ack: %+v", got)
	}
	if rep := mustRead(t, c); rep.ID != "c1" {
		t.Fatalf("rep %+v", rep)
	}
	if n := h.Bus.Inflight(); n != 0 {
		t.Fatalf("delivery still leased after rep: %d", n)
	}
}
