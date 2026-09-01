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
