package hub

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pyrex41/zmqcat/internal/mailbox"
	"github.com/pyrex41/zmqcat/internal/wire"
)

func TestPutTakeOverPipe(t *testing.T) {
	h := New(mailbox.New())
	defer h.Close()
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
	h := New(mailbox.New())
	defer h.Close()
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

func mustWrite(t *testing.T, w io.Writer, f wire.Frame) {
	t.Helper()
	if err := wire.Write(w, f); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, r io.Reader) wire.Frame {
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
