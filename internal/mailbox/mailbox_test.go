package mailbox

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPutTake(t *testing.T) {
	b := New()
	defer b.Close()
	if err := b.Put("inbox", Msg{From: "a", Body: []byte("hi")}); err != nil {
		t.Fatal(err)
	}
	msg, err := b.Take(context.Background(), "inbox")
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Body) != "hi" || msg.From != "a" {
		t.Fatalf("got %+v", msg)
	}
}

func TestPersistentReserveAckRedelivery(t *testing.T) {
	p := t.TempDir() + "/mail.json"
	b, err := OpenPersistent(p, 10, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.Put("in", Msg{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	d, err := b.Reserve(context.Background(), "in", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if d.ID == "" {
		t.Fatal("missing delivery id")
	}
	time.Sleep(3 * time.Millisecond)
	d2, err := b.Reserve(context.Background(), "in", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(d2.Msg.Body) != "x" {
		t.Fatal("not redelivered")
	}
	if err := b.Ack(d2.ID); err != nil {
		t.Fatal(err)
	}
	b2, err := OpenPersistent(p, 10, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if _, err := b2.TryReserve("in", time.Second); !errors.Is(err, ErrEmpty) {
		t.Fatalf("acked message persisted: %v", err)
	}
}

func TestTakeBlocks(t *testing.T) {
	b := New()
	defer b.Close()
	got := make(chan Msg, 1)
	go func() {
		msg, err := b.Take(context.Background(), "inbox")
		if err != nil {
			t.Errorf("take: %v", err)
			return
		}
		got <- msg
	}()
	time.Sleep(20 * time.Millisecond)
	if err := b.Put("inbox", Msg{Body: []byte("later")}); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-got:
		if string(msg.Body) != "later" {
			t.Fatalf("got %q", msg.Body)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked")
	}
}

func TestTakeCancel(t *testing.T) {
	b := New()
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := b.Take(ctx, "inbox")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if err := b.Put("inbox", Msg{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	msg, err := b.TryTake("inbox")
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Body) != "x" {
		t.Fatalf("lost message after cancel: %q", msg.Body)
	}
}

func TestOverflowRejects(t *testing.T) {
	b := NewWithLimits(2, 1024)
	defer b.Close()
	if err := b.Put("q", Msg{Body: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if err := b.Put("q", Msg{Body: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	err := b.Put("q", Msg{Body: []byte("3")})
	if !errors.Is(err, ErrDropped) {
		t.Fatalf("got %v", err)
	}
	a, err := b.TryTake("q")
	if err != nil {
		t.Fatal(err)
	}
	c, err := b.TryTake("q")
	if err != nil {
		t.Fatal(err)
	}
	if string(a.Body)+string(c.Body) != "12" {
		t.Fatalf("dropped oldest: got %s%s", a.Body, c.Body)
	}
	if _, err := b.TryTake("q"); !errors.Is(err, ErrEmpty) {
		t.Fatalf("third message was queued: %v", err)
	}
}

func TestLeaseExpireRequeue(t *testing.T) {
	b := New()
	defer b.Close()
	if err := b.Put("q", Msg{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Reserve(context.Background(), "q", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := b.TryTake("q"); !errors.Is(err, ErrEmpty) {
		t.Fatal("reserved message still queued")
	}
	time.Sleep(3 * time.Millisecond)
	b.ExpireLeases()
	msg, err := b.TryTake("q")
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Body) != "x" {
		t.Fatalf("got %q", msg.Body)
	}
}

func TestNackRequeue(t *testing.T) {
	b := New()
	defer b.Close()
	if err := b.Put("q", Msg{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	d, err := b.Reserve(context.Background(), "q", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Nack(d.ID); err != nil {
		t.Fatal(err)
	}
	msg, err := b.TryTake("q")
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Body) != "x" {
		t.Fatalf("got %q", msg.Body)
	}
}

func TestLastValueCacheReplay(t *testing.T) {
	b := New()
	defer b.Close()
	if err := b.Pub("events.foo", Msg{Body: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := b.Pub("events.bar", Msg{Body: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if err := b.Pub("other", Msg{Body: []byte("no")}); err != nil {
		t.Fatal(err)
	}
	ch, cancel, err := b.Sub("events.")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	got := map[string]string{}
	deadline := time.After(time.Second)
	for len(got) < 2 {
		select {
		case msg := <-ch:
			got[msg.Name] = string(msg.Body)
		case <-deadline:
			t.Fatalf("lvc replay missing: %v", got)
		}
	}
	if got["events.foo"] != "a" || got["events.bar"] != "b" {
		t.Fatalf("got %v", got)
	}
	select {
	case msg := <-ch:
		t.Fatalf("unexpected %q %q", msg.Name, msg.Body)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestPubSubPrefix(t *testing.T) {
	b := New()
	defer b.Close()
	ch, cancel, err := b.Sub("events.")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if err := b.Pub("events.foo", Msg{Body: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := b.Pub("other", Msg{Body: []byte("no")}); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-ch:
		if string(msg.Body) != "a" || msg.Name != "events.foo" {
			t.Fatalf("got %+v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("no pub")
	}
	select {
	case msg := <-ch:
		t.Fatalf("unexpected %q", msg.Body)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestCloseUnblocks(t *testing.T) {
	b := New()
	done := make(chan error, 1)
	go func() {
		_, err := b.Take(context.Background(), "x")
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	b.Close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("not unblocked")
	}
}

func TestPutDuplicateID(t *testing.T) {
	b := New()
	defer b.Close()
	if err := b.Put("q", Msg{ID: "same", Body: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if err := b.Put("q", Msg{ID: "same", Body: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	if b.Depth("q") != 1 {
		t.Fatalf("depth %d", b.Depth("q"))
	}
}

// Take and re-Put of the same id must work: the dedup index has to release
// the id once the message leaves the queue.
func TestDedupIndexReleasedAfterTake(t *testing.T) {
	b := New()
	defer b.Close()
	if err := b.Put("q", Msg{ID: "same", Body: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.TryTake("q"); err != nil {
		t.Fatal(err)
	}
	if err := b.Put("q", Msg{ID: "same", Body: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	msg, err := b.TryTake("q")
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Body) != "2" {
		t.Fatalf("got %q", msg.Body)
	}
}

// While a message is leased, a duplicate Put is the same message. Once it is
// acked the id is free again.
func TestDedupWhileLeased(t *testing.T) {
	b := New()
	defer b.Close()
	if err := b.Put("q", Msg{ID: "same", Body: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	d, err := b.TryReserve("q", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put("q", Msg{ID: "same", Body: []byte("dup")}); err != nil {
		t.Fatal(err)
	}
	if b.Depth("q") != 0 {
		t.Fatalf("duplicate queued while leased: depth %d", b.Depth("q"))
	}
	if err := b.Ack(d.ID); err != nil {
		t.Fatal(err)
	}
	if err := b.Put("q", Msg{ID: "same", Body: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	if b.Depth("q") != 1 {
		t.Fatalf("id not released after ack: depth %d", b.Depth("q"))
	}
}

// Reserve blocks until a message arrives instead of failing on an empty box.
func TestReserveWaits(t *testing.T) {
	b := New()
	defer b.Close()
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = b.Put("q", Msg{Body: []byte("late")})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	d, err := b.Reserve(ctx, "q", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if string(d.Msg.Body) != "late" {
		t.Fatalf("got %q", d.Msg.Body)
	}
}
