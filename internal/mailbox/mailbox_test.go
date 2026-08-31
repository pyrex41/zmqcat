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

func TestOverflowDropsOldest(t *testing.T) {
	b := NewWithLimits(2, 1024)
	defer b.Close()
	_ = b.Put("q", Msg{Body: []byte("1")})
	_ = b.Put("q", Msg{Body: []byte("2")})
	err := b.Put("q", Msg{Body: []byte("3")})
	if !errors.Is(err, ErrDropped) {
		t.Fatalf("got %v", err)
	}
	a, _ := b.TryTake("q")
	c, _ := b.TryTake("q")
	if string(a.Body)+string(c.Body) != "23" {
		t.Fatalf("got %s%s", a.Body, c.Body)
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
