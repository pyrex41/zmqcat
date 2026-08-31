package zmqcat

import (
	"context"
	"testing"
	"time"
)

func TestLocalServePutTake(t *testing.T) {
	dir := t.TempDir()
	sock := dir + "/zmqcat.sock"
	n, err := Serve(context.Background(), Config{
		Listen:    sock,
		LocalOnly: true,
		Logf:      func(string, ...any) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	c, err := Dial(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Hello("tester"); err != nil {
		t.Fatal(err)
	}
	if err := c.Ping(); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("jobs", "do-it", nil); err != nil {
		t.Fatal(err)
	}
	f, err := c.Take("jobs")
	if err != nil {
		t.Fatal(err)
	}
	if string(f.Payload()) != "do-it" {
		t.Fatalf("got %q", f.Payload())
	}
}

func TestLocalPubSub(t *testing.T) {
	sock := t.TempDir() + "/bus.sock"
	n, err := Serve(context.Background(), Config{Listen: sock, LocalOnly: true, Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	sub, err := Dial(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.Sub("harness."); err != nil {
		t.Fatal(err)
	}

	pub, err := Dial(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := pub.Pub("harness.done", "ok", nil); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout")
		default:
		}
		f, err := sub.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if f.Op == "msg" && string(f.Payload()) == "ok" {
			return
		}
	}
}
