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
		Heartbeat: -1,
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
	n, err := Serve(context.Background(), Config{Listen: sock, LocalOnly: true, Heartbeat: -1, Logf: func(string, ...any) {}})
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

func TestLocalHeartbeatKeepsSession(t *testing.T) {
	sock := t.TempDir() + "/hb.sock"
	n, err := Serve(context.Background(), Config{
		Listen:    sock,
		LocalOnly: true,
		Heartbeat: 30 * time.Millisecond,
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
	_ = c.Conn.SetReadDeadline(time.Now().Add(120 * time.Millisecond))
	if _, err := c.Recv(); err == nil {
		t.Fatal("expected deadline while draining server pings")
	}
	_ = c.Conn.SetDeadline(time.Time{})
	if err := c.Put("jobs", "after-hb", nil); err != nil {
		t.Fatal(err)
	}
	f, err := c.Take("jobs")
	if err != nil {
		t.Fatal(err)
	}
	if string(f.Payload()) != "after-hb" {
		t.Fatalf("got %q", f.Payload())
	}
}

func TestLocalRequestRetry(t *testing.T) {
	sock := t.TempDir() + "/req.sock"
	n, err := Serve(context.Background(), Config{
		Listen:    sock,
		LocalOnly: true,
		Heartbeat: -1,
		Logf:      func(string, ...any) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	worker, err := Dial(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	done := make(chan error, 1)
	go func() {
		time.Sleep(80 * time.Millisecond)
		job, err := worker.Ready("echo")
		if err != nil {
			done <- err
			return
		}
		done <- worker.Rep(job.ID, "echo", "pong-"+string(job.Payload()), nil)
	}()

	c, err := Dial(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	f, err := c.Request("echo", "hi", nil, 40*time.Millisecond, 5)
	if err != nil {
		t.Fatal(err)
	}
	if string(f.Payload()) != "pong-hi" {
		t.Fatalf("got %q op=%s", f.Payload(), f.Op)
	}
	if err := <-done; err != nil {
		t.Fatalf("worker: %v", err)
	}
}

// Correlation ids double as mailbox message ids, so two clients must never
// generate the same one: otherwise one client's job is silently deduplicated
// against the other's and the producer is still told "ok".
func TestClientIDUnique(t *testing.T) {
	sock := t.TempDir() + "/bus.sock"
	n, err := Serve(context.Background(), Config{
		Listen: sock, LocalOnly: true, Heartbeat: -1, Logf: func(string, ...any) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	a, err := Dial(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Dial(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if a.nextID() == b.nextID() {
		t.Fatal("two clients generated the same correlation id")
	}

	if err := a.Put("jobs", "from-a", nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Put("jobs", "from-b", nil); err != nil {
		t.Fatal(err)
	}

	r, err := Dial(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		_ = r.Conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		f, err := r.Take("jobs")
		if err != nil {
			t.Fatalf("take %d: %v", i, err)
		}
		seen[string(f.Payload())] = true
	}
	if !seen["from-a"] || !seen["from-b"] {
		t.Fatalf("a job was swallowed: %v", seen)
	}
}
