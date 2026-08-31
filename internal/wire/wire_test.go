package wire

import (
	"bytes"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	in := Frame{Op: OpPut, Name: "inbox", From: "bot", Text: "hello", Body: []byte("hello")}
	if err := Write(&buf, in); err != nil {
		t.Fatal(err)
	}
	out, err := Read(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if out.Op != OpPut || out.Name != "inbox" || string(out.Payload()) != "hello" {
		t.Fatalf("got %+v", out)
	}
	if out.V != Version {
		t.Fatalf("version %d", out.V)
	}
}

func TestBadMagic(t *testing.T) {
	_, err := Read(bytes.NewReader([]byte("NOPE\x00\x00\x00\x01x")))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTextFallback(t *testing.T) {
	f := Frame{Text: "abc"}
	if string(f.Payload()) != "abc" {
		t.Fatal(f.Payload())
	}
}
