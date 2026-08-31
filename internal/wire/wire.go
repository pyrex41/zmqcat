// Package wire is the zmqcat session protocol: magic + length + JSON.
//
// Any language that can write four bytes of length and a JSON object can
// speak it — OpenResty, Python, Go, a one-liner in a harness.
package wire

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	Magic   = "ZMQC"
	Version = 1
	MaxBody = 2 << 20 // 2 MiB envelope cap
)

var (
	ErrMagic   = errors.New("zmqcat: bad magic")
	ErrTooBig  = errors.New("zmqcat: frame too large")
	ErrVersion = errors.New("zmqcat: unsupported version")
)

// Ops.
const (
	OpHello = "hello"
	OpPut   = "put"
	OpTake  = "take"
	OpPub   = "pub"
	OpSub   = "sub"
	OpUnsub = "unsub"
	OpOK    = "ok"
	OpErr   = "err"
	OpMsg   = "msg"
	OpPing  = "ping"
	OpPong  = "pong"
)

// Frame is one request or reply.
type Frame struct {
	V     int    `json:"v"`
	Op    string `json:"op"`
	ID    string `json:"id,omitempty"`
	Name  string `json:"name,omitempty"`  // mailbox or topic
	From  string `json:"from,omitempty"`
	Text  string `json:"text,omitempty"`  // utf-8 convenience
	Body  []byte `json:"body,omitempty"`  // raw bytes (JSON base64)
	Error string `json:"error,omitempty"`
}

func (f Frame) Payload() []byte {
	if len(f.Body) > 0 {
		return f.Body
	}
	if f.Text != "" {
		return []byte(f.Text)
	}
	return nil
}

func Write(w io.Writer, f Frame) error {
	if f.V == 0 {
		f.V = Version
	}
	raw, err := json.Marshal(f)
	if err != nil {
		return err
	}
	if len(raw) > MaxBody {
		return ErrTooBig
	}
	var hdr [8]byte
	copy(hdr[:4], Magic)
	binary.BigEndian.PutUint32(hdr[4:], uint32(len(raw)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(raw)
	return err
}

func Read(r io.Reader) (Frame, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	if string(hdr[:4]) != Magic {
		return Frame{}, fmt.Errorf("%w: %q", ErrMagic, hdr[:4])
	}
	n := binary.BigEndian.Uint32(hdr[4:])
	if n == 0 || n > MaxBody {
		return Frame{}, ErrTooBig
	}
	raw := make([]byte, n)
	if _, err := io.ReadFull(r, raw); err != nil {
		return Frame{}, err
	}
	var f Frame
	if err := json.Unmarshal(raw, &f); err != nil {
		return Frame{}, err
	}
	if f.V != 0 && f.V != Version {
		return Frame{}, fmt.Errorf("%w: %d", ErrVersion, f.V)
	}
	return f, nil
}
