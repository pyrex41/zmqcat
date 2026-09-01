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
	OpHello   = "hello"
	OpPut     = "put"
	OpTake    = "take"
	OpPub     = "pub"
	OpSub     = "sub"
	OpUnsub   = "unsub"
	OpOK      = "ok"
	OpErr     = "err"
	OpMsg     = "msg"
	OpPing    = "ping"
	OpPong    = "pong"
	OpReserve = "reserve"
	OpAck     = "ack"
	OpNack    = "nack"
	OpReady   = "ready"
	OpReq     = "req"
	OpRep     = "rep"
)

// Frame is one request or reply.
type Frame struct {
	V        int    `json:"v"`
	Op       string `json:"op"`
	ID       string `json:"id,omitempty"`
	Name     string `json:"name,omitempty"` // mailbox or topic
	From     string `json:"from,omitempty"`
	Text     string `json:"text,omitempty"` // utf-8 convenience
	Body     []byte `json:"body,omitempty"` // raw bytes (JSON base64)
	Error    string `json:"error,omitempty"`
	Lease    int64  `json:"lease,omitempty"`
	Delivery string `json:"delivery,omitempty"`
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

// Read decodes one frame.
func Read(r io.Reader) (Frame, error) {
	f, _, err := ReadCounted(r)
	return f, err
}

// ReadCounted decodes one frame and also reports how many bytes it consumed.
// A caller whose read was interrupted (for example by a deadline) uses the
// count to tell a clean frame boundary from a desynchronized stream: zero
// bytes consumed means the next Read can still resynchronize, anything else
// means the remainder of a partial frame is still queued on the wire.
func ReadCounted(r io.Reader) (Frame, int, error) {
	var hdr [8]byte
	read, err := io.ReadFull(r, hdr[:])
	if err != nil {
		return Frame{}, read, err
	}
	if string(hdr[:4]) != Magic {
		return Frame{}, read, fmt.Errorf("%w: %q", ErrMagic, hdr[:4])
	}
	n := binary.BigEndian.Uint32(hdr[4:])
	if n == 0 || n > MaxBody {
		return Frame{}, read, ErrTooBig
	}
	raw := make([]byte, n)
	got, err := io.ReadFull(r, raw)
	read += got
	if err != nil {
		return Frame{}, read, err
	}
	var f Frame
	if err := json.Unmarshal(raw, &f); err != nil {
		return Frame{}, read, err
	}
	if f.V != 0 && f.V != Version {
		return Frame{}, read, fmt.Errorf("%w: %d", ErrVersion, f.V)
	}
	return f, read, nil
}
