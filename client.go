package zmqcat

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/pyrex41/zmqcat/internal/wire"
)

// ErrAbandoned is returned by Request after retries are exhausted.
var ErrAbandoned = errors.New("zmqcat: request abandoned")

// Client is a single session on a hub (local unix/tcp or a spliced tunnel).
type Client struct {
	Conn net.Conn
	Name string
	seq  uint64
}

// Dial opens a session to a local sidecar listen address.
func Dial(listen string) (*Client, error) {
	c, err := listenDial(listen)
	if err != nil {
		return nil, err
	}
	cl := &Client{Conn: c}
	return cl, cl.Hello("")
}

func (c *Client) Close() error {
	if c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}

func (c *Client) Hello(name string) error {
	if name != "" {
		c.Name = name
	}
	f, err := c.roundTrip(wire.Frame{Op: wire.OpHello, From: c.Name})
	if err != nil {
		return err
	}
	if f.Op == wire.OpErr {
		return fmt.Errorf("hello: %s", f.Error)
	}
	return nil
}

func (c *Client) Put(name, text string, body []byte) error {
	f, err := c.roundTrip(wire.Frame{Op: wire.OpPut, Name: name, From: c.Name, Text: text, Body: body})
	if err != nil {
		return err
	}
	if f.Op == wire.OpErr {
		return fmt.Errorf("put %s: %s", name, f.Error)
	}
	return nil
}

func (c *Client) Take(name string) (wire.Frame, error) {
	f, err := c.roundTrip(wire.Frame{Op: wire.OpTake, Name: name, From: c.Name})
	if err != nil {
		return f, err
	}
	if f.Op == wire.OpErr {
		return f, fmt.Errorf("take %s: %s", name, f.Error)
	}
	return f, nil
}

func (c *Client) Reserve(name string, lease time.Duration) (wire.Frame, error) {
	f, e := c.roundTrip(wire.Frame{Op: wire.OpReserve, Name: name, From: c.Name, Lease: int64(lease / time.Second)})
	if e != nil {
		return f, e
	}
	if f.Op == wire.OpErr {
		return f, fmt.Errorf("reserve %s: %s", name, f.Error)
	}
	return f, nil
}

func (c *Client) Ack(delivery string) error {
	f, e := c.roundTrip(wire.Frame{Op: wire.OpAck, Delivery: delivery})
	if e != nil {
		return e
	}
	if f.Op == wire.OpErr {
		return fmt.Errorf("ack: %s", f.Error)
	}
	return nil
}

func (c *Client) Nack(delivery string) error {
	f, e := c.roundTrip(wire.Frame{Op: wire.OpNack, Delivery: delivery})
	if e != nil {
		return e
	}
	if f.Op == wire.OpErr {
		return fmt.Errorf("nack: %s", f.Error)
	}
	return nil
}

// Ready registers as a competing consumer for service and waits for one job.
func (c *Client) Ready(service string) (wire.Frame, error) {
	f, err := c.roundTrip(wire.Frame{Op: wire.OpReady, Name: service, From: c.Name})
	if err != nil {
		return f, err
	}
	if f.Op == wire.OpErr {
		return f, fmt.Errorf("ready %s: %s", service, f.Error)
	}
	return f, nil
}

func (c *Client) Rep(id, name, text string, body []byte) error {
	f, err := c.roundTrip(wire.Frame{Op: wire.OpRep, ID: id, Name: name, From: c.Name, Text: text, Body: body})
	if err != nil {
		return err
	}
	if f.Op == wire.OpErr {
		return fmt.Errorf("rep: %s", f.Error)
	}
	return nil
}

// Request is Lazy Pirate: req/rep with a correlation id, timeout, and retries.
// Duplicate delivery is possible; the same id is reused across attempts.
func (c *Client) Request(name, text string, body []byte, timeout time.Duration, attempts int) (wire.Frame, error) {
	if attempts <= 0 {
		attempts = 3
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	id := c.nextID()
	var last error
	for i := 0; i < attempts; i++ {
		f, err := c.roundTripTimeout(wire.Frame{
			Op:   wire.OpReq,
			ID:   id,
			Name: name,
			From: c.Name,
			Text: text,
			Body: body,
		}, timeout)
		if err == nil && f.Op != wire.OpErr {
			return f, nil
		}
		if err != nil {
			last = err
		} else {
			last = fmt.Errorf("req %s: %s", name, f.Error)
		}
	}
	return wire.Frame{}, fmt.Errorf("%w after %d attempts: %v", ErrAbandoned, attempts, last)
}

func (c *Client) Pub(topic, text string, body []byte) error {
	f, err := c.roundTrip(wire.Frame{Op: wire.OpPub, Name: topic, From: c.Name, Text: text, Body: body})
	if err != nil {
		return err
	}
	if f.Op == wire.OpErr {
		return fmt.Errorf("pub %s: %s", topic, f.Error)
	}
	return nil
}

func (c *Client) Sub(prefix string) error {
	f, err := c.roundTrip(wire.Frame{Op: wire.OpSub, Name: prefix, From: c.Name})
	if err != nil {
		return err
	}
	if f.Op == wire.OpErr {
		return fmt.Errorf("sub %s: %s", prefix, f.Error)
	}
	return nil
}

func (c *Client) Recv() (wire.Frame, error) {
	return c.readSkipPing()
}

func (c *Client) Ping() error {
	f, err := c.roundTrip(wire.Frame{Op: wire.OpPing})
	if err != nil {
		return err
	}
	if f.Op != wire.OpPong {
		return fmt.Errorf("ping: got %s %s", f.Op, f.Error)
	}
	return nil
}

func (c *Client) nextID() string {
	return fmt.Sprintf("r-%d", atomic.AddUint64(&c.seq, 1))
}

func (c *Client) roundTrip(req wire.Frame) (wire.Frame, error) {
	return c.roundTripTimeout(req, 0)
}

func (c *Client) roundTripTimeout(req wire.Frame, timeout time.Duration) (wire.Frame, error) {
	if req.ID == "" {
		req.ID = c.nextID()
	}
	if timeout > 0 {
		_ = c.Conn.SetDeadline(time.Now().Add(timeout))
		defer c.Conn.SetDeadline(time.Time{})
	}
	if err := wire.Write(c.Conn, req); err != nil {
		return wire.Frame{}, err
	}
	return c.readSkipPing()
}

func (c *Client) readSkipPing() (wire.Frame, error) {
	for {
		f, err := wire.Read(c.Conn)
		if err != nil {
			return f, err
		}
		if f.Op == wire.OpPing {
			_ = wire.Write(c.Conn, wire.Frame{Op: wire.OpPong, ID: f.ID})
			continue
		}
		return f, nil
	}
}
