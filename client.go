package zmqcat

import (
	"fmt"
	"net"

	"github.com/pyrex41/zmqcat/internal/wire"
)

// Client is a single session on a hub (local unix/tcp or a spliced tunnel).
type Client struct {
	Conn net.Conn
	Name string
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
	return wire.Read(c.Conn)
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

func (c *Client) roundTrip(req wire.Frame) (wire.Frame, error) {
	if err := wire.Write(c.Conn, req); err != nil {
		return wire.Frame{}, err
	}
	return wire.Read(c.Conn)
}
