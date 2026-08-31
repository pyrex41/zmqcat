// Package hub serves the zmqcat wire protocol against a mailbox.Bus.
package hub

import (
	"context"
	"io"
	"log"
	"net"
	"sync"

	"github.com/pyrex41/zmqcat/internal/mailbox"
	"github.com/pyrex41/zmqcat/internal/wire"
)

type Logger func(format string, args ...any)

// Hub owns the bus and sessions.
type Hub struct {
	Bus  *mailbox.Bus
	Logf Logger

	mu       sync.Mutex
	sessions map[*session]struct{}
}

func New(bus *mailbox.Bus) *Hub {
	if bus == nil {
		bus = mailbox.New()
	}
	return &Hub{
		Bus:      bus,
		Logf:     log.Printf,
		sessions: make(map[*session]struct{}),
	}
}

func (h *Hub) Close() {
	h.Bus.Close()
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.sessions {
		s.close()
	}
}

// ServeConn runs one client until the connection ends.
func (h *Hub) ServeConn(ctx context.Context, c net.Conn) {
	s := &session{h: h, c: c, name: c.RemoteAddr().String()}
	h.mu.Lock()
	h.sessions[s] = struct{}{}
	h.mu.Unlock()
	defer func() {
		s.close()
		h.mu.Lock()
		delete(h.sessions, s)
		h.mu.Unlock()
	}()
	s.run(ctx)
}

type session struct {
	h    *Hub
	c    net.Conn
	name string

	mu     sync.Mutex
	closed bool
	unsub  []func()
}

func (s *session) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, u := range s.unsub {
		u()
	}
	s.unsub = nil
	_ = s.c.Close()
}

func (s *session) run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		s.close()
	}()

	for {
		f, err := wire.Read(s.c)
		if err != nil {
			if err != io.EOF && !isClosed(err) {
				s.h.Logf("zmqcat session %s: %v", s.name, err)
			}
			return
		}
		if err := s.handle(ctx, f); err != nil {
			s.reply(wire.Frame{Op: wire.OpErr, ID: f.ID, Error: err.Error()})
		}
	}
}

func (s *session) handle(ctx context.Context, f wire.Frame) error {
	switch f.Op {
	case wire.OpHello:
		if f.From != "" {
			s.name = f.From
		}
		return s.reply(wire.Frame{Op: wire.OpOK, ID: f.ID, From: s.name})
	case wire.OpPing:
		return s.reply(wire.Frame{Op: wire.OpPong, ID: f.ID})
	case wire.OpPut:
		msg := mailbox.Msg{ID: f.ID, From: s.from(f), Body: f.Payload()}
		err := s.h.Bus.Put(f.Name, msg)
		if err != nil && err != mailbox.ErrDropped {
			return err
		}
		out := wire.Frame{Op: wire.OpOK, ID: f.ID, Name: f.Name}
		if err == mailbox.ErrDropped {
			out.Error = err.Error()
		}
		return s.reply(out)
	case wire.OpTake:
		msg, err := s.h.Bus.Take(ctx, f.Name)
		if err != nil {
			return err
		}
		return s.reply(msgFrame(f.ID, msg))
	case wire.OpPub:
		msg := mailbox.Msg{ID: f.ID, From: s.from(f), Body: f.Payload()}
		if err := s.h.Bus.Pub(f.Name, msg); err != nil {
			return err
		}
		return s.reply(wire.Frame{Op: wire.OpOK, ID: f.ID, Name: f.Name})
	case wire.OpSub:
		ch, cancel, err := s.h.Bus.Sub(f.Name)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.unsub = append(s.unsub, cancel)
		s.mu.Unlock()
		if err := s.reply(wire.Frame{Op: wire.OpOK, ID: f.ID, Name: f.Name}); err != nil {
			cancel()
			return err
		}
		go s.pumpSub(ch)
		return nil
	case wire.OpUnsub:
		s.mu.Lock()
		for _, u := range s.unsub {
			u()
		}
		s.unsub = nil
		s.mu.Unlock()
		return s.reply(wire.Frame{Op: wire.OpOK, ID: f.ID})
	default:
		return s.reply(wire.Frame{Op: wire.OpErr, ID: f.ID, Error: "unknown op " + f.Op})
	}
}

func (s *session) pumpSub(ch <-chan mailbox.Msg) {
	for msg := range ch {
		if err := s.reply(msgFrame("", msg)); err != nil {
			return
		}
	}
}

func (s *session) from(f wire.Frame) string {
	if f.From != "" {
		return f.From
	}
	return s.name
}

func (s *session) reply(f wire.Frame) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return mailbox.ErrClosed
	}
	return wire.Write(s.c, f)
}

func msgFrame(id string, msg mailbox.Msg) wire.Frame {
	return wire.Frame{
		Op:   wire.OpMsg,
		ID:   first(id, msg.ID),
		Name: msg.Name,
		From: msg.From,
		Body: msg.Body,
		Text: string(msg.Body),
	}
}

func first(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func isClosed(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return s == "use of closed network connection" ||
		s == "read from closed connection" ||
		s == "write on closed connection"
}
