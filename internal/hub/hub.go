// Package hub serves the zmqcat wire protocol against a mailbox.Bus.
package hub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/pyrex41/zmqcat/internal/mailbox"
	"github.com/pyrex41/zmqcat/internal/wire"
)

type Logger func(format string, args ...any)

// TraceFunc is a quiet per-frame hook (session, direction, frame).
type TraceFunc func(session, dir string, f wire.Frame)

const (
	DefaultHeartbeat = 5 * time.Second
	DefaultLiveness  = 3
	// DefaultPendingTTL bounds an unanswered req's slot in the pending table
	// so a long-lived client cannot accumulate them without limit.
	DefaultPendingTTL = 10 * time.Minute
)

// Hub owns the bus and sessions.
type Hub struct {
	Bus       *mailbox.Bus
	Logf      Logger
	Trace     TraceFunc
	Heartbeat time.Duration
	Liveness  int

	// PendingTTL bounds how long an unanswered req occupies the pending
	// table. Zero uses DefaultPendingTTL.
	PendingTTL time.Duration

	mu       sync.Mutex
	sids     uint64
	sessions map[*session]struct{}
	pending  map[string]pendingReq // scoped req key -> waiting client
}

// pendingReq is one in-flight req awaiting a worker rep.
type pendingReq struct {
	s        *session
	clientID string // the id the requester used, echoed back on the rep
	expires  time.Time
}

func New(bus *mailbox.Bus) *Hub {
	if bus == nil {
		bus = mailbox.New()
	}
	return &Hub{
		Bus:       bus,
		Logf:      log.Printf,
		Heartbeat: DefaultHeartbeat,
		Liveness:  DefaultLiveness,
		sessions:  make(map[*session]struct{}),
		pending:   make(map[string]pendingReq),
	}
}

func (h *Hub) Close() {
	h.Bus.Close()
	h.mu.Lock()
	sessions := make([]*session, 0, len(h.sessions))
	for s := range h.sessions {
		sessions = append(sessions, s)
	}
	h.sessions = make(map[*session]struct{})
	h.mu.Unlock()
	for _, s := range sessions {
		s.close()
	}
}

func (h *Hub) trace(session, dir string, f wire.Frame) {
	if h.Trace != nil {
		h.Trace(session, dir, f)
	}
}

func (h *Hub) clearPending(s *session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, wait := range h.pending {
		if wait.s == s {
			delete(h.pending, id)
		}
	}
}

// ServeConn runs one client until the connection ends.
func (h *Hub) ServeConn(ctx context.Context, c net.Conn) {
	s := &session{h: h, c: c, name: c.RemoteAddr().String(), lastSeen: time.Now()}
	h.mu.Lock()
	h.sids++
	s.sid = h.sids
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
	sid  uint64

	mu       sync.Mutex
	closed   bool
	unsub    []func()
	inflight []string
	repAck   map[string]string // delivered message id -> delivery id
	lastSeen time.Time
}

func (s *session) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	unsub := s.unsub
	s.unsub = nil
	ids := append([]string(nil), s.inflight...)
	s.inflight = nil
	_ = s.c.Close()
	s.mu.Unlock()
	for _, u := range unsub {
		u()
	}
	for _, id := range ids {
		_ = s.h.Bus.Nack(id)
	}
	s.h.clearPending(s)
}

func (s *session) touch() {
	s.mu.Lock()
	s.lastSeen = time.Now()
	s.mu.Unlock()
}

func (s *session) idleLocked(now time.Time) time.Duration {
	if s.lastSeen.IsZero() {
		return 0
	}
	return now.Sub(s.lastSeen)
}

func (s *session) writeTimeout() time.Duration {
	if s.h.Heartbeat > 0 {
		return s.h.Heartbeat
	}
	return DefaultHeartbeat
}

// claimInflight removes id if the session is still open. False means close()
// already copied inflight and will Nack.
func (s *session) claimInflight(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	out := s.inflight[:0]
	for _, x := range s.inflight {
		if x != id {
			out = append(out, x)
		}
	}
	s.inflight = out
	return true
}

// trackRepAck remembers which delivery a rep for msgID should acknowledge.
func (s *session) trackRepAck(msgID, delivery string) {
	if msgID == "" {
		return
	}
	s.mu.Lock()
	if s.repAck == nil {
		s.repAck = make(map[string]string)
	}
	s.repAck[msgID] = delivery
	s.mu.Unlock()
}

func (s *session) takeRepAck(msgID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.repAck[msgID]
	delete(s.repAck, msgID)
	return d
}

func (s *session) dropRepAck(msgID string) {
	s.mu.Lock()
	delete(s.repAck, msgID)
	s.mu.Unlock()
}

func (s *session) trackInflight(id string) {
	s.mu.Lock()
	s.inflight = append(s.inflight, id)
	s.mu.Unlock()
}

func (s *session) dropInflight(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.inflight[:0]
	for _, x := range s.inflight {
		if x != id {
			out = append(out, x)
		}
	}
	s.inflight = out
}

func (s *session) run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		s.close()
	}()
	go s.heartbeat(ctx)

	for {
		f, err := wire.Read(s.c)
		if err != nil {
			if err != io.EOF && !isClosed(err) {
				s.h.Logf("zmqcat session %s: %v", s.name, err)
			}
			return
		}
		s.touch()
		s.h.trace(s.name, "in", f)
		if err := s.handle(ctx, f); err != nil {
			s.reply(wire.Frame{Op: wire.OpErr, ID: f.ID, Error: err.Error()})
		}
	}
}

func (s *session) heartbeat(ctx context.Context) {
	iv := s.h.Heartbeat
	if iv <= 0 {
		return
	}
	live := s.h.Liveness
	if live <= 0 {
		live = DefaultLiveness
	}
	death := iv * time.Duration(live)
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := time.Now()
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				return
			}
			idle := s.idleLocked(now)
			s.mu.Unlock()
			if idle >= death {
				s.h.Logf("zmqcat session %s: heartbeat timeout", s.name)
				s.close()
				return
			}
			if idle >= iv {
				if err := s.reply(wire.Frame{Op: wire.OpPing}); err != nil {
					s.close()
					return
				}
			}
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
	case wire.OpPong:
		return nil
	case wire.OpPut:
		// Correlation ids double as mailbox message ids for idempotent
		// retries, so they must be scoped to the sender: two clients that
		// happen to pick the same id must not deduplicate each other.
		msg := mailbox.Msg{ID: s.scopedID(f.ID), From: s.from(f), Body: f.Payload()}
		if err := s.h.Bus.Put(f.Name, msg); err != nil {
			return err
		}
		return s.reply(wire.Frame{Op: wire.OpOK, ID: f.ID, Name: f.Name})
	case wire.OpTake:
		// take is the historical fire-and-forget pop: the lease exists only
		// so a failed write requeues.
		go s.takeJob(ctx, f, false)
		return nil
	case wire.OpReady:
		// A competing consumer keeps the lease until it reps, acks, or dies,
		// so a worker that dies holding a job gives it back.
		go s.takeJob(ctx, f, true)
		return nil
	case wire.OpReserve:
		go s.reserveJob(ctx, f)
		return nil
	case wire.OpAck:
		if err := s.h.Bus.Ack(f.Delivery); err != nil {
			return err
		}
		s.dropInflight(f.Delivery)
		return s.reply(wire.Frame{Op: wire.OpOK, ID: f.ID})
	case wire.OpNack:
		if err := s.h.Bus.Nack(f.Delivery); err != nil {
			return err
		}
		s.dropInflight(f.Delivery)
		return s.reply(wire.Frame{Op: wire.OpOK, ID: f.ID})
	case wire.OpReq:
		if f.ID == "" {
			return errors.New("req: missing correlation id")
		}
		key := s.scopedID(f.ID)
		now := time.Now()
		s.h.mu.Lock()
		s.h.expirePendingLocked(now)
		s.h.pending[key] = pendingReq{s: s, clientID: f.ID, expires: now.Add(s.h.pendingTTL())}
		s.h.mu.Unlock()
		// A retry re-enqueues rather than being dropped: Bus.Put deduplicates
		// while the job is still queued or leased, so this is a no-op for a
		// merely slow worker but recovers the request if the worker that held
		// it died. Without it a Lazy Pirate retry could never make progress.
		msg := mailbox.Msg{ID: key, From: s.from(f), Body: f.Payload()}
		if err := s.h.Bus.Put(f.Name, msg); err != nil {
			s.h.mu.Lock()
			if p, ok := s.h.pending[key]; ok && p.s == s {
				delete(s.h.pending, key)
			}
			s.h.mu.Unlock()
			return err
		}
		return nil
	case wire.OpRep:
		if d := s.takeRepAck(f.ID); d != "" {
			if s.claimInflight(d) {
				_ = s.h.Bus.Ack(d)
			}
		}
		s.h.mu.Lock()
		dest, found := s.h.pending[f.ID]
		delete(s.h.pending, f.ID)
		s.h.mu.Unlock()
		if err := s.reply(wire.Frame{Op: wire.OpOK, ID: f.ID}); err != nil {
			return err
		}
		if found && dest.s != nil {
			out := wire.Frame{
				Op:   wire.OpRep,
				ID:   dest.clientID,
				Name: f.Name,
				From: s.from(f),
				Body: f.Body,
				Text: f.Text,
			}
			_ = dest.s.reply(out)
		}
		return nil
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

// takeJob pops one job and writes it to this session. When hold is set the
// delivery stays leased until the worker reps, acks, nacks, or disconnects;
// otherwise it is acknowledged as soon as the frame is on the wire.
func (s *session) takeJob(ctx context.Context, f wire.Frame, hold bool) {
	msg, err := s.h.Bus.Take(ctx, f.Name)
	if err != nil {
		s.reply(wire.Frame{Op: wire.OpErr, ID: f.ID, Error: err.Error()})
		return
	}
	d, err := s.h.Bus.Hold(msg, s.takeLease())
	if err != nil {
		_ = s.h.Bus.Requeue(msg)
		s.reply(wire.Frame{Op: wire.OpErr, ID: f.ID, Error: err.Error()})
		return
	}
	s.trackInflight(d.ID)
	out := msgFrame(msg.ID, msg)
	if out.ID == "" {
		out.ID = f.ID
	}
	if hold {
		out.Delivery = d.ID
		out.Lease = d.Expires.Unix()
		s.trackRepAck(out.ID, d.ID)
	}
	if err := s.reply(out); err != nil {
		s.dropRepAck(out.ID)
		if s.claimInflight(d.ID) {
			_ = s.h.Bus.Nack(d.ID)
		}
		return
	}
	if hold {
		return
	}
	if s.claimInflight(d.ID) {
		_ = s.h.Bus.Ack(d.ID)
	}
}

// scopedID namespaces a client-chosen correlation id by session so ids from
// different clients cannot collide in the mailbox or the pending table. An
// empty id stays empty: the bus then generates a unique one and no
// deduplication applies.
func (s *session) scopedID(id string) string {
	if id == "" {
		return ""
	}
	return fmt.Sprintf("s%d:%s", s.sid, id)
}

func (h *Hub) pendingTTL() time.Duration {
	if h.PendingTTL > 0 {
		return h.PendingTTL
	}
	return DefaultPendingTTL
}

// expirePendingLocked drops req slots nobody ever answered.
func (h *Hub) expirePendingLocked(now time.Time) {
	for id, p := range h.pending {
		if !p.expires.IsZero() && now.After(p.expires) {
			delete(h.pending, id)
		}
	}
}

func (s *session) reserveJob(ctx context.Context, f wire.Frame) {
	d, err := s.h.Bus.Reserve(ctx, f.Name, time.Duration(f.Lease)*time.Second)
	if err != nil {
		s.reply(wire.Frame{Op: wire.OpErr, ID: f.ID, Error: err.Error()})
		return
	}
	s.trackInflight(d.ID)
	out := msgFrame(f.ID, d.Msg)
	out.Delivery = d.ID
	out.Lease = d.Expires.Unix()
	if err := s.reply(out); err != nil {
		if s.claimInflight(d.ID) {
			_ = s.h.Bus.Nack(d.ID)
		}
	}
}

func (s *session) takeLease() time.Duration {
	iv := s.h.Heartbeat
	if iv <= 0 {
		iv = DefaultHeartbeat
	}
	live := s.h.Liveness
	if live <= 0 {
		live = DefaultLiveness
	}
	d := iv * time.Duration(live) * 2
	if d < time.Minute {
		return time.Minute
	}
	return d
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
	if s.closed {
		s.mu.Unlock()
		return mailbox.ErrClosed
	}
	c := s.c
	name := s.name
	timeout := s.writeTimeout()
	s.mu.Unlock()
	s.h.trace(name, "out", f)
	if timeout > 0 {
		_ = c.SetWriteDeadline(time.Now().Add(timeout))
	}
	err := wire.Write(c, f)
	if timeout > 0 {
		_ = c.SetWriteDeadline(time.Time{})
	}
	if err != nil {
		s.close()
		return err
	}
	return nil
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
