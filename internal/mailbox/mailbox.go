// Package mailbox is an in-process ZMQ-style bus: named queues (push/pull)
// and prefix topics (pub/sub).
package mailbox

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrClosed    = errors.New("mailbox: closed")
	ErrEmpty     = errors.New("mailbox: empty")
	ErrTooLarge  = errors.New("mailbox: payload too large")
	ErrBadName   = errors.New("mailbox: empty name")
	ErrDropped   = errors.New("mailbox: queue full, oldest dropped")
)

const (
	DefaultMaxQueue = 1024
	DefaultMaxBody  = 1 << 20 // 1 MiB
)

// Msg is one mailbox or topic payload.
type Msg struct {
	ID   string
	Name string // mailbox or topic
	From string
	Body []byte
	At   time.Time
}

// Bus is a process-local message bus.
type Bus struct {
	maxQueue int
	maxBody  int

	mu     sync.Mutex
	closed bool
	boxes  map[string]*box
	subs   []sub
}

type box struct {
	q       []Msg
	waiters []chan Msg
}

type sub struct {
	prefix string
	ch     chan Msg
}

// New returns a bus with default limits.
func New() *Bus {
	return NewWithLimits(DefaultMaxQueue, DefaultMaxBody)
}

// NewWithLimits returns a bus that keeps at most maxQueue messages per
// mailbox and rejects bodies larger than maxBody.
func NewWithLimits(maxQueue, maxBody int) *Bus {
	if maxQueue <= 0 {
		maxQueue = DefaultMaxQueue
	}
	if maxBody <= 0 {
		maxBody = DefaultMaxBody
	}
	return &Bus{
		maxQueue: maxQueue,
		maxBody:  maxBody,
		boxes:    make(map[string]*box),
	}
}

// Close unblocks waiters and subscribers. Further Put/Take/Pub/Sub fail.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for _, bx := range b.boxes {
		for _, w := range bx.waiters {
			close(w)
		}
		bx.waiters = nil
		bx.q = nil
	}
	for _, s := range b.subs {
		close(s.ch)
	}
	b.subs = nil
}

// Put appends msg onto named mailbox. If waiters exist, the oldest waiter
// gets it immediately. If the queue is full, the oldest queued message is
// dropped so the bus never blocks a publisher.
func (b *Bus) Put(name string, msg Msg) error {
	if name == "" {
		return ErrBadName
	}
	if len(msg.Body) > b.maxBody {
		return ErrTooLarge
	}
	msg.Name = name
	if msg.At.IsZero() {
		msg.At = time.Now()
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	bx := b.boxLocked(name)
	if len(bx.waiters) > 0 {
		w := bx.waiters[0]
		bx.waiters = bx.waiters[1:]
		w <- msg
		close(w)
		return nil
	}
	if len(bx.q) >= b.maxQueue {
		bx.q = bx.q[1:]
		bx.q = append(bx.q, msg)
		return ErrDropped
	}
	bx.q = append(bx.q, msg)
	return nil
}

// Take pops the next message from name, waiting until ctx is done.
func (b *Bus) Take(ctx context.Context, name string) (Msg, error) {
	if name == "" {
		return Msg{}, ErrBadName
	}
	ch, err := b.takeChan(name)
	if err != nil {
		return Msg{}, err
	}
	select {
	case msg, ok := <-ch:
		if !ok {
			return Msg{}, ErrClosed
		}
		return msg, nil
	case <-ctx.Done():
		b.cancelWaiter(name, ch)
		return Msg{}, ctx.Err()
	}
}

// TryTake pops a message without waiting.
func (b *Bus) TryTake(name string) (Msg, error) {
	if name == "" {
		return Msg{}, ErrBadName
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return Msg{}, ErrClosed
	}
	bx := b.boxes[name]
	if bx == nil || len(bx.q) == 0 {
		return Msg{}, ErrEmpty
	}
	msg := bx.q[0]
	bx.q = bx.q[1:]
	return msg, nil
}

// Pub delivers msg to every subscriber whose prefix matches topic.
func (b *Bus) Pub(topic string, msg Msg) error {
	if topic == "" {
		return ErrBadName
	}
	if len(msg.Body) > b.maxBody {
		return ErrTooLarge
	}
	msg.Name = topic
	if msg.At.IsZero() {
		msg.At = time.Now()
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	for _, s := range b.subs {
		if len(s.prefix) == 0 || matchPrefix(topic, s.prefix) {
			select {
			case s.ch <- msg:
			default:
				// Slow subscriber: drop this delivery, keep the sub.
			}
		}
	}
	return nil
}

// Sub returns a channel of matching publications. The channel is closed
// when the bus is closed or the returned cancel func is called.
// Buffer is 64; overflow drops that delivery for this subscriber only.
func (b *Bus) Sub(prefix string) (<-chan Msg, func(), error) {
	ch := make(chan Msg, 64)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		close(ch)
		return ch, func() {}, ErrClosed
	}
	b.subs = append(b.subs, sub{prefix: prefix, ch: ch})
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.unsub(ch)
		})
	}
	return ch, cancel, nil
}

// Depth returns queued messages in a mailbox.
func (b *Bus) Depth(name string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	bx := b.boxes[name]
	if bx == nil {
		return 0
	}
	return len(bx.q)
}

func (b *Bus) takeChan(name string) (chan Msg, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	bx := b.boxLocked(name)
	if len(bx.q) > 0 {
		msg := bx.q[0]
		bx.q = bx.q[1:]
		ch := make(chan Msg, 1)
		ch <- msg
		close(ch)
		return ch, nil
	}
	ch := make(chan Msg, 1)
	bx.waiters = append(bx.waiters, ch)
	return ch, nil
}

func (b *Bus) cancelWaiter(name string, ch chan Msg) {
	b.mu.Lock()
	defer b.mu.Unlock()
	bx := b.boxes[name]
	if bx == nil {
		return
	}
	out := bx.waiters[:0]
	for _, w := range bx.waiters {
		if w != ch {
			out = append(out, w)
		}
	}
	bx.waiters = out
	select {
	case msg, ok := <-ch:
		if ok {
			// Message arrived as we cancelled: put it back at the front.
			bx.q = append([]Msg{msg}, bx.q...)
		}
	default:
	}
}

func (b *Bus) unsub(ch chan Msg) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.subs[:0]
	for _, s := range b.subs {
		if s.ch != ch {
			out = append(out, s)
			continue
		}
		close(s.ch)
	}
	b.subs = out
}

func (b *Bus) boxLocked(name string) *box {
	bx := b.boxes[name]
	if bx == nil {
		bx = &box{}
		b.boxes[name] = bx
	}
	return bx
}

func matchPrefix(topic, prefix string) bool {
	if len(topic) < len(prefix) {
		return false
	}
	return topic[:len(prefix)] == prefix
}
