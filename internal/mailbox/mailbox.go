// Package mailbox is an in-process ZMQ-style bus: named queues (push/pull)
// and prefix topics (pub/sub).
package mailbox

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

var (
	ErrClosed   = errors.New("mailbox: closed")
	ErrEmpty    = errors.New("mailbox: empty")
	ErrTooLarge = errors.New("mailbox: payload too large")
	ErrBadName  = errors.New("mailbox: empty name")
	ErrDropped  = errors.New("mailbox: queue full")
	ErrNotFound = errors.New("mailbox: delivery not found")
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

// Delivery is a reserved message with a visibility lease.
type Delivery struct {
	ID      string
	Msg     Msg
	Expires time.Time
}

// Bus is a process-local message bus.
type Bus struct {
	maxQueue int
	maxBody  int

	mu       sync.Mutex
	closed   bool
	boxes    map[string]*box
	subs     []sub
	last     map[string]Msg // last-value cache per pub topic
	path     string
	inflight map[string]Delivery
	stop     chan struct{}
	wg       sync.WaitGroup
}

func (b *Bus) Durable() bool { return b.path != "" }

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
	b := &Bus{
		maxQueue: maxQueue,
		maxBody:  maxBody,
		boxes:    make(map[string]*box),
		last:     make(map[string]Msg),
		inflight: make(map[string]Delivery),
		stop:     make(chan struct{}),
	}
	b.startSweeper()
	return b
}

// OpenPersistent opens a small JSON-backed durable mailbox store.
func OpenPersistent(path string, maxQueue, maxBody int) (*Bus, error) {
	b := NewWithLimits(maxQueue, maxBody)
	b.path = path
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return b, nil
	}
	if err != nil {
		return nil, err
	}
	var st struct {
		Boxes    map[string][]Msg    `json:"boxes"`
		Inflight map[string]Delivery `json:"inflight"`
	}
	if err = json.Unmarshal(raw, &st); err != nil {
		return nil, err
	}
	for n, q := range st.Boxes {
		b.boxes[n] = &box{q: q}
	}
	for id, d := range st.Inflight {
		b.inflight[id] = d
	}
	return b, nil
}

func (b *Bus) persistLocked() error {
	if b.path == "" {
		return nil
	}
	st := struct {
		Boxes    map[string][]Msg    `json:"boxes"`
		Inflight map[string]Delivery `json:"inflight"`
	}{map[string][]Msg{}, b.inflight}
	for n, bx := range b.boxes {
		if len(bx.q) > 0 {
			st.Boxes[n] = bx.q
		}
	}
	raw, e := json.Marshal(st)
	if e != nil {
		return e
	}
	t := b.path + ".tmp"
	if e = os.WriteFile(t, raw, 0600); e != nil {
		return e
	}
	return os.Rename(t, b.path)
}

func (b *Bus) startSweeper() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-b.stop:
				return
			case <-t.C:
				b.ExpireLeases()
			}
		}
	}()
}

// Close unblocks waiters and subscribers. Further Put/Take/Pub/Sub fail.
func (b *Bus) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	stop := b.stop
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
	b.mu.Unlock()
	if stop != nil {
		close(stop)
		b.wg.Wait()
	}
}

// Put appends msg onto named mailbox. If waiters exist, the oldest waiter
// gets it immediately. If the queue is full, Put rejects with ErrDropped
// and does not drop the oldest message (jobs are not lossy; pub/sub is).
func (b *Bus) Put(name string, msg Msg) error {
	if name == "" {
		return ErrBadName
	}
	if len(msg.Body) > b.maxBody {
		return ErrTooLarge
	}
	msg.Name = name
	if msg.ID == "" {
		msg.ID = newID()
	}
	if msg.At.IsZero() {
		msg.At = time.Now()
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	bx := b.boxLocked(name)
	if msg.ID != "" {
		for _, existing := range bx.q {
			if existing.ID == msg.ID {
				return nil
			}
		}
		for _, existing := range b.inflight {
			if existing.Msg.Name == name && existing.Msg.ID == msg.ID {
				return nil
			}
		}
	}
	if len(bx.waiters) > 0 {
		w := bx.waiters[0]
		bx.waiters = bx.waiters[1:]
		w <- msg
		close(w)
		return nil
	}
	if len(bx.q) >= b.maxQueue {
		return ErrDropped
	}
	bx.q = append(bx.q, msg)
	return b.persistLocked()
}

// Reserve claims a message with an at-least-once visibility lease.
func (b *Bus) Reserve(ctx context.Context, name string, lease time.Duration) (Delivery, error) {
	if name == "" {
		return Delivery{}, ErrBadName
	}
	if lease <= 0 {
		lease = time.Minute
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return Delivery{}, ErrClosed
	}
	b.requeueExpiredLocked(time.Now())
	bx := b.boxes[name]
	if bx == nil || len(bx.q) == 0 {
		return Delivery{}, ErrEmpty
	}
	m := bx.q[0]
	bx.q = bx.q[1:]
	d := Delivery{ID: newID(), Msg: m, Expires: time.Now().Add(lease)}
	b.inflight[d.ID] = d
	if e := b.persistLocked(); e != nil {
		return Delivery{}, e
	}
	return d, nil
}

// Hold places an already-taken message into the inflight set so close/expiry
// can Nack it. Used by the hub after Take/Ready before the write completes.
func (b *Bus) Hold(msg Msg, lease time.Duration) (Delivery, error) {
	if msg.Name == "" {
		return Delivery{}, ErrBadName
	}
	if lease <= 0 {
		lease = time.Minute
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return Delivery{}, ErrClosed
	}
	d := Delivery{ID: newID(), Msg: msg, Expires: time.Now().Add(lease)}
	b.inflight[d.ID] = d
	if e := b.persistLocked(); e != nil {
		delete(b.inflight, d.ID)
		return Delivery{}, e
	}
	return d, nil
}

func (b *Bus) Ack(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.inflight[id]; !ok {
		return ErrNotFound
	}
	delete(b.inflight, id)
	return b.persistLocked()
}

func (b *Bus) Nack(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	d, ok := b.inflight[id]
	if !ok {
		return ErrNotFound
	}
	delete(b.inflight, id)
	b.deliverLocked(d.Msg)
	return b.persistLocked()
}

// Requeue puts msg at the front of its mailbox, or hands it to a waiter.
// Used when a session fails after Take. Never drops the oldest job.
func (b *Bus) Requeue(msg Msg) error {
	if msg.Name == "" {
		return ErrBadName
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	b.deliverLocked(msg)
	return b.persistLocked()
}

func (b *Bus) deliverLocked(msg Msg) {
	bx := b.boxLocked(msg.Name)
	if len(bx.waiters) > 0 {
		w := bx.waiters[0]
		bx.waiters = bx.waiters[1:]
		w <- msg
		close(w)
		return
	}
	bx.q = append([]Msg{msg}, bx.q...)
}

// ExpireLeases requeues reserved messages whose visibility lease has elapsed.
func (b *Bus) ExpireLeases() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	if b.requeueExpiredLocked(time.Now()) {
		_ = b.persistLocked()
	}
}

func (b *Bus) requeueExpiredLocked(now time.Time) bool {
	changed := false
	for id, d := range b.inflight {
		if now.After(d.Expires) {
			delete(b.inflight, id)
			b.deliverLocked(d.Msg)
			changed = true
		}
	}
	return changed
}

func newID() string {
	var x [12]byte
	if _, e := rand.Read(x[:]); e != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("%x", x[:])
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
	b.requeueExpiredLocked(time.Now())
	bx := b.boxes[name]
	if bx == nil || len(bx.q) == 0 {
		return Msg{}, ErrEmpty
	}
	msg := bx.q[0]
	bx.q = bx.q[1:]
	if err := b.persistLocked(); err != nil {
		bx.q = append([]Msg{msg}, bx.q...)
		return Msg{}, err
	}
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
	b.last[topic] = msg
	for _, s := range b.subs {
		if matchTopic(topic, s.prefix) {
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
// The last message for each matching topic is replayed immediately.
func (b *Bus) Sub(prefix string) (<-chan Msg, func(), error) {
	ch := make(chan Msg, 64)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		close(ch)
		return ch, func() {}, ErrClosed
	}
	b.subs = append(b.subs, sub{prefix: prefix, ch: ch})
	for topic, msg := range b.last {
		if matchTopic(topic, prefix) {
			select {
			case ch <- msg:
			default:
			}
		}
	}
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
	b.requeueExpiredLocked(time.Now())
	bx := b.boxLocked(name)
	if len(bx.q) > 0 {
		msg := bx.q[0]
		bx.q = bx.q[1:]
		if err := b.persistLocked(); err != nil {
			bx.q = append([]Msg{msg}, bx.q...)
			return nil, err
		}
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
			_ = b.persistLocked()
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

func matchTopic(topic, prefix string) bool {
	if prefix == "" {
		return true
	}
	return matchPrefix(topic, prefix)
}

func matchPrefix(topic, prefix string) bool {
	if len(topic) < len(prefix) {
		return false
	}
	return topic[:len(prefix)] == prefix
}
