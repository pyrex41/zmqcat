// Package zmqcat is a ZMQ-style mailbox bus over Tailcat.
//
// One process serves (prints a tailcat token). Others join with that token.
// Local processes talk over a unix/tcp socket so OpenResty, Python, and
// AI harnesses do not need Tailcat or libzmq.
package zmqcat

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/pyrex41/zmqcat/internal/addr"
	"github.com/pyrex41/zmqcat/internal/hub"
	"github.com/pyrex41/zmqcat/internal/mailbox"
	"github.com/pyrex41/zmqcat/internal/wire"
	"github.com/tailscale/tailcat"
	"tailscale.com/types/key"
	"tailscale.com/types/logger"
	"tailscale.com/wgengine/filter"
)

// MailboxPort is the TCP port on the Tailcat server that speaks zmqcat.
const MailboxPort uint16 = 7

// Config is Serve/Join options.
type Config struct {
	// MailboxPath enables durable at-least-once mailboxes on the serving node.
	// Empty keeps the historical in-memory behavior.
	MailboxPath string
	// Heartbeat is the session liveness interval. Zero means 5s; negative disables.
	Heartbeat time.Duration
	// Trace logs each ZMQC frame (session, direction, op/id/name).
	Trace bool
	// Listen is the local sidecar address (unix:// or tcp://). Empty uses
	// unix:///tmp/zmqcat-<uid>.sock.
	Listen string
	// Name is this node's hello identity.
	Name string
	// Logf logs diagnostics. Nil uses log.Printf. Set to a no-op to hush.
	Logf func(string, ...any)
	// Quiet suppresses Tailcat's own chatter.
	Quiet bool
	// AllowedClients, if non-empty, is a Tailcat nodekey allowlist.
	AllowedClients []key.NodePublic
	// DERPMapURL overrides Tailcat's default DERP map.
	DERPMapURL string
	// LocalOnly skips Tailcat (tests, same-host bus).
	LocalOnly bool
	// ForwardPorts are extra TCP ports on localhost to expose through the
	// tunnel (a real libzmq bind, buzz, whatever). Serve dials
	// 127.0.0.1:port; Join listens on 127.0.0.1:port and dials the server.
	ForwardPorts []uint16
}

// Node is a running serve or join sidecar.
type Node struct {
	cfg    Config
	logf   func(string, ...any)
	hub    *hub.Hub
	token  tailcat.ConnBlob
	listen string

	mu      sync.Mutex
	closers []io.Closer
}

func (n *Node) Token() string  { return string(n.token) }
func (n *Node) Listen() string { return n.listen }

func (n *Node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	var first error
	for i := len(n.closers) - 1; i >= 0; i-- {
		if err := n.closers[i].Close(); err != nil && first == nil {
			first = err
		}
	}
	n.closers = nil
	return first
}

func (n *Node) addCloser(c io.Closer) {
	n.mu.Lock()
	n.closers = append(n.closers, c)
	n.mu.Unlock()
}

// Serve starts a mailbox hub, a local sidecar, and (unless LocalOnly) a
// Tailcat server. The token is available from Node.Token after return.
func Serve(ctx context.Context, cfg Config) (*Node, error) {
	n, err := newLocal(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.LocalOnly {
		return n, nil
	}

	s := &tailcat.Server{
		Logf:           n.tailcatLogf(),
		DERPMapURL:     cfg.DERPMapURL,
		AllowedClients: cfg.AllowedClients,
		ServedTCPPorts: portRanges(append([]uint16{MailboxPort}, cfg.ForwardPorts...)),
		OnTCP: func(port uint16) func(net.Conn) {
			if port == MailboxPort {
				return func(c net.Conn) { n.hub.ServeConn(ctx, c) }
			}
			if !containsPort(cfg.ForwardPorts, port) {
				return nil
			}
			return forwardLocal(n.logf, port)
		},
	}
	if err := s.Start(); err != nil {
		n.Close()
		return nil, fmt.Errorf("tailcat serve: %w", err)
	}
	n.token = s.ConnBlob()
	n.addCloser(s)
	n.logf("zmqcat hub listening locally at %s", n.listen)
	n.logf("zmqcat tailcat token:\n%s", n.token)
	return n, nil
}

// Join connects to a Serve token, then exposes the same local sidecar.
// Each local connection is a Tailcat TCP session to MailboxPort.
func Join(ctx context.Context, token string, cfg Config) (*Node, error) {
	if token == "" {
		return nil, fmt.Errorf("join: empty token")
	}
	if cfg.LocalOnly {
		return nil, fmt.Errorf("join: LocalOnly is serve-only")
	}

	listen := cfg.Listen
	if listen == "" {
		listen = addr.Default()
	}
	ln, err := addr.Listen(listen)
	if err != nil {
		return nil, err
	}

	n := &Node{
		cfg:    cfg,
		logf:   logfOr(cfg.Logf),
		listen: listen,
		token:  tailcat.ConnBlob(token),
	}
	n.addCloser(ln)
	n.addCloser(closerFunc(func() error {
		if network, path, err := addr.Parse(listen); err == nil && network == "unix" {
			_ = os.Remove(path)
		}
		return nil
	}))

	cl := &tailcat.Client{
		Server:     tailcat.ConnBlob(token),
		Logf:       n.tailcatLogf(),
		DERPMapURL: cfg.DERPMapURL,
	}
	n.addCloser(cl)

	if _, err := cl.Ping(ctx); err != nil {
		n.Close()
		return nil, fmt.Errorf("tailcat ping: %w", err)
	}

	go n.acceptProxy(ctx, ln, cl)
	for _, p := range cfg.ForwardPorts {
		fln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(p))))
		if err != nil {
			n.Close()
			return nil, fmt.Errorf("forward listen %d: %w", p, err)
		}
		n.addCloser(fln)
		go n.acceptForward(ctx, fln, cl, p)
	}
	n.logf("zmqcat joined %s… locally at %s", trimToken(token), n.listen)
	return n, nil
}

func newLocal(cfg Config) (*Node, error) {
	listen := cfg.Listen
	if listen == "" {
		listen = addr.Default()
	}
	ln, err := addr.Listen(listen)
	if err != nil {
		return nil, err
	}
	var bus *mailbox.Bus
	if cfg.MailboxPath != "" {
		var err error
		bus, err = mailbox.OpenPersistent(cfg.MailboxPath, mailbox.DefaultMaxQueue, mailbox.DefaultMaxBody)
		if err != nil {
			_ = ln.Close()
			return nil, fmt.Errorf("mailbox storage: %w", err)
		}
	} else {
		bus = mailbox.New()
	}
	h := hub.New(bus)
	if cfg.Logf != nil {
		h.Logf = cfg.Logf
	}
	switch {
	case cfg.Heartbeat < 0:
		h.Heartbeat = 0
	case cfg.Heartbeat > 0:
		h.Heartbeat = cfg.Heartbeat
	}
	if cfg.Trace {
		logf := logfOr(cfg.Logf)
		h.Trace = func(session, dir string, f wire.Frame) {
			logf("zmqcat %s %s op=%s id=%s name=%s", session, dir, f.Op, f.ID, f.Name)
		}
	}
	n := &Node{
		cfg:    cfg,
		logf:   logfOr(cfg.Logf),
		hub:    h,
		listen: listen,
	}
	n.addCloser(ln)
	n.addCloser(closerFunc(func() error {
		h.Close()
		if network, path, err := addr.Parse(listen); err == nil && network == "unix" {
			_ = os.Remove(path)
		}
		return nil
	}))
	go n.acceptHub(context.Background(), ln)
	return n, nil
}

func (n *Node) acceptHub(ctx context.Context, ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go n.hub.ServeConn(ctx, c)
	}
}

func (n *Node) acceptForward(ctx context.Context, ln net.Listener, cl *tailcat.Client, port uint16) {
	for {
		local, err := ln.Accept()
		if err != nil {
			return
		}
		go spliceDial(ctx, n.logf, local, cl, port)
	}
}

func spliceDial(ctx context.Context, logf func(string, ...any), local net.Conn, cl *tailcat.Client, port uint16) {
	defer local.Close()
	remote, err := cl.DialTCPPort(ctx, port)
	if err != nil {
		logf("zmqcat dial :%d: %v", port, err)
		return
	}
	defer remote.Close()
	splice(local, remote)
}

func forwardLocal(logf func(string, ...any), port uint16) func(net.Conn) {
	dst := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port)))
	return func(c net.Conn) {
		defer c.Close()
		backend, err := net.Dial("tcp", dst)
		if err != nil {
			logf("zmqcat forward %s: %v", dst, err)
			return
		}
		defer backend.Close()
		splice(c, backend)
	}
}

func splice(a, b net.Conn) {
	errc := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(b, a); errc <- struct{}{} }()
	go func() { _, _ = io.Copy(a, b); errc <- struct{}{} }()
	<-errc
}

func portRanges(ports []uint16) []filter.PortRange {
	out := make([]filter.PortRange, 0, len(ports))
	for _, p := range ports {
		out = append(out, filter.PortRange{First: p, Last: p})
	}
	return out
}

func containsPort(ports []uint16, p uint16) bool {
	for _, x := range ports {
		if x == p {
			return true
		}
	}
	return false
}

func (n *Node) acceptProxy(ctx context.Context, ln net.Listener, cl *tailcat.Client) {
	for {
		local, err := ln.Accept()
		if err != nil {
			return
		}
		go spliceDial(ctx, n.logf, local, cl, MailboxPort)
	}
}

func (n *Node) tailcatLogf() logger.Logf {
	if n.cfg.Quiet {
		return logger.Discard
	}
	return n.logf
}

func logfOr(f func(string, ...any)) func(string, ...any) {
	if f != nil {
		return f
	}
	return log.Printf
}

func trimToken(s string) string {
	if len(s) < 16 {
		return s
	}
	return s[:12] + "…"
}

func listenDial(listen string) (net.Conn, error) {
	if listen == "" {
		listen = addr.Default()
	}
	return addr.Dial(listen)
}

type closerFunc func() error

func (f closerFunc) Close() error { return f() }
