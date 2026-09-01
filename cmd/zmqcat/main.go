// Command zmqcat is a ZMQ-style mailbox over Tailcat.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pyrex41/zmqcat"
	"tailscale.com/types/key"
)

func usage() {
	fmt.Fprintf(os.Stderr, `zmqcat — mailbox bus over Tailcat (WireGuard, no Tailscale account)

Usage:
  zmqcat serve [flags]
  zmqcat join  <token> [flags]
  zmqcat put   <mailbox> [text...]
  zmqcat take  <mailbox>
  zmqcat pub   <topic> [text...]
  zmqcat sub   [topic-prefix]
  zmqcat ping
  zmqcat ready <service>
  zmqcat req   <service> [text...]

Local apps (OpenResty, Python, harnesses) talk to --listen.
Default listen: unix:///tmp/zmqcat-<uid>.sock

Serve flags:
  --listen ADDR     local sidecar (unix:// or tcp://)
  --name NAME       this node's identity
  --allow nodekey:  restrict Tailcat clients (repeatable)
  --forward PORT    also expose localhost:PORT through the tunnel (repeatable)
  --quiet           hush Tailcat logs
  --local           no Tailcat; same-host bus only
  --mailbox PATH    durable mailbox state file (default in-memory)
  --trace           log ZMQC frames (op/id/name)
  --heartbeat DUR   session liveness interval (default 5s)

Join flags: --listen, --forward, --quiet

Client flags (put/take/pub/sub/ping/ready/req): --listen, --name
  req also: --timeout DUR (default 1s), --retries N (default 3)
`)
}

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 || os.Args[1] == "-h" || os.Args[1] == "--help" {
		usage()
		os.Exit(0)
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "serve":
		err = cmdServe(args)
	case "join":
		err = cmdJoin(args)
	case "put":
		err = cmdPut(args)
	case "take":
		err = cmdTake(args)
	case "pub":
		err = cmdPub(args)
	case "sub":
		err = cmdSub(args)
	case "ping":
		err = cmdPing(args)
	case "ready":
		err = cmdReady(args)
	case "req":
		err = cmdReq(args)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", "", "local sidecar address")
	name := fs.String("name", hostname(), "node identity")
	quiet := fs.Bool("quiet", false, "hush Tailcat logs")
	local := fs.Bool("local", false, "no Tailcat")
	mailboxPath := fs.String("mailbox", "", "durable mailbox state file")
	trace := fs.Bool("trace", false, "log ZMQC frames")
	heartbeat := fs.Duration("heartbeat", 5*time.Second, "session liveness interval")
	var allow allowList
	var fwd portList
	fs.Var(&allow, "allow", "allowed client nodekey (repeatable)")
	fs.Var(&fwd, "forward", "localhost TCP port to expose (repeatable)")
	_ = parseArgs(fs, args)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	n, err := zmqcat.Serve(ctx, zmqcat.Config{
		Listen:         *listen,
		Name:           *name,
		Quiet:          *quiet,
		LocalOnly:      *local,
		MailboxPath:    *mailboxPath,
		Trace:          *trace,
		Heartbeat:      *heartbeat,
		AllowedClients: allow.keys,
		ForwardPorts:   fwd.ports,
	})
	if err != nil {
		return err
	}
	defer n.Close()
	if tok := n.Token(); tok != "" {
		fmt.Println(tok)
	} else {
		fmt.Fprintf(os.Stderr, "local bus at %s\n", n.Listen())
	}
	<-ctx.Done()
	return nil
}

func cmdJoin(args []string) error {
	fs := flag.NewFlagSet("join", flag.ExitOnError)
	listen := fs.String("listen", "", "local sidecar address")
	quiet := fs.Bool("quiet", false, "hush Tailcat logs")
	var fwd portList
	fs.Var(&fwd, "forward", "listen locally and dial this port on the server")
	_ = parseArgs(fs, args)
	if fs.NArg() < 1 {
		return fmt.Errorf("join: missing token")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	n, err := zmqcat.Join(ctx, fs.Arg(0), zmqcat.Config{
		Listen:       *listen,
		Quiet:        *quiet,
		ForwardPorts: fwd.ports,
	})
	if err != nil {
		return err
	}
	defer n.Close()
	fmt.Fprintf(os.Stderr, "joined; local %s\n", n.Listen())
	<-ctx.Done()
	return nil
}

func cmdPut(args []string) error {
	fs, listen, name := clientFlags("put")
	_ = parseArgs(fs, args)
	if fs.NArg() < 1 {
		return fmt.Errorf("put: missing mailbox")
	}
	body, err := payload(fs.Args()[1:])
	if err != nil {
		return err
	}
	c, err := dial(*listen, *name)
	if err != nil {
		return err
	}
	defer c.Close()
	return c.Put(fs.Arg(0), string(body), body)
}

func cmdTake(args []string) error {
	fs, listen, name := clientFlags("take")
	_ = parseArgs(fs, args)
	if fs.NArg() < 1 {
		return fmt.Errorf("take: missing mailbox")
	}
	c, err := dial(*listen, *name)
	if err != nil {
		return err
	}
	defer c.Close()
	f, err := c.Take(fs.Arg(0))
	if err != nil {
		return err
	}
	os.Stdout.Write(f.Payload())
	if len(f.Payload()) == 0 || f.Payload()[len(f.Payload())-1] != '\n' {
		fmt.Println()
	}
	return nil
}

func cmdPub(args []string) error {
	fs, listen, name := clientFlags("pub")
	_ = parseArgs(fs, args)
	if fs.NArg() < 1 {
		return fmt.Errorf("pub: missing topic")
	}
	body, err := payload(fs.Args()[1:])
	if err != nil {
		return err
	}
	c, err := dial(*listen, *name)
	if err != nil {
		return err
	}
	defer c.Close()
	return c.Pub(fs.Arg(0), string(body), body)
}

func cmdSub(args []string) error {
	fs, listen, name := clientFlags("sub")
	_ = parseArgs(fs, args)
	prefix := ""
	if fs.NArg() > 0 {
		prefix = fs.Arg(0)
	}
	c, err := dial(*listen, *name)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Sub(prefix); err != nil {
		return err
	}
	for {
		f, err := c.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if f.Op == "msg" {
			fmt.Printf("%s\t%s\t%s\n", f.Name, f.From, f.Payload())
		}
	}
}

func cmdPing(args []string) error {
	fs, listen, name := clientFlags("ping")
	_ = parseArgs(fs, args)
	c, err := dial(*listen, *name)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Ping(); err != nil {
		return err
	}
	fmt.Println("pong")
	return nil
}

func cmdReady(args []string) error {
	fs, listen, name := clientFlags("ready")
	_ = parseArgs(fs, args)
	if fs.NArg() < 1 {
		return fmt.Errorf("ready: missing service")
	}
	c, err := dial(*listen, *name)
	if err != nil {
		return err
	}
	defer c.Close()
	f, err := c.Ready(fs.Arg(0))
	if err != nil {
		return err
	}
	os.Stdout.Write(f.Payload())
	if len(f.Payload()) == 0 || f.Payload()[len(f.Payload())-1] != '\n' {
		fmt.Println()
	}
	return nil
}

func cmdReq(args []string) error {
	fs, listen, name := clientFlags("req")
	timeout := fs.Duration("timeout", time.Second, "per-attempt wait")
	retries := fs.Int("retries", 3, "Lazy Pirate attempts")
	_ = parseArgs(fs, args)
	if fs.NArg() < 1 {
		return fmt.Errorf("req: missing service")
	}
	body, err := payload(fs.Args()[1:])
	if err != nil {
		return err
	}
	c, err := dial(*listen, *name)
	if err != nil {
		return err
	}
	defer c.Close()
	f, err := c.Request(fs.Arg(0), string(body), body, *timeout, *retries)
	if err != nil {
		return err
	}
	os.Stdout.Write(f.Payload())
	if len(f.Payload()) == 0 || f.Payload()[len(f.Payload())-1] != '\n' {
		fmt.Println()
	}
	return nil
}

func clientFlags(name string) (*flag.FlagSet, *string, *string) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	listen := fs.String("listen", "", "sidecar address")
	n := fs.String("name", hostname(), "identity")
	return fs, listen, n
}

func dial(listen, name string) (*zmqcat.Client, error) {
	c, err := zmqcat.Dial(listen)
	if err != nil {
		return nil, fmt.Errorf("dial sidecar (%s): %w\nstart `zmqcat serve` or `zmqcat join <token>` first", listen, err)
	}
	if name != "" {
		c.Name = name
		if err := c.Hello(name); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

func payload(args []string) ([]byte, error) {
	if len(args) > 0 {
		return []byte(strings.Join(args, " ")), nil
	}
	st, _ := os.Stdin.Stat()
	if st != nil && st.Mode()&os.ModeCharDevice == 0 {
		return io.ReadAll(os.Stdin)
	}
	return nil, nil
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "zmqcat"
	}
	return h
}

type allowList struct {
	keys []key.NodePublic
}

func (a *allowList) String() string { return "" }

type portList struct {
	ports []uint16
}

func (p *portList) String() string { return "" }
func (p *portList) Set(v string) error {
	n, err := strconv.ParseUint(v, 10, 16)
	if err != nil || n == 0 {
		return fmt.Errorf("forward: bad port %q", v)
	}
	p.ports = append(p.ports, uint16(n))
	return nil
}

func (a *allowList) Set(v string) error {
	var k key.NodePublic
	if err := k.UnmarshalText([]byte(v)); err != nil {
		return fmt.Errorf("allow: %w", err)
	}
	a.keys = append(a.keys, k)
	return nil
}
