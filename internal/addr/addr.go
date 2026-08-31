// Package addr parses zmqcat listen strings.
package addr

import (
	"fmt"
	"net"
	"os"
	"strings"
)

// Default returns a per-user unix socket path.
func Default() string {
	return fmt.Sprintf("unix:///tmp/zmqcat-%d.sock", os.Getuid())
}

// Parse turns "unix:///tmp/x.sock", "/tmp/x.sock", "tcp://127.0.0.1:5555",
// or "127.0.0.1:5555" into a net.Listen network and address.
func Parse(s string) (network, address string, err error) {
	if s == "" {
		s = Default()
	}
	switch {
	case strings.HasPrefix(s, "unix://"):
		return "unix", strings.TrimPrefix(s, "unix://"), nil
	case strings.HasPrefix(s, "tcp://"):
		return "tcp", strings.TrimPrefix(s, "tcp://"), nil
	case strings.HasPrefix(s, "/"), strings.HasPrefix(s, "."):
		return "unix", s, nil
	case strings.Contains(s, ":"):
		return "tcp", s, nil
	default:
		return "", "", fmt.Errorf("listen address %q: want unix://, tcp://, path, or host:port", s)
	}
}

func Listen(s string) (net.Listener, error) {
	network, address, err := Parse(s)
	if err != nil {
		return nil, err
	}
	if network == "unix" {
		_ = os.Remove(address)
	}
	return net.Listen(network, address)
}

func Dial(s string) (net.Conn, error) {
	network, address, err := Parse(s)
	if err != nil {
		return nil, err
	}
	return net.Dial(network, address)
}
