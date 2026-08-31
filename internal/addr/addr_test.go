package addr

import "testing"

func TestParse(t *testing.T) {
	tests := []struct {
		in, net, addr string
	}{
		{"unix:///tmp/x.sock", "unix", "/tmp/x.sock"},
		{"/tmp/x.sock", "unix", "/tmp/x.sock"},
		{"tcp://127.0.0.1:5555", "tcp", "127.0.0.1:5555"},
		{"127.0.0.1:5555", "tcp", "127.0.0.1:5555"},
	}
	for _, tt := range tests {
		n, a, err := Parse(tt.in)
		if err != nil {
			t.Fatalf("%s: %v", tt.in, err)
		}
		if n != tt.net || a != tt.addr {
			t.Fatalf("%s: got %s %s", tt.in, n, a)
		}
	}
}
