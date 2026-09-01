package main

import (
	"flag"
	"testing"
)

func newFS() (*flag.FlagSet, *string, *bool, *int) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	listen := fs.String("listen", "", "")
	quiet := fs.Bool("quiet", false, "")
	retries := fs.Int("retries", 0, "")
	return fs, listen, quiet, retries
}

// The shape that silently broke `zmqcat join TOKEN --listen X`: Go's parser
// stops at the first positional, so the flag was dropped and the process
// bound the default socket instead.
func TestFlagsAfterPositional(t *testing.T) {
	fs, listen, quiet, retries := newFS()
	if err := parseArgs(fs, []string{"tc-token", "--listen", "unix:///tmp/x.sock", "--quiet", "--retries", "5"}); err != nil {
		t.Fatal(err)
	}
	if *listen != "unix:///tmp/x.sock" {
		t.Fatalf("listen = %q, want the flag after the positional to apply", *listen)
	}
	if !*quiet || *retries != 5 {
		t.Fatalf("quiet=%v retries=%d", *quiet, *retries)
	}
	if fs.NArg() != 1 || fs.Arg(0) != "tc-token" {
		t.Fatalf("positional lost: %v", fs.Args())
	}
}

func TestFlagsBeforePositionalStillWork(t *testing.T) {
	fs, listen, _, _ := newFS()
	if err := parseArgs(fs, []string{"--listen", "a", "svc", "payload"}); err != nil {
		t.Fatal(err)
	}
	if *listen != "a" || fs.NArg() != 2 || fs.Arg(1) != "payload" {
		t.Fatalf("listen=%q args=%v", *listen, fs.Args())
	}
}

func TestEqualsFormAndBoolDoNotEatPositionals(t *testing.T) {
	fs, listen, quiet, _ := newFS()
	if err := parseArgs(fs, []string{"--listen=b", "--quiet", "svc"}); err != nil {
		t.Fatal(err)
	}
	if *listen != "b" || !*quiet {
		t.Fatalf("listen=%q quiet=%v", *listen, *quiet)
	}
	if fs.NArg() != 1 || fs.Arg(0) != "svc" {
		t.Fatalf("a bool flag swallowed the positional: %v", fs.Args())
	}
}

// Everything after -- is positional, even if it looks like a flag.
func TestDoubleDashTerminator(t *testing.T) {
	fs, listen, _, _ := newFS()
	if err := parseArgs(fs, []string{"--listen", "a", "--", "--not-a-flag"}); err != nil {
		t.Fatal(err)
	}
	if *listen != "a" || fs.NArg() != 1 || fs.Arg(0) != "--not-a-flag" {
		t.Fatalf("listen=%q args=%v", *listen, fs.Args())
	}
}

// A JSON payload starting with { is positional, not a flag.
func TestJSONPayloadSurvives(t *testing.T) {
	fs, listen, _, _ := newFS()
	body := `{"jsonrpc":"2.0","method":"session/list"}`
	if err := parseArgs(fs, []string{"svc", body, "--listen", "a"}); err != nil {
		t.Fatal(err)
	}
	if *listen != "a" || fs.Arg(1) != body {
		t.Fatalf("listen=%q arg1=%q", *listen, fs.Arg(1))
	}
}
