package main

import (
	"flag"
	"strings"
)

// parseArgs is flag.FlagSet.Parse that tolerates flags after positional
// arguments. Go's parser stops at the first non-flag, so `zmqcat join TOKEN
// --listen X` silently discarded --listen and joined on the default socket —
// a failure with no error message, where nothing can reach the bus.
//
// Flags and positionals are separated first, then parsed in the order the
// flag package expects. A flag that takes a value consumes the next argument
// unless it was written as --name=value; boolean flags never do.
func parseArgs(fs *flag.FlagSet, args []string) error {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(a, "-") || a == "-" {
			positional = append(positional, a)
			continue
		}
		flags = append(flags, a)
		name := strings.TrimLeft(a, "-")
		if idx := strings.IndexByte(name, '='); idx >= 0 {
			continue // value came with the flag
		}
		f := fs.Lookup(name)
		if f == nil {
			continue // let the flag package report it
		}
		if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() {
			continue
		}
		if i+1 < len(args) {
			i++
			flags = append(flags, args[i])
		}
	}
	// The explicit terminator keeps a positional that looks like a flag
	// (a JSON payload, a "--"-prefixed literal) from being parsed as one.
	return fs.Parse(append(flags, append([]string{"--"}, positional...)...))
}
