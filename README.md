# zmqcat

A ZMQ-style **mailbox bus** over [Tailcat](https://github.com/tailscale/tailcat): WireGuard + NAT traversal, no Tailscale account, no root, no TUN device.

One side `serve`s and prints a token. Everyone else `join`s with that token. Local processes (OpenResty, Python, Go, an AI harness, buzz) talk to a unix/tcp sidecar — they never see Tailcat.

```
  harness / openresty / pyzmq-shaped scripts
                 │
          unix socket / tcp
                 │
              zmqcat sidecar
                 │
         Tailcat (DERP → direct UDP)
                 │
              zmqcat hub
           mailboxes + topics
```

## Install

```sh
go install github.com/pyrex41/zmqcat/cmd/zmqcat@latest
```

## Just works

Hub (prints a `tc…` token):

```sh
zmqcat serve
```

Spoke, anywhere NAT’d:

```sh
zmqcat join tcXXXXXXXXX
```

Same machine, no Tailcat:

```sh
zmqcat serve --local

# durable jobs (atomic local state file)
zmqcat serve --local --mailbox ./zmqcat-mailbox.json
```

Then, from any process on that host:

```sh
zmqcat put jobs '{"task":"summarize","id":1}'
zmqcat take jobs
zmqcat pub harness.done '{"ok":true}'
zmqcat sub harness
zmqcat ping
zmqcat ready jobs
zmqcat req echo '{"hello":true}'
```

Default sidecar: `unix:///tmp/zmqcat-<uid>.sock`. Override with `--listen tcp://127.0.0.1:5555` or `ZMQCAT_LISTEN`.

## Patterns

| ZMQ Guide | zmqcat |
| --- | --- |
| PUSH / PULL (jobs) | `put` / `take` on a named mailbox (FIFO, blocking take). Full mailbox **rejects** (`ErrDropped`); oldest is never dropped. |
| PUB / SUB (events) | `pub` / `sub` on a topic prefix (`events.` matches `events.foo`). Slow subscribers drop **that delivery only**. On `sub`, the last message per matching topic is replayed (last-value cache). |
| Majordomo-lite | `ready <service>` registers a competing consumer on that name; `put` / `take` / `reserve` on the same name share the queue. |
| Lazy Pirate | `req` / `rep` with a correlation `id`; the client retries with timeout and abandons after N attempts. Duplicate delivery is possible — ids are required. |
| Heartbeats + leases | `ping` / `pong` on the same session socket; any inbound frame is liveness. Default interval ~5s, death after ~3 missed. `reserve` visibility leases expire and requeue; session close **nacks** that session's inflight. |
| identity | `--name` / hello `from` |
| trace | `zmqcat serve --trace` (or `Config.Trace`) logs frames quietly (`op` / `id` / `name`) |

Queue cap is 1024 jobs. Pub/sub is intentionally lossy and ephemeral; mailboxes are not.

## Existing ZeroMQ broker

If you already have libzmq on a port, punch it through the same tunnel:

```sh
# host with the real ZMQ bind
zmqcat serve --forward 5555

# elsewhere
zmqcat join "$TOKEN" --forward 5555
# localhost:5555 is now the remote broker
```

## OpenResty

Sidecar on the nginx host (`zmqcat serve` or `zmqcat join $TOKEN`), then:

```lua
local zmqcat = require "zmqcat"  -- examples/openresty/zmqcat.lua
local c = assert(zmqcat.connect(os.getenv("ZMQCAT_LISTEN")))
c:put("jobs", '{"run":true}')
local msg = assert(c:take("jobs.out"))
ngx.print(msg.text)
c:close()
```

`examples/openresty/nginx.conf` exposes `POST /mbox/:name` and `GET /mbox/:name`.

## Python harness

```python
from zmqcat import Client   # examples/python/zmqcat.py
c = Client(name="agent")
c.put("jobs", '{"task":"ping"}')
print(c.take("jobs"))
```

`examples/python/agent.py` is a blocking worker loop: take `jobs`, put `jobs.out`, pub `harness.done`.

## Go library

```go
n, err := zmqcat.Serve(ctx, zmqcat.Config{})
token := n.Token()

peer, err := zmqcat.Join(ctx, token, zmqcat.Config{})
c, err := zmqcat.Dial(peer.Listen())
c.Put("inbox", "hello", nil)
```

## Wire protocol

Local and tunneled sessions are the same:

```
magic "ZMQC" | uint32be length | JSON
```

```json
{"v":1,"op":"put","name":"jobs","from":"agent","text":"..."}
```

Ops: `hello`, `put`, `take`, `pub`, `sub`, `unsub`, `ping`, `pong`, `reserve`, `ack`, `nack`, `ready`, `req`, `rep` → `ok` / `err` / `msg` / `pong` / `rep`. Binary payloads use JSON `body` (standard base64). `text` is the UTF-8 convenience field. Correlation `id` is required for `req`/`rep` (retries reuse the same id).

## Security

The Tailcat token **is** the capability. Anyone with it can join the bus.

- Ephemeral keys by default (new token every `serve`).
- `--allow nodekey:…` to pin client identities (from `tailcat genkey --client`).
- `--forward` is a hole to localhost; only expose ports you mean.

Public Tailcat DERP relays are rate-limited and have no SLA. Bring your own: `tailcat` `--region=derp.example.com` / `DERPMapURL`.

Tailcat itself has no API or CLI stability promise; zmqcat will track it.

## Why not “just Tailcat + netcat”?

Tailcat is a pipe. zmqcat is a **bus**: many clients, named mailboxes, topics, a stable local socket so nginx workers and agents do not each bring up WireGuard.

## Durable mailboxes (v2)

Set `Config.MailboxPath` when serving to persist queued and in-flight messages (JSON with atomic replacement; suitable for modest orchestrator traffic). The Go client exposes `Reserve`, `Ack`, and `Nack`. A reservation uses a visibility lease: acknowledgement removes it, nack, lease expiry, or session disconnect redelivers it, providing at-least-once delivery. Delivery IDs are unique and message IDs are generated when omitted. Payloads and mailbox names retain the existing bounds. `Put` rejects when the mailbox is full (`ErrDropped`) and never drops the oldest job.

Durability is local to the single serving hub; Tailcat provides transport encryption but mailbox-level identity/ACLs are not yet implemented. Consumers must tolerate duplicate deliveries and acknowledge only after successful processing. `Pub/Sub` remains intentionally lossy and ephemeral.
