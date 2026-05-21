# Load Balancer — Design Notes & Learnings

A living knowledge store for this project: the **design decisions** made while
building the load balancer from raw sockets, and the **networking / concurrency
concepts** learned along the way. The goal is that anyone (including future-me)
can read this and understand not just *what* the code does, but *why*.

Keep it coherent — renumber, rewrite, and reorganize freely so it stays a clean,
accurate reference. Accuracy beats preserving history.

---

## How to add an entry

**Decisions** use this template:

```
### Dn — Short title
**Context:** what situation forced a choice.
**Decision:** what we chose.
**Why:** the reasoning, including alternatives rejected.
**Status:** Active | Superseded by Dx | Under review
```

**Learnings** are concepts worth remembering — keep them short and concrete.

---

## Decisions

### D1 — Raw sockets, no frameworks
**Context:** Building an HTTP server in Python; could use `http.server`, Flask, etc.
**Decision:** Build directly on `socket` — no frameworks, no `http.server`. Parse
HTTP off the wire by hand.
**Why:** The project's purpose is *learning how things work*. Abstractions hide
exactly the parts worth understanding (TCP streams, HTTP framing, the accept loop).
**Status:** Active.

### D2 — Console-only logging
**Context:** Need visibility into incoming requests.
**Decision:** Log to the console with `print()`, not to a file.
**Why:** Simpler; immediate feedback while developing. A file adds rotation,
flushing, and path concerns with no learning payoff right now.
**Status:** Active.

### D3 — Bind to 127.0.0.1, not 0.0.0.0
**Context:** Choosing the bind address.
**Decision:** Bind to `127.0.0.1` (localhost only).
**Why:** `0.0.0.0` exposes the server on every network interface (reachable by
others on the LAN). For local development that's unnecessary attack surface.
**Status:** Active.

### D4 — Evolve concurrency in stages
**Context:** A single-threaded server blocks everyone behind one slow client.
**Decision:** Progress deliberately: single-threaded → thread-per-connection →
asyncio event loop. Keep each stage as its own file.
**Why:** Each stage isolates one lesson. Keeping old files preserves the
before/after contrast.
**Status:** Active. All three stages now exist as separate files. See D12.

### D5 — `settimeout(1.0)` on the listening socket
**Context:** On Windows, a blocking `accept()` swallows Ctrl+C — the server
couldn't be stopped until a connection happened to arrive.
**Decision:** Set a 1-second timeout on the *listening* socket; wrap `accept()`
in `try/except socket.timeout: continue`.
**Why:** The timeout returns control to Python once a second so a pending
`KeyboardInterrupt` gets processed.
**Status:** Active.

### D6 — `Connection: close` on every response
**Context:** HTTP can keep a TCP connection open for multiple requests.
**Decision:** Send `Connection: close` and close the socket after one response.
**Why:** Simplest correct behavior. It also makes response framing trivial for
the load balancer — read until the socket closes (see D8).
**Status:** Active. A keep-alive design would revisit this.

### D7 — Forward raw bytes verbatim (raw socket, not `http.client`)
**Context:** The LB already has the client's complete request as bytes.
**Decision:** The LB forwards request/response bytes verbatim over a raw socket;
it does not re-parse them through `http.client`.
**Why:** Using `http.client` would mean parsing the bytes into objects and
re-serializing them — more code, and every step risks subtly altering the request.
A proxy's nature is "move bytes through without caring what they mean."
(`client.py` *does* use `http.client` — because it *invents* requests from
nothing, which is the opposite job.)
**Status:** Active.

### D8 — LB reads the backend response until the socket closes
**Context:** The LB must know when the backend's response is complete.
**Decision:** Loop `recv()` until it returns empty bytes.
**Why:** Works because the backend sends `Connection: close` (D6) and hangs up
when done. A keep-alive backend would instead require parsing `Content-Length`
or chunked encoding.
**Status:** Active. Tied to D6.

### D9 — Hand-built 502 when the backend is unreachable
**Context:** If the backend is down, `connect()` raises `ConnectionRefusedError`.
**Decision:** Catch it and return a hand-built `502 Bad Gateway` to the client.
**Why:** A proxy should fail honestly, not crash the worker. Mirrors how real
load balancers behave.
**Status:** Active.

### D10 — Per-connection timeout to drop silent clients
**Context:** A client can complete the TCP handshake and then send nothing (or a
partial request). The worker thread blocks in `recv()` forever and leaks — the
slowloris failure mode (see `silent_client.py`).
**Decision:** Give every *accepted* connection its own deadline for sending a
request: `conn.settimeout(CLIENT_TIMEOUT)` in the threaded servers.
**Why:** The listening socket's timeout (D5) only affects `accept()` — the
connection socket is separate and has no timeout of its own. A per-connection
deadline makes `recv()` raise `socket.timeout` so the thread can exit.
**Status:** Active for the threaded servers. The asyncio LB enforces the same
deadline with `asyncio.wait_for()` instead of `settimeout()` — see D12.

### D11 — Shared helpers in `http_utils.py`
**Context:** `recv_request_blocking`/`parse_request` were duplicated across server files.
**Decision:** Extract them into `http_utils.py` and import.
**Why:** One source of truth. (Caught a real bug: a caller unpacked 2 values from
a 4-tuple return — see Learnings.)
**Status:** Active.

### D12 — asyncio event loop for the load balancer
**Context:** The load balancer must handle many concurrent connections. The
thread-per-connection model costs ~1 MB of stack per connection plus growing
OS-scheduler overhead as thread count rises.
**Decision:** Adopt a single-threaded asyncio event loop as the concurrency
model for the load balancer (`event_loop_load_balancer.py`). The threaded
version (`multi_threaded_load_balancer.py`) is kept alongside it for comparison.
**Why:** A proxy is pure I/O — it shuffles bytes with no per-request CPU work.
asyncio scales far cheaper here: ~KB per connection vs ~1 MB per thread, and no
scheduler overhead. The "heavy data" worry does not apply — heavy data transfer
is I/O, which the event loop handles by design (chunked `await`s yield
constantly); only CPU work between `await`s would block the loop, and a proxy
has none. Threads would only win on genuine per-request CPU work.
**Status:** Active. Resolves the former "Threads vs asyncio" open question.

---

## Open questions / under review

- **Phase 2:** round-robin over a list of backends.
- **Backend-side timeout:** `forward_to_backend()` has no timeout on the backend
  socket — a hung backend would still stall an LB thread.
- **Header rewriting:** real LBs add `X-Forwarded-For` and fix the `Host` header;
  currently bytes are forwarded untouched.

---

## Learnings (concepts)

### TCP is a byte stream, not a message queue
One `recv()` may return half a request, a whole one, or one-and-a-half. You must
loop until you've seen the end-of-headers marker (`\r\n\r\n`), then read exactly
`Content-Length` more bytes for the body.

### HTTP wire format
Request/response = a request/status line, then `Name: value` headers, a blank
line, then the body. Every line ends with CRLF (`\r\n`), not just `\n`.

### `0.0.0.0` vs `127.0.0.1`
`0.0.0.0` is a wildcard bind — all network interfaces, reachable from the LAN.
`127.0.0.1` is loopback only — same machine.

### `listen(backlog)`
`backlog` is the **maximum** size of the kernel's queue of connections waiting to
be `accept()`ed. It is not a threshold or trigger — `accept()` returns as soon as
*one* connection is queued.

### `connect()` and `send()` are separate steps
The TCP 3-way handshake (`connect`) carries zero application bytes. A connection
can sit `ESTABLISHED` forever with nothing ever sent. This is why a "silent
client" exists — and why per-connection timeouts (D10) are needed.

### Listening socket vs connection socket
`accept()` returns a **new, separate** socket for that one client. Options like
timeouts set on the listening socket do **not** carry over — the connection
socket must be configured on its own.

### The taxonomy of "timeouts"
Distinct things, often all called "connection timeout": connect timeout, read/recv
timeout, idle/keep-alive timeout (closes idle sockets), request/header timeout
(anti-slowloris), and OS-level TCP keepalive (`SO_KEEPALIVE`, dead-peer detection).

### DNS resolution happens in `getaddrinfo()`
Neither `http.client` nor raw `socket` code resolves names itself — both funnel
into the OS call `getaddrinfo()`. IP literals like `127.0.0.1` short-circuit it:
no hosts-file lookup, no DNS query, no network packet. Python does not cache DNS;
the OS does.

### Threads vs asyncio — the real distinction
Blocking on **I/O** does not block an event loop; blocking the **CPU** (or calling
a sync function) does. Heavy *data transfer* is I/O — chunked `await`s yield
constantly, so it never starves other connections. asyncio's only real weakness
is per-request CPU work. CPython's GIL means threads don't give true CPU
parallelism anyway.

### Concurrency vs parallelism — the umbrella distinction
Two different things, constantly conflated:
- **Concurrency** = *structure*: making overlapping progress on many tasks.
  Does **not** require multiple CPU cores.
- **Parallelism** = *execution*: literally running things at the same instant.
  **Requires** multiple cores.

"Concurrency is dealing with many things at once; parallelism is doing many
things at once." A single-threaded event loop is concurrent but not parallel —
and it is still very much *concurrent programming*.

Three ways to get concurrency in Python — **all three are concurrent
programming**, async included:

| Model | Unit | Switching | Parallel CPU? |
|---|---|---|---|
| asyncio (event loop) | 1 thread | cooperative — at `await` | No |
| threading | N threads | preemptive — OS decides | **No** (the GIL) |
| multiprocessing | N processes | preemptive — OS decides | **Yes** |

The GIL (Global Interpreter Lock) lets only one thread run Python bytecode at a
time, so CPython threads give concurrency but **not** CPU parallelism. They
still help I/O-bound work (the GIL is released during blocking I/O). True
parallelism comes from `multiprocessing`.

Caveat: "async = single-threaded" holds for Python's asyncio and JavaScript,
but not universally — Go and Rust's Tokio run async across many threads. Async
means "switch at explicit yield points," not "one thread."

### How an event loop works
The loop never busy-polls sockets. It delegates waiting to the OS **selector**
(`epoll` on Linux, `kqueue` on macOS, `select`/IOCP on Windows). When all
coroutines are paused, the loop makes one OS call: "sleep this thread; wake me
when any of these file descriptors is ready, or when this timeout expires."

One iteration ("tick"):
1. Run the callbacks already queued as ready.
2. Compute a timeout = time until the soonest pending timer (0 if work is
   already ready; indefinite if nothing is pending).
3. Block in the selector until an FD becomes ready or the timeout expires.
4. Queue the tasks waiting on now-ready FDs, and any timers now due.
5. Repeat.

So "stop waiting" is triggered by the selector call returning — the OS tells
the loop a socket is ready. `await loop.sock_recv(conn, ...)` internally
registers the FD with the selector, suspends the task, and hands control back.

Scheduling when multiple tasks are ready: `_ready` is a **FIFO deque** — no
priorities, strictly arrival order. (`_scheduled`, the timer queue, is a
min-heap ordered by deadline.) A resumed task runs until its **next `await`** —
never time-sliced. Callbacks scheduled *during* a tick run on the *next* tick,
which guarantees the loop returns to I/O polling regularly (fairness).

### Tuple-unpacking arity must match
`parse_request` returns a 4-tuple `(method, path, headers, body)`. Unpacking it
into 2 names raised `ValueError: too many values to unpack (expected 2, got 4)`.
Use `method, path, _, _ = parse_request(...)` to discard unused values.
