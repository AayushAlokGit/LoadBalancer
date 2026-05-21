"""A load balancer built on asyncio — one thread, one event loop.

Same job as multi_threaded_load_balancer.py: accept a client, forward its
request to the backend, relay the response back. The difference is the
concurrency model.

    multi_threaded_load_balancer.py : one OS thread per connection.
                                      The OS preempts threads.
    event_loop_load_balancer.py     : one thread running an event loop.
                                      Coroutines yield cooperatively at 'await'.

Every call that would block on I/O (accept, recv, send, connect) becomes an
'await'. At each await the coroutine hands control back to the loop, which
runs whatever other connection is ready. Thousands of mostly-idle
connections cost KB each here, vs ~1 MB per thread.

Kept low-level on purpose (raw sockets + loop.sock_*), matching the project's
"minimal abstractions" rule — no asyncio.start_server, no StreamReader.

Note: there is NO log lock here. With a single thread, a coroutine cannot be
interrupted mid-print() — control only ever switches at an 'await'. So log
lines cannot interleave, and the threading.Lock the threaded version needs
simply isn't required.
"""

import asyncio
import socket
from datetime import datetime

from http_utils import parse_request, recv_request_non_blocking 

HOST = "127.0.0.1"
PORT = 9000

# The single backend we forward everything to (your threaded_server.py).
BACKEND_HOST = "127.0.0.1"
BACKEND_PORT = 8000

# Seconds a client gets to send a complete request. Enforced with
# asyncio.wait_for() instead of socket.settimeout() — see handle_client.
CLIENT_TIMEOUT = 5.0


def log(message):
    """Print one timestamped line, prefixed with the current task's name.

    asyncio names tasks Task-1, Task-2, ... from an internal counter — the
    same idea as threading naming threads Thread-1, Thread-2.
    """
    try:
        name = asyncio.current_task().get_name()
    except (RuntimeError, AttributeError):
        name = "main"
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} [{name}] {message}")


async def forward_to_backend(loop, request_bytes):
    """Send request_bytes to the backend and return its full response bytes.

    The load balancer acts as a CLIENT here. Same shape as the threaded
    version's forward_to_backend, but connect/sendall/recv are all awaited.
    The recv loop runs until the backend closes the connection (it sends
    'Connection: close'), which marks the response complete.
    """
    backend = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # required: loop.sock_* needs non-blocking sockets. Blocking sockets disrupt entire event loop thread
    backend.setblocking(False)       
    try:
        await loop.sock_connect(backend, (BACKEND_HOST, BACKEND_PORT))
        await loop.sock_sendall(backend, request_bytes)

        response = b""
        while True:
            chunk = await loop.sock_recv(backend, 4096)
            if not chunk:            # backend closed -> response is complete
                break
            response += chunk
        return response
    finally:
        backend.close()


async def handle_client(conn, addr):
    """Relay one client <-> backend exchange. Runs as its own asyncio Task."""
    loop = asyncio.get_running_loop()
    try:
        # asyncio has no per-socket timeout like socket.settimeout(). Instead
        # we wrap the coroutine in wait_for(): if recv_request_blocking hasn't finished
        # within CLIENT_TIMEOUT seconds, it is cancelled and TimeoutError is
        # raised. This is how a silent client (see silent_client.py) gets
        # dropped instead of pinning a task forever.
        try:
            request_bytes = await asyncio.wait_for(
                recv_request_non_blocking(loop, conn), timeout=CLIENT_TIMEOUT
            )
        except asyncio.TimeoutError:
            log(f"timeout: {addr[0]} connected but sent no full request in "
                f"{CLIENT_TIMEOUT}s — dropping")
            return

        if not request_bytes:
            return  # client connected then left without sending anything

        method, path, _, _ = parse_request(request_bytes)
        log(f"{method} {path} from {addr[0]} -> forwarding to "
            f"{BACKEND_HOST}:{BACKEND_PORT}")

        try:
            response_bytes = await forward_to_backend(loop, request_bytes)
        except ConnectionRefusedError:
            # The backend is down. Don't crash -- tell the client honestly.
            log(f"backend {BACKEND_HOST}:{BACKEND_PORT} refused the connection")
            body = b"Bad Gateway: backend unavailable\n"
            response_bytes = (
                b"HTTP/1.1 502 Bad Gateway\r\n"
                b"Content-Type: text/plain\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                b"Connection: close\r\n"
                b"\r\n"
                + body
            )

        await loop.sock_sendall(conn, response_bytes)
        log(f"{method} {path} from {addr[0]} <- relayed {len(response_bytes)} bytes")
    except Exception as exc:
        # One task crashing must not take down the balancer. Log and move on.
        log(f"error handling {addr[0]}: {exc!r}")
    finally:
        conn.close()


async def main():
    loop = asyncio.get_running_loop() # the event loop

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(128)
    server.setblocking(False)        # required for loop.sock_accept

    log(f"Event-loop load balancer listening on http://{HOST}:{PORT}")
    log(f"Forwarding all requests to http://{BACKEND_HOST}:{BACKEND_PORT}")

    try:
        while True:
            # await, not block: while we wait for a connection the loop is
            # free to run every in-flight handle_client task.
            conn, addr = await loop.sock_accept(server)
            conn.setblocking(False)  # required for loop.sock_recv / sock_sendall
            log(f"new connection from {addr[0]}:{addr[1]}")

            # create_task schedules handle_client on the loop and returns
            # immediately -- the asyncio equivalent of starting a thread.
            asyncio.create_task(handle_client(conn, addr))
    finally:
        server.close()


if __name__ == "__main__":
    # asyncio.run builds the event loop, runs main() until it finishes, and
    # cleans up. A Ctrl+C surfaces here as KeyboardInterrupt.
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down")
