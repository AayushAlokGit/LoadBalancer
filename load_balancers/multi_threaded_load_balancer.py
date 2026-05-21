"""A low-level HTTP load balancer (Phase 1: single backend).

This program is both a server AND a client at the same time:

    client  ──TCP──▶  load_balancer  ──TCP──▶  backend server
            (we are a            (this file)        (we are a
             server here)                            client here)

For every client connection we: read the request, open a fresh TCP
connection to the backend, forward the request bytes verbatim, read the
backend's full response, and relay that response back to the client.
"""

import socket
import threading
from datetime import datetime

from http_utils import parse_request, recv_request_blocking

HOST = "127.0.0.1"
PORT = 9000

# The single backend we forward everything to (your threaded_server.py).
BACKEND_HOST = "127.0.0.1"
BACKEND_PORT = 8000

# Seconds a client gets to send a complete request before recv() gives up.
# Without this, a client that connects and stays silent pins a worker thread
# forever (see silent_client.py).
CLIENT_TIMEOUT = 10.0

# print() from many threads can interleave mid-line; this keeps each line atomic.
log_lock = threading.Lock()


def log(message):
    """Print one timestamped line to the console, prefixed with the thread name."""
    thread_name = threading.current_thread().name
    with log_lock:
        print(f"{datetime.now():%Y-%m-%d %H:%M:%S} [{thread_name}] {message}")

def forward_to_backend(request_bytes):
    """Send request_bytes to the backend and return its full response bytes.

    Here the load balancer acts as a CLIENT: it opens its own TCP socket,
    sends the request, and reads the reply.

    Reading the reply is the interesting part. We loop recv() until it
    returns empty bytes -- i.e. until the backend closes the connection.
    That works because the backend sends "Connection: close" and then hangs
    up once the response is complete. A keep-alive backend would NOT close,
    so there we'd have to parse Content-Length instead. (Phase 1 keeps it
    simple on purpose.)
    """
    backend = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        backend.connect((BACKEND_HOST, BACKEND_PORT))
        backend.sendall(request_bytes)

        response = b""
        while True:
            chunk = backend.recv(4096)
            if not chunk:            # backend closed -> the response is complete
                break
            response += chunk
        return response
    finally:
        backend.close()


def handle_client(conn, addr):
    """Relay one client <-> backend exchange. Runs inside its own thread."""
    try:
        request_bytes = recv_request_blocking(conn)
        if not request_bytes:
            return  # client connected then left without sending anything

        # parse_request returns (method, path, headers, body); the LB only
        # needs method/path for its log line, so discard the other two.
        method, path, _, _ = parse_request(request_bytes)
        log(f"{method} {path} from {addr[0]} -> forwarding to "
            f"{BACKEND_HOST}:{BACKEND_PORT}")

        try:
            response_bytes = forward_to_backend(request_bytes)
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

        conn.sendall(response_bytes)
        log(f"{method} {path} from {addr[0]} <- relayed {len(response_bytes)} bytes")
    except socket.timeout:
        # The client connected but didn't send a full request in time.
        # recv() raised instead of blocking forever, so this thread can exit.
        log(f"timeout: {addr[0]} connected but sent no full request in "
            f"{CLIENT_TIMEOUT}s — dropping")
    except Exception as exc:
        # One thread crashing must not take down the balancer. Log and move on.
        log(f"error handling {addr[0]}: {exc!r}")
    finally:
        conn.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(128)

    # accept() gives up after 1s so Ctrl+C is noticed promptly on Windows.
    server.settimeout(1.0)
    log(f"Load balancer listening on http://{HOST}:{PORT}")
    log(f"Forwarding all requests to http://{BACKEND_HOST}:{BACKEND_PORT}")

    try:
        while True:
            try:
                conn, addr = server.accept()
            except socket.timeout:
                continue  # no one connected this second; loop and check for Ctrl+C

            # settimeout(1.0) above is on the LISTENING socket and only affects
            # accept(). The accepted connection socket has no timeout of its
            # own, so give each client its own deadline for sending a request.
            conn.settimeout(CLIENT_TIMEOUT)

            worker = threading.Thread(
                target=handle_client,
                args=(conn, addr),
                daemon=True,
            )
            worker.start()
    except KeyboardInterrupt:
        log("Shutting down")
    finally:
        server.close()


if __name__ == "__main__":
    main()
