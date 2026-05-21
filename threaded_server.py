"""A low-level HTTP server that handles each client in its own thread.

Same raw-socket approach as single_threaded_server.py, with one change: the
accept loop no longer handles a client itself. It spawns a thread per
connection and immediately loops back to accept the next one. Slow clients
no longer block everyone behind them.
"""

import socket
import threading
from datetime import datetime

from http_utils import parse_request, recv_request

HOST = "127.0.0.1"  # http://localhost:
PORT = 8000

# print() from many threads at once can interleave mid-line, producing
# garbled output. This lock makes each log line atomic.
log_lock = threading.Lock()


def log(message):
    """Print one timestamped line to the console, prefixed with the thread name."""
    thread_name = threading.current_thread().name
    with log_lock:
        print(f"{datetime.now():%Y-%m-%d %H:%M:%S} [{thread_name}] {message}")


def handle_client(conn, addr):
    """Serve one client from start to finish. This runs inside its own thread."""
    try:
        raw = recv_request(conn)
        if not raw:
            return  # client connected then left without sending anything

        method, path, headers, body = parse_request(raw)
        log(f"{method} {path} from {addr[0]} | "
            f"headers={headers} | "
            f"body={body.decode('utf-8', errors='replace')}")        

        # Build the response by hand: status line + headers + blank line + body.
        response_body = b"OK\n"
        response = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain\r\n"
            b"Content-Length: " + str(len(response_body)).encode() + b"\r\n"
            b"Connection: close\r\n"
            b"\r\n"
            + response_body
        )
        conn.sendall(response)
    except Exception as exc:
        # One thread crashing must not take down the server. Log and move on.
        log(f"error handling {addr[0]}: {exc!r}")
    finally:
        conn.close()  # we said "Connection: close", so end this TCP connection


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(128)          # bigger backlog: bursts are now drained fast

    # accept() gives up after 1s so Ctrl+C is noticed promptly on Windows.
    server.settimeout(1.0)
    log(f"Listening on http://{HOST}:{PORT}")

    try:
        while True:
            try:
                conn, addr = server.accept()
            except socket.timeout:
                continue  # no one connected this second; loop and check for Ctrl+C

            # Hand this client to a worker thread, then immediately loop back
            # to accept the next one. daemon=True means the thread won't keep
            # the process alive after main() exits.
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
