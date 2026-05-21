"""A low-level HTTP server built directly on TCP sockets.

No http.server, no frameworks. Just a socket, the bytes that arrive on it,
and the bytes we write back. This is what HTTP actually is: text over TCP.
"""

import socket
from datetime import datetime

from http_utils import parse_request, recv_request_blocking

HOST = "127.0.0.1"  # http://localhost:
PORT = 8000


def log(message):
    """Print one timestamped line to the console."""
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} {message}")


def main():
    # AF_INET = IPv4, SOCK_STREAM = TCP. This is just a file descriptor so far.
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Without this, the port stays "in use" for a minute after the server stops,
    # and restarting immediately would fail with "address already in use".
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server.bind((HOST, PORT))   # claim the (host, port) pair
    server.listen(5)            # start queuing incoming connections; 5 = backlog size

    # Make accept() give up after 1 second instead of blocking forever. On
    # Windows a blocking socket call swallows Ctrl+C, so without this timeout
    # the server can't be stopped until a connection happens to arrive. The
    # timeout lets Python regain control once a second and notice the signal.
    server.settimeout(1.0)
    log(f"Listening on http://{HOST}:{PORT}")

    try:
        while True:
            # accept() blocks (up to 1s) until a client connects, then hands
            # back a NEW socket dedicated to that one client, plus its address.
            try:
                conn, addr = server.accept()
            except socket.timeout:
                continue  # no one connected this second; loop and check for Ctrl+C
            try:
                raw = recv_request_blocking(conn)
                if not raw:
                    continue  # client connected then left without sending anything

                method, path, headers, body = parse_request(raw)
                log(f"{method} {path} from {addr[0]} | "
                    f"headers={headers} | "
                    f"body={body.decode('utf-8', errors='replace')}")

                # Build the response by hand. Note the exact shape of HTTP:
                #   status line  +  headers  +  blank line  +  body
                # and every line is separated by CRLF (\r\n), not just \n.
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
            finally:
                conn.close()  # we said "Connection: close", so end this TCP connection
    except KeyboardInterrupt:
        log("Shutting down")
    finally:
        server.close()


if __name__ == "__main__":
    main()
