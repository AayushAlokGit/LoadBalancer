"""A client that connects but never sends a single byte.

This demonstrates the failure mode discussed earlier: at the TCP level,
connecting and sending data are two separate steps. This client does the
first and deliberately skips the second.

What happens on the server side:
  - The TCP handshake completes, so the server's accept() returns and it
    spawns a worker thread.
  - That thread calls recv_request_blocking() -> conn.recv(), which BLOCKS forever
    because no bytes are ever coming.
  - The server logs nothing for this connection (the log line is after
    recv_request_blocking, which never returns) and the thread is stuck.

Run threaded_server.py first, then run this. Each connection here pins one
server worker thread. Press Ctrl+C here to release them.

Run:  python silent_client.py
"""

import socket
import time

HOST = "127.0.0.1"
PORT = 8000          # the server; point at 9000 to stall the load balancer instead
NUM_CONNECTIONS = 3  # each one pins a separate server worker thread
# Must stay comfortably ABOVE the server's CLIENT_TIMEOUT (10s) so the
# server's recv() deadline fires first. If this drops to <= the server
# timeout, the client closes the socket first and the server sees a clean
# disconnect (recv returns b"") instead of raising socket.timeout.
HOLD_SECONDS = 10


def main():
    sockets = []
    for n in range(1, NUM_CONNECTIONS + 1):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((HOST, PORT))   # handshake completes -> server spawns a thread
        # Note what is NOT here: no s.send(), no s.sendall(). Total silence.
        sockets.append(s)
        print(f"connection {n}: connected to {HOST}:{PORT}, sending nothing")

    print(f"\n{NUM_CONNECTIONS} silent connections open. "
          f"Each is pinning one server worker thread.")
    print(f"Holding for {HOLD_SECONDS}s. Press Ctrl+C to close them early.")

    try:
        time.sleep(HOLD_SECONDS)
    except KeyboardInterrupt:
        print("\nclosing connections")
    finally:
        for s in sockets:
            s.close()


if __name__ == "__main__":
    main()
