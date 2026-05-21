"""Fire multiple concurrent requests at the server to exercise its threading.

Each request runs in its own thread, so they hit the server at the same time
rather than one after another. Watch the server's console: you should see
several worker threads logging at once.

Run:  python client.py
"""

import http.client
import threading
import time

HOST = "127.0.0.1"
PORT = 8000
NUM_REQUESTS = 10


def send_request(n):
    """Send one HTTP request and print the status and response body."""
    conn = http.client.HTTPConnection(HOST, PORT)  # one TCP socket to the server
    try:
        conn.request("GET", f"/req-{n}")           # send the request line + headers
        response = conn.getresponse()              # block until the server replies
        body = response.read().decode().strip()
        print(f"request {n:>2}: {response.status} {response.reason} -> {body}")
    except Exception as exc:
        print(f"request {n:>2}: failed -> {exc!r}")
    finally:
        conn.close()


def main():
    start = time.time()

    # One thread per request: start them all, then wait for all to finish.
    threads = [
        threading.Thread(target=send_request, args=(n,))
        for n in range(1, NUM_REQUESTS + 1)
    ]
    for t in threads:
        t.start()       # fire the request
    for t in threads:
        t.join()        # block until this thread has finished

    print(f"\n{NUM_REQUESTS} requests completed in {time.time() - start:.3f}s")


if __name__ == "__main__":
    main()
