def recv_request_blocking(conn):
    """Read one full HTTP request off the connection and return raw bytes.

    TCP is a stream, not a message queue: a single recv() may return half a
    request, or a whole one, or one-and-a-half. So we loop until we've seen
    the blank line (\\r\\n\\r\\n) that ends the headers, then keep reading
    until we've collected as many body bytes as Content-Length promised.
    """
    data = b""

    # Phase 1: read until the end-of-headers marker appears.
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(4096)      # grab up to 4096 bytes; blocks until some arrive
        if not chunk:                # empty bytes == the client closed the connection
            return data
        data += chunk

    # Phase 2: figure out how long the body is, then read exactly that much.
    head, _, body = data.partition(b"\r\n\r\n")
    content_length = 0
    for line in head.split(b"\r\n")[1:]:          # skip the request line, scan headers
        name, _, value = line.partition(b":")
        if name.strip().lower() == b"content-length":
            content_length = int(value.strip())

    while len(body) < content_length:
        chunk = conn.recv(4096)
        if not chunk:
            break
        body += chunk

    return head + b"\r\n\r\n" + body


def parse_request(raw):
    """Split raw request bytes into (method, path, headers dict, body)."""
    head, _, body = raw.partition(b"\r\n\r\n")
    lines = head.split(b"\r\n")

    # The very first line is the "request line", e.g.  GET /hello HTTP/1.1
    request_line = lines[0].decode("utf-8", errors="replace")
    parts = request_line.split(" ")
    method = parts[0] if len(parts) > 0 else ""
    path = parts[1] if len(parts) > 1 else ""

    # Every line after that, up to the blank line, is "Name: value".
    headers = {}
    for line in lines[1:]:
        name, sep, value = line.partition(b":")
        if sep:
            headers[name.strip().decode()] = value.strip().decode()

    return method, path, headers, body


async def recv_request_non_blocking(loop, conn):
    """Async twin of recv_request_blocking.

    Identical logic — read until the end-of-headers marker, then read exactly
    Content-Length body bytes — but every conn.recv() is now an awaited
    loop.sock_recv(). The blocking version in http_utils can't be reused here:
    a blocking recv() would freeze the entire event loop, not just this
    coroutine.
    """
    data = b""

    # Phase 1: read until the end-of-headers marker appears.
    while b"\r\n\r\n" not in data:
        chunk = await loop.sock_recv(conn, 4096)
        if not chunk:                # client closed the connection
            return data
        data += chunk

    # Phase 2: figure out how long the body is, then read exactly that much.
    head, _, body = data.partition(b"\r\n\r\n")
    content_length = 0
    for line in head.split(b"\r\n")[1:]:
        name, _, value = line.partition(b":")
        if name.strip().lower() == b"content-length":
            content_length = int(value.strip())

    while len(body) < content_length:
        chunk = await loop.sock_recv(conn, 4096)
        if not chunk:
            break
        body += chunk

    return head + b"\r\n\r\n" + body

