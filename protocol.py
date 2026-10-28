"""Four-byte big-endian length-prefixed JSON messages."""
import json
import struct

MAX_MESSAGE_SIZE = 1024 * 1024  # Reject messages larger than 1 MiB.


def recv_exact(sock, count):
    """Read exactly count bytes, even when TCP splits the data."""
    data = bytearray()
    while len(data) < count:
        chunk = sock.recv(count - len(data))
        if not chunk:
            raise ConnectionError("Connection closed before message was complete")
        data.extend(chunk)
    return bytes(data)


def send_message(sock, message):
    """Send a JSON object with its four-byte length header."""
    payload = json.dumps(message).encode("utf-8")
    if len(payload) > MAX_MESSAGE_SIZE:
        raise ValueError("Message is too large")
    header = struct.pack("!I", len(payload))
    sock.sendall(header + payload)


def recv_message(sock):
    """Receive and decode one complete length-prefixed JSON object."""
    header = recv_exact(sock, 4)
    length = struct.unpack("!I", header)[0]
    if length > MAX_MESSAGE_SIZE:
        raise ValueError("Message is too large")
    payload = recv_exact(sock, length)
    message = json.loads(payload.decode("utf-8"))
    if not isinstance(message, dict):
        raise ValueError("JSON message must be an object")
    return message
