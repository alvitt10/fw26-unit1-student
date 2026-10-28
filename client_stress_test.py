"""Test 20 concurrent clients against the TCP server."""
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from protocol import send_message, recv_message

HOST = "127.0.0.1"
PORT = 9090
CLIENTS = 20


def request(message):
    with socket.create_connection((HOST, PORT), timeout=5) as sock:
        sock.settimeout(5)
        send_message(sock, message)
        return recv_message(sock)


def worker(number):
    # Unique keys and values prevent workers overwriting each other's tests.
    key = f"worker_{number}"
    value = f"value_{number}"
    saved = request({"action": "PUT", "key": key, "value": value})
    loaded = request({"action": "GET", "key": key})
    if saved.get("status") != "ok" or loaded.get("value") != value:
        raise AssertionError(f"Worker {number} received incorrect data")
    return number


def main():
    print(f"[*] Launching {CLIENTS} concurrent client worker threads...")
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=CLIENTS) as pool:
        # list() waits for every worker and propagates any worker exception.
        results = list(pool.map(worker, range(CLIENTS)))
    for number in results:
        print(f"[Worker Thread {number:02d}] Transaction verified successfully.")
    elapsed = time.perf_counter() - start
    print(f"[+] All {len(results)} workers completed execution in {elapsed:.3f} seconds.")


if __name__ == "__main__":
    main()
