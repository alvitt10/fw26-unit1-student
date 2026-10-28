"""Concurrent TCP server for key-value and task queue requests."""
import socket
from concurrent.futures import ThreadPoolExecutor
from protocol import recv_message, send_message
from storage import Storage

HOST = "0.0.0.0"
PORT = 9090
WORKERS = 10
store = Storage()


def process_request(request):
    action = request.get("action")
    if action == "PUT":
        if "key" not in request or "value" not in request:
            return {"status": "error", "message": "PUT requires key and value"}
        store.put(str(request["key"]), request["value"])
        return {"status": "ok"}
    if action == "GET":
        if "key" not in request:
            return {"status": "error", "message": "GET requires key"}
        key = str(request["key"])
        value = store.get(key)
        return {"status": "ok", "value": value}
    if action == "ENQUEUE":
        if "task" not in request:
            return {"status": "error", "message": "ENQUEUE requires task"}
        store.enqueue(request["task"])
        return {"status": "ok"}
    if action == "DEQUEUE":
        task = store.dequeue()
        return {"status": "ok", "task": task}
    return {"status": "error", "message": "Unknown action"}


def handle_client(client):
    with client:
        client.settimeout(5)
        try:
            # One request per connection keeps the worker pool bounded.
            request = recv_message(client)
            response = process_request(request)
            send_message(client, response)
        except (ConnectionError, OSError, ValueError, UnicodeError) as error:
            print("[!] Client connection issue:", error)


def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT))
        server.listen()
        print(f"[*] Server initialized on {HOST}:{PORT}", flush=True)
        print(f"[*] Thread Pool initialized with max_workers={WORKERS}...", flush=True)
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            while True:
                client, _address = server.accept()
                pool.submit(handle_client, client)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[*] Server stopped")
