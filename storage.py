"""Thread-safe key-value storage and a condition-backed FIFO task queue."""
import threading
from collections import deque


class Storage:
    def __init__(self):
        self.values = {}
        self.lock = threading.Lock()
        self.tasks = deque()
        self.condition = threading.Condition(threading.Lock())

    def put(self, key, value):
        with self.lock:
            self.values[key] = value

    def get(self, key):
        with self.lock:
            return self.values.get(key)

    def enqueue(self, task):
        with self.condition:
            self.tasks.append(task)
            self.condition.notify()

    def dequeue(self, timeout=1.0):
        # Wait for an item, but do not block a server worker forever.
        with self.condition:
            available = self.condition.wait_for(lambda: bool(self.tasks), timeout)
            if not available:
                return None
            return self.tasks.popleft()
