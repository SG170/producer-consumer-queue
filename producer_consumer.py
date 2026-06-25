"""
Thread-Safe Producer-Consumer Queue

"""

import threading
import queue
import time
import random


class TaskQueueSystem:
    def __init__(self, num_producers=2, num_consumers=3, tasks_per_producer=5, max_queue_size=5):
        self.num_producers = num_producers
        self.num_consumers = num_consumers
        self.tasks_per_producer = tasks_per_producer

        # Bounded queue: thread-safe by design.
        # max_queue_size limits memory use and forces producers to wait
        # if consumers fall behind (this is "backpressure").
        self.task_queue = queue.Queue(maxsize=max_queue_size)

        # Lock used only for printing cleanly from multiple threads.
        self.print_lock = threading.Lock()

        # Tracks how many tasks have been processed (shared counter).
        self.processed_count = 0
        self.counter_lock = threading.Lock()

        # Sentinel value to tell consumers "no more work is coming".
        self.SENTINEL = None

    def safe_print(self, message):
        with self.print_lock:
            print(message)

    def producer(self, producer_id):
        """Generates tasks and puts them on the shared queue."""
        for i in range(self.tasks_per_producer):
            task = f"Producer-{producer_id}-Task-{i}"
            # put() blocks automatically if the queue is full,
            # which is the synchronization happening for us.
            self.task_queue.put(task)
            self.safe_print(f"[Producer {producer_id}] produced: {task}")
            time.sleep(random.uniform(0.05, 0.2))  # simulate variable work

        self.safe_print(f"[Producer {producer_id}] finished producing.")

    def consumer(self, consumer_id):
        """Pulls tasks off the shared queue and processes them."""
        while True:
            task = self.task_queue.get()  # blocks if queue is empty

            if task is self.SENTINEL:
                # Put the sentinel back so other consumers also see it,
                # then stop.
                self.task_queue.put(self.SENTINEL)
                break

            self.safe_print(f"  [Consumer {consumer_id}] processing: {task}")
            time.sleep(random.uniform(0.1, 0.3))  # simulate processing time

            with self.counter_lock:
                self.processed_count += 1

            self.task_queue.task_done()

        self.safe_print(f"  [Consumer {consumer_id}] shutting down.")

    def run(self):
        producers = [
            threading.Thread(target=self.producer, args=(i,))
            for i in range(self.num_producers)
        ]
        consumers = [
            threading.Thread(target=self.consumer, args=(i,))
            for i in range(self.num_consumers)
        ]

        for t in producers + consumers:
            t.start()

        # Wait for all producers to finish generating tasks.
        for t in producers:
            t.join()

        # Signal consumers that no more tasks are coming.
        self.task_queue.put(self.SENTINEL)

        for t in consumers:
            t.join()

        total_expected = self.num_producers * self.tasks_per_producer
        self.safe_print(
            f"\nDone. Processed {self.processed_count}/{total_expected} tasks."
        )


if __name__ == "__main__":
    system = TaskQueueSystem(
        num_producers=2,
        num_consumers=3,
        tasks_per_producer=5,
        max_queue_size=5,
    )
    system.run()
