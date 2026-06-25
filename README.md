Thread-Safe Producer-Consumer Queue
A small Python project demonstrating multi-threaded synchronization using the classic Producer-Consumer pattern — a fundamental concurrency problem in systems engineering.
What it does
Producer threads generate tasks and add them to a shared queue.
Consumer threads pull tasks off the queue and process them.
Multiple producers and consumers run concurrently, and the queue safely coordinates access between them so no task is lost, duplicated, or corrupted.
Why this is a synchronization problem
If multiple threads read and write a shared data structure (like a list) at the same time without coordination, you get a race condition — for example, two consumer threads could read the same task and process it twice, or a producer and consumer could corrupt the queue's internal state mid-update.
This project solves that using Python's built-in `queue.Queue`, which is internally protected by a lock and condition variable, so:
`put()` and `get()` are safe to call from any thread at the same time.
`put()` automatically blocks if the queue is full (backpressure), so producers can't overwhelm consumers.
`get()` automatically blocks if the queue is empty, so consumers wait efficiently instead of busy-looping.
A separate `threading.Lock` is also used to keep console output readable when multiple threads print at once, and another lock protects a shared counter (`processed_count`) that all consumer threads update.
How to run
```bash
python producer_consumer.py
```
You'll see interleaved output from multiple producer and consumer threads, followed by a final summary confirming all tasks were processed exactly once.
Key concepts demonstrated
Concept	Where it shows up
Thread creation & management	`threading.Thread`, `.start()`, `.join()`
Mutual exclusion (locks)	`counter_lock`, `print_lock`
Blocking queue / condition variables	`queue.Queue(maxsize=...)`
Producer-consumer coordination	Sentinel value used to signal shutdown
Backpressure	Bounded queue size limits memory growth
Possible extensions
Swap `queue.Queue` for a custom implementation using `threading.Condition` to show lower-level synchronization primitives.
Add multiprocessing version using `multiprocessing.Queue` to compare process-based vs thread-based concurrency.
Add unit tests verifying no tasks are lost or duplicated under high thread counts.
Tech
Python 3, standard library only (`threading`, `queue`) — no external dependencies.
