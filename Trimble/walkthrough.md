# Walkthrough - Python In-Memory Pub/Sub Broker

Implemented a thread-safe in-memory Publish-Subscribe (Pub/Sub) message broker in Python with JSON support, fine-grained logging of message exchanges and topic offsets, and comprehensive unit tests.

## Changes Made

### Core Components

- **[broker.py](file:///d:/Trimble/broker.py)**:
  - `Message`: Value object encapsulating `topic`, `offset`, `payload`, `timestamp`, and `json_payload()` parsing.
  - `Topic`: Message sequence log guarded by fine-grained `threading.Lock`. Logs `[EXCHANGE: PUBLISH]` with topic name and assigned sequential offset.
  - `OffsetStore` & `InMemoryOffsetStore`: Manages subscriber read positions (`consumer_id, topic`) and logs `[OFFSET ADVANCE]` on polling.
  - `Broker`: Facade providing `subscribe()`, `publish()`, `poll()`, `publish_json()`, `poll_json()`. Handles auto-topic creation using double-checked locking and exchange logging.
  - Custom exceptions: `BrokerError`, `TopicNotFoundError`, `NotSubscribedError`.

### Scenarios & Examples

- **[events_example.json](file:///d:/Trimble/events_example.json)**:
  - Contains YouTube video upload events for the `"Programming"` topic (`"Python Tutorial"`, `"Java Tutorial"`, `"C++ Tutorial"`).

- **[demo_runner.py](file:///d:/Trimble/demo_runner.py)**:
  - Configures standard Python `logging` to format exchange events with timestamps.
  - Loads JSON event data, registers consumers (`Alice`, `Bob`, `Charlie`), publishes messages, and logs independent offset consumption.

### Test Suite

- **[test_broker.py](file:///d:/Trimble/test_broker.py)**:
  - Contains 11 unit tests covering sequential offsets, FIFO order, independent polling, unsubscribed errors, JSON serialization, multi-threaded concurrency (500 concurrent messages across 8 threads), and logging assertions.

---

## Verification Results

### Automated Test Suite (`pytest`)
Ran command: `python -m pytest test_broker.py -v`

Output:
```txt
test_broker.py::test_publish_returns_sequential_offsets PASSED           [  9%]
test_broker.py::test_subscribe_then_poll_returns_messages_in_order PASSED [ 18%]
test_broker.py::test_poll_respects_max_messages_and_advances_cursor PASSED [ 27%]
test_broker.py::test_polling_after_reading_everything_returns_empty_not_error PASSED [ 36%]
test_broker.py::test_two_consumers_on_same_topic_read_independently PASSED [ 45%]
test_broker.py::test_unsubscribed_consumer_raises_not_subscribed_error PASSED [ 54%]
test_broker.py::test_poll_non_existent_topic_unsubscribed_raises_error PASSED [ 63%]
test_broker.py::test_invalid_max_messages_raises_value_error PASSED      [ 72%]
test_broker.py::test_publish_and_poll_json PASSED                        [ 81%]
test_broker.py::test_concurrent_publishing_and_polling PASSED            [ 90%]
test_broker.py::test_exchange_logging PASSED                             [100%]

============================= 11 passed in 0.04s ==============================
```

### Manual Demo Execution (`demo_runner.py`)
Ran command: `python demo_runner.py`

Output snippet showing structured logger for exchanges and offsets:
```txt
10:09:37 | INFO  | [TOPIC CREATED] Topic='Programming'
10:09:37 | INFO  | [SUBSCRIPTION] Consumer='Alice' subscribed to Topic='Programming' at Offset=0
10:09:37 | INFO  | [SUBSCRIPTION] Consumer='Bob' subscribed to Topic='Programming' at Offset=0
10:09:37 | INFO  | [SUBSCRIPTION] Consumer='Charlie' subscribed to Topic='Programming' at Offset=0
10:09:37 | INFO  | [EXCHANGE: PUBLISH] Topic='Programming' | Offset=0 | PayloadPreview='{"id": "video_101", "title": "Python Tutorial", "u'
10:09:37 | INFO  | [EXCHANGE: PUBLISH] Topic='Programming' | Offset=1 | PayloadPreview='{"id": "video_102", "title": "Java Tutorial", "upl'
10:09:37 | INFO  | [EXCHANGE: PUBLISH] Topic='Programming' | Offset=2 | PayloadPreview='{"id": "video_103", "title": "C++ Tutorial", "uplo'
10:09:37 | INFO  | [OFFSET ADVANCE] Consumer='Alice' | Topic='Programming' | Offset: 0 -> 2
10:09:37 | INFO  | [EXCHANGE: POLL] Consumer='Alice' | Topic='Programming' | Fetched=2 msg(s) | Offsets [0..1]
10:09:37 | INFO  | [OFFSET ADVANCE] Consumer='Bob' | Topic='Programming' | Offset: 0 -> 3
10:09:37 | INFO  | [EXCHANGE: POLL] Consumer='Bob' | Topic='Programming' | Fetched=3 msg(s) | Offsets [0..2]
```
