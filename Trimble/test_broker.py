from concurrent.futures import ThreadPoolExecutor
import logging
import pytest
from broker import Broker, NotSubscribedError, TopicNotFoundError


def test_publish_returns_sequential_offsets():
    broker = Broker()
    off0 = broker.publish("Programming", "Python Tutorial")
    off1 = broker.publish("Programming", "Java Tutorial")
    off2 = broker.publish("Programming", "C++ Tutorial")

    assert off0 == 0
    assert off1 == 1
    assert off2 == 2


def test_subscribe_then_poll_returns_messages_in_order():
    broker = Broker()
    broker.subscribe("Alice", "Programming")
    broker.publish("Programming", "Python Tutorial")
    broker.publish("Programming", "Java Tutorial")

    msgs = broker.poll("Alice", "Programming", max_messages=10)
    assert len(msgs) == 2
    assert msgs[0].payload == "Python Tutorial"
    assert msgs[0].offset == 0
    assert msgs[1].payload == "Java Tutorial"
    assert msgs[1].offset == 1


def test_poll_respects_max_messages_and_advances_cursor():
    broker = Broker()
    broker.subscribe("Alice", "Programming")
    broker.publish("Programming", "Python Tutorial")
    broker.publish("Programming", "Java Tutorial")
    broker.publish("Programming", "C++ Tutorial")

    msgs1 = broker.poll("Alice", "Programming", max_messages=2)
    assert len(msgs1) == 2
    assert [m.offset for m in msgs1] == [0, 1]

    msgs2 = broker.poll("Alice", "Programming", max_messages=2)
    assert len(msgs2) == 1
    assert msgs2[0].offset == 2


def test_polling_after_reading_everything_returns_empty_not_error():
    broker = Broker()
    broker.subscribe("Alice", "Programming")
    broker.publish("Programming", "Python Tutorial")

    msgs1 = broker.poll("Alice", "Programming", max_messages=5)
    assert len(msgs1) == 1

    msgs2 = broker.poll("Alice", "Programming", max_messages=5)
    assert msgs2 == []


def test_two_consumers_on_same_topic_read_independently():
    broker = Broker()
    broker.subscribe("Alice", "Programming")
    broker.subscribe("Bob", "Programming")

    broker.publish("Programming", "Python Tutorial")
    broker.publish("Programming", "Java Tutorial")

    alice_msgs = broker.poll("Alice", "Programming", max_messages=1)
    assert len(alice_msgs) == 1
    assert alice_msgs[0].payload == "Python Tutorial"

    bob_msgs = broker.poll("Bob", "Programming", max_messages=5)
    assert len(bob_msgs) == 2
    assert bob_msgs[0].payload == "Python Tutorial"
    assert bob_msgs[1].payload == "Java Tutorial"


def test_unsubscribed_consumer_raises_not_subscribed_error():
    broker = Broker()
    broker.publish("Programming", "Python Tutorial")

    with pytest.raises(NotSubscribedError):
        broker.poll("UnknownConsumer", "Programming")


def test_poll_non_existent_topic_unsubscribed_raises_error():
    broker = Broker()
    with pytest.raises(NotSubscribedError):
        broker.poll("Alice", "NonExistentTopic")


def test_invalid_max_messages_raises_value_error():
    broker = Broker()
    broker.subscribe("Alice", "Programming")
    broker.publish("Programming", "Python Tutorial")

    with pytest.raises(ValueError):
        broker.poll("Alice", "Programming", max_messages=0)


def test_publish_and_poll_json():
    broker = Broker()
    broker.subscribe("Alice", "Programming")

    video = {"id": "v1", "title": "Python Tutorial", "category": "Python"}
    off = broker.publish_json("Programming", video)
    assert off == 0

    json_msgs = broker.poll_json("Alice", "Programming", max_messages=5)
    assert len(json_msgs) == 1
    assert json_msgs[0]["title"] == "Python Tutorial"
    assert json_msgs[0]["category"] == "Python"


def test_concurrent_publishing_and_polling():
    broker = Broker()
    topic = "ConcurrencyTopic"
    num_publishers = 5
    msgs_per_pub = 100

    broker.subscribe("ConcurrentConsumer", topic)

    def publish_worker(pub_id):
        for i in range(msgs_per_pub):
            broker.publish(topic, f"pub-{pub_id}-msg-{i}")

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(publish_worker, i) for i in range(num_publishers)]
        for f in futures:
            f.result()

    polled = []
    while len(polled) < num_publishers * msgs_per_pub:
        batch = broker.poll("ConcurrentConsumer", topic, max_messages=50)
        polled.extend(batch)

    offsets = [m.offset for m in polled]
    assert offsets == list(range(num_publishers * msgs_per_pub))


def test_exchange_logging(caplog):
    broker = Broker()
    with caplog.at_level(logging.INFO):
        broker.subscribe("Alice", "Programming")
        broker.publish("Programming", "Python Tutorial")
        broker.poll("Alice", "Programming", max_messages=1)

    logs = caplog.text
    assert "[SUBSCRIPTION]" in logs
    assert "[EXCHANGE: PUBLISH]" in logs
    assert "[EXCHANGE: POLL]" in logs
    assert "Topic='Programming'" in logs
    assert "Offset=0" in logs


def test_offline_consumer_retains_backlog_and_catches_up():
    """Verifies topic retains all messages while consumer is offline, and consumer catches up fully."""
    broker = Broker()
    topic = "DataScience"

    # Publisher uploads 5 messages while consumer David is completely absent
    for i in range(5):
        broker.publish(topic, f"video-{i}")

    # David comes online later and subscribes
    broker.subscribe("David", topic)

    # David polls in batches of 2
    batch1 = broker.poll("David", topic, max_messages=2)
    assert [m.payload for m in batch1] == ["video-0", "video-1"]
    assert [m.offset for m in batch1] == [0, 1]

    batch2 = broker.poll("David", topic, max_messages=2)
    assert [m.payload for m in batch2] == ["video-2", "video-3"]
    assert [m.offset for m in batch2] == [2, 3]

    batch3 = broker.poll("David", topic, max_messages=2)
    assert [m.payload for m in batch3] == ["video-4"]
    assert [m.offset for m in batch3] == [4]


def test_consumer_disconnect_reconnect_resumes_from_last_offset():
    """Verifies that a consumer can read part of the topic, pause (disconnect), and resume cleanly."""
    broker = Broker()
    topic = "IoTData"

    broker.subscribe("SensorConsumer", topic)
    broker.publish(topic, "msg-0")
    broker.publish(topic, "msg-1")

    # Poll first message
    m1 = broker.poll("SensorConsumer", topic, max_messages=1)
    assert m1[0].offset == 0

    # Producer publishes msg-2 while consumer is offline
    broker.publish(topic, "msg-2")

    # Consumer reconnects and polls remaining
    remaining = broker.poll("SensorConsumer", topic, max_messages=10)
    assert len(remaining) == 2
    assert [m.offset for m in remaining] == [1, 2]
    assert [m.payload for m in remaining] == ["msg-1", "msg-2"]


def test_concurrent_subscriptions_and_polling_by_many_consumers():
    """
    Verifies that multiple consumers subscribing and polling concurrently on the same topic
    each receive the full ordered sequence of messages without race conditions.
    """
    broker = Broker()
    topic = "ConcurrentConsumersTopic"
    total_messages = 100
    num_consumers = 10

    # Publish 100 messages
    for i in range(total_messages):
        broker.publish(topic, f"msg-{i}")

    # Worker function for each consumer running in parallel threads
    def consumer_worker(consumer_id: str) -> list[int]:
        broker.subscribe(consumer_id, topic)
        read_offsets = []
        while len(read_offsets) < total_messages:
            batch = broker.poll(consumer_id, topic, max_messages=15)
            read_offsets.extend([m.offset for m in batch])
        return read_offsets

    # Run 10 consumers concurrently across 10 threads
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(consumer_worker, f"Consumer_{c}")
            for c in range(num_consumers)
        ]
        results = [f.result() for f in futures]

    # Verify that EVERY consumer got all 100 messages in exact order [0..99]
    expected_offsets = list(range(total_messages))
    for c_idx, offsets in enumerate(results):
        assert offsets == expected_offsets, f"Consumer_{c_idx} offsets did not match expected history!"
