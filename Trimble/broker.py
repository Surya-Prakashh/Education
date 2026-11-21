from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import json
import logging
import time

logger = logging.getLogger("pubsub.broker")


class BrokerError(Exception):
    """Base exception for all Pub/Sub Broker errors."""
    pass


class TopicNotFoundError(BrokerError):
    """Raised when an operation targets a topic that does not exist."""
    pass


class NotSubscribedError(BrokerError):
    """Raised when a consumer attempts to poll a topic it has not subscribed to."""
    pass


@dataclass(frozen=True)
class Message:
    """Immutable message value object stored in a topic log."""
    topic: str
    offset: int
    payload: str
    timestamp: float = field(default_factory=time.time)

    def json_payload(self) -> dict:
        """Parses and returns payload as a JSON dictionary if possible."""
        try:
            return json.loads(self.payload)
        except (json.JSONDecodeError, TypeError):
            return {"raw": self.payload}


class Topic:
    """
    Encapsulates an ordered FIFO sequence of messages for a single topic.
    Guarded by its own lock for fine-grained concurrency.
    """
    def __init__(self, name: str):
        self.name = name
        self._messages: list[Message] = []
        import threading
        self._lock = threading.Lock()

    def append(self, payload: str) -> Message:
        """Appends a new message to the topic log and returns the Message object."""
        with self._lock:
            offset = len(self._messages)
            msg = Message(topic=self.name, offset=offset, payload=payload)
            self._messages.append(msg)
            logger.info(
                f"[EXCHANGE: PUBLISH] Topic='{self.name}' | Offset={offset} | PayloadPreview='{payload[:50]}'"
            )
            return msg

    def read_from(self, start_offset: int, max_messages: int) -> list[Message]:
        """Reads up to max_messages starting from start_offset."""
        if max_messages <= 0:
            raise ValueError("max_messages must be greater than 0")

        with self._lock:
            if start_offset >= len(self._messages):
                return []
            end_offset = min(start_offset + max_messages, len(self._messages))
            return self._messages[start_offset:end_offset]

    def size(self) -> int:
        """Returns the total number of messages in the topic."""
        with self._lock:
            return len(self._messages)


class OffsetStore(ABC):
    """Abstract interface for managing consumer subscription cursors."""

    @abstractmethod
    def subscribe(self, consumer_id: str, topic_name: str) -> None:
        pass

    @abstractmethod
    def is_subscribed(self, consumer_id: str, topic_name: str) -> bool:
        pass

    @abstractmethod
    def get_offset(self, consumer_id: str, topic_name: str) -> int:
        pass

    @abstractmethod
    def advance(self, consumer_id: str, topic_name: str, new_offset: int) -> None:
        pass


class InMemoryOffsetStore(OffsetStore):
    """In-memory thread-safe implementation of OffsetStore."""

    def __init__(self):
        self._offsets: dict[tuple[str, str], int] = {}
        import threading
        self._lock = threading.Lock()

    def subscribe(self, consumer_id: str, topic_name: str) -> None:
        with self._lock:
            key = (consumer_id, topic_name)
            if key not in self._offsets:
                self._offsets[key] = 0
                logger.info(f"[SUBSCRIPTION] Consumer='{consumer_id}' subscribed to Topic='{topic_name}' at Offset=0")

    def is_subscribed(self, consumer_id: str, topic_name: str) -> bool:
        with self._lock:
            return (consumer_id, topic_name) in self._offsets

    def get_offset(self, consumer_id: str, topic_name: str) -> int:
        with self._lock:
            key = (consumer_id, topic_name)
            if key not in self._offsets:
                raise NotSubscribedError(f"Consumer '{consumer_id}' is not subscribed to topic '{topic_name}'")
            return self._offsets[key]

    def advance(self, consumer_id: str, topic_name: str, new_offset: int) -> None:
        with self._lock:
            key = (consumer_id, topic_name)
            if key not in self._offsets:
                raise NotSubscribedError(f"Consumer '{consumer_id}' is not subscribed to topic '{topic_name}'")
            old_offset = self._offsets[key]
            self._offsets[key] = new_offset
            logger.info(
                f"[OFFSET ADVANCE] Consumer='{consumer_id}' | Topic='{topic_name}' | Offset: {old_offset} -> {new_offset}"
            )


class Broker:
    """
    Facade orchestrator coordinating topics and consumer offset tracking.
    Thread-safe against concurrent publish and poll invocations.
    """
    def __init__(self, offset_store: OffsetStore | None = None):
        self._topics: dict[str, Topic] = {}
        import threading
        self._topics_lock = threading.Lock()
        self._offset_store = offset_store or InMemoryOffsetStore()

    def _get_or_create_topic(self, topic_name: str) -> Topic:
        """Retrieves an existing topic or auto-creates it safely using double-checked locking."""
        if topic_name in self._topics:
            return self._topics[topic_name]

        with self._topics_lock:
            if topic_name not in self._topics:
                self._topics[topic_name] = Topic(topic_name)
                logger.info(f"[TOPIC CREATED] Topic='{topic_name}'")
            return self._topics[topic_name]

    def subscribe(self, consumer_id: str, topic_name: str) -> None:
        """Subscribes a consumer to a topic. Auto-creates topic if it doesn't exist."""
        self._get_or_create_topic(topic_name)
        self._offset_store.subscribe(consumer_id, topic_name)

    def publish(self, topic_name: str, payload: str) -> int:
        """Publishes a string payload to a topic and returns the assigned offset."""
        topic = self._get_or_create_topic(topic_name)
        msg = topic.append(payload)
        return msg.offset

    def publish_json(self, topic_name: str, data: dict) -> int:
        """Convenience method to publish a dictionary serialized as JSON."""
        return self.publish(topic_name, json.dumps(data))

    def poll(self, consumer_id: str, topic_name: str, max_messages: int = 10) -> list[Message]:
        """
        Polls up to max_messages for a subscribed consumer on a topic.
        Advances consumer read offset upon returning messages.
        """
        if topic_name not in self._topics:
            if not self._offset_store.is_subscribed(consumer_id, topic_name):
                raise NotSubscribedError(f"Consumer '{consumer_id}' is not subscribed to topic '{topic_name}'")
            raise TopicNotFoundError(f"Topic '{topic_name}' does not exist")

        if not self._offset_store.is_subscribed(consumer_id, topic_name):
            raise NotSubscribedError(f"Consumer '{consumer_id}' is not subscribed to topic '{topic_name}'")

        topic = self._topics[topic_name]
        current_offset = self._offset_store.get_offset(consumer_id, topic_name)
        messages = topic.read_from(current_offset, max_messages)

        if messages:
            new_offset = current_offset + len(messages)
            self._offset_store.advance(consumer_id, topic_name, new_offset)
            logger.info(
                f"[EXCHANGE: POLL] Consumer='{consumer_id}' | Topic='{topic_name}' | "
                f"Fetched={len(messages)} msg(s) | Offsets [{messages[0].offset}..{messages[-1].offset}]"
            )
        else:
            logger.debug(
                f"[EXCHANGE: POLL EMPTY] Consumer='{consumer_id}' | Topic='{topic_name}' | Offset={current_offset} (Caught up)"
            )

        return messages

    def poll_json(self, consumer_id: str, topic_name: str, max_messages: int = 10) -> list[dict]:
        """Convenience method polling messages and returning parsed JSON dictionaries."""
        messages = self.poll(consumer_id, topic_name, max_messages)
        return [msg.json_payload() for msg in messages]
