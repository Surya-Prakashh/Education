import json
import logging
from broker import Broker

# Configure logging to display exact exchange events and offsets
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(message)s",
    datefmt="%H:%M:%S",
)


def run_youtube_demo():
    print("=" * 75)
    print("      Pub/Sub Broker Demo: Consumer Backlog & Offline Replay")
    print("=" * 75)

    broker = Broker()

    with open("events_example.json", "r") as f:
        data = json.load(f)

    # -------------------------------------------------------------------------
    # SCENARIO 1: Standard Multi-Subscriber Real-Time Polling
    # -------------------------------------------------------------------------
    print("\n" + "-" * 75)
    print(" SCENARIO 1: Real-time Subscribers (Alice, Bob, Charlie)")
    print("-" * 75)

    topic_prog = "Programming"
    for consumer in ["Alice", "Bob", "Charlie"]:
        broker.subscribe(consumer, topic_prog)

    for video in data["topics"]["Programming"]:
        off = broker.publish_json(topic_prog, video)
        print(f"-> [PRODUCER] Uploaded '{video['title']}' to '{topic_prog}' @ Offset {off}")

    print("\n[Polling Scenario 1]")
    alice_batch = broker.poll_json("Alice", topic_prog, max_messages=2)
    print(f"   Alice polled 2 msgs: {[m['title'] for m in alice_batch]}")

    bob_batch = broker.poll_json("Bob", topic_prog, max_messages=3)
    print(f"   Bob polled 3 msgs: {[m['title'] for m in bob_batch]}")

    # -------------------------------------------------------------------------
    # SCENARIO 2: Consumer Offline / Unavailable while Publisher uploads videos
    # -------------------------------------------------------------------------
    print("\n" + "-" * 75)
    print(" SCENARIO 2: Offline Consumer Simulation (Producer publishes while consumer is offline)")
    print("-" * 75)

    topic_ds = "DataScience"

    # Step A: Publisher uploads 4 videos to DataScience topic
    # NO CONSUMER IS ONLINE OR POLLING YET!
    print("\n[PHASE 1: Consumer 'David' is OFFLINE. Producer uploads 4 videos...]")
    for video in data["topics"]["DataScience"]:
        off = broker.publish_json(topic_ds, video)
        print(f"-> [PRODUCER] Uploaded '{video['title']}' to '{topic_ds}' @ Offset {off}")

    print("\n>>> Status: Topic 'DataScience' has 4 accumulated messages in log.")
    print(">>> Status: Consumer 'David' was absent/offline during all uploads.\n")

    # Step B: David comes online later and subscribes
    print("[PHASE 2: Consumer 'David' COMES ONLINE and Subscribes...]")
    broker.subscribe("David", topic_ds)

    # Step C: David polls batch 1 (max 2 messages)
    print("\n[David Batch 1 (max_messages=2)]")
    david_batch_1 = broker.poll_json("David", topic_ds, max_messages=2)
    for m in david_batch_1:
        print(f"   David received backlog msg: '{m['title']}'")

    # Step D: David polls batch 2 (max 2 messages) to catch up completely
    print("\n[David Batch 2 (max_messages=2) - Catching up remaining backlog]")
    david_batch_2 = broker.poll_json("David", topic_ds, max_messages=2)
    for m in david_batch_2:
        print(f"   David received backlog msg: '{m['title']}'")

    # Step E: David goes offline again, 1 new video is published, David reconnects & resumes
    print("\n[PHASE 3: David goes offline again. Producer uploads 1 new video...]")
    new_video = {
        "id": "ds_205",
        "title": "AI Agents Architecture",
        "uploader": "DataWizard",
        "duration_minutes": 55,
        "tags": ["agents", "ai"]
    }
    new_off = broker.publish_json(topic_ds, new_video)
    print(f"-> [PRODUCER] Uploaded '{new_video['title']}' to '{topic_ds}' @ Offset {new_off}")

    print("\n[PHASE 4: David reconnects and polls for new messages]")
    david_batch_3 = broker.poll_json("David", topic_ds, max_messages=10)
    for m in david_batch_3:
        print(f"   David resumed and received: '{m['title']}'")

    print("\n" + "=" * 75)
    print("      Demo & Simulation Completed Successfully!")
    print("=" * 75)


if __name__ == "__main__":
    run_youtube_demo()
