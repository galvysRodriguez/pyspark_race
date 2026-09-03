import json
import random
import time
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

KAFKA_TOPIC = "race_events"
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'race-sensor-simulator'
}

CHECKPOINTS = [
    {"id": "START", "distance": 0.0},
    {"id": "KM_2.5", "distance": 2.5},
    {"id": "KM_5.0", "distance": 5.0},
    {"id": "KM_7.5", "distance": 7.5},
    {"id": "KM_9.0", "distance": 9.0},
    {"id": "FINISH", "distance": 10.0},
]

NUM_RUNNERS = 1000

def generate_runner_roster(num_runners):
    runners = []
    for bib in range(1001, 1001 + num_runners):
        runners.append({
            "bib_number": bib,
            "runner_name": fake.name(),
            "age_group": random.choice(["18-29", "30-39", "40-49", "50+"]),
            "gender": random.choice(["M", "F"]),
            "base_pace": random.uniform(3.5, 8.0),  # min/km pace
            "current_checkpoint_idx": 0,
            "elapsed_seconds": 0.0
        })
    return runners

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for key {msg.key()}: {err}")

def run_race_simulation():
    producer = Producer(KAFKA_CONFIG)
    runners = generate_runner_roster(NUM_RUNNERS)
    race_start_time = datetime.now(timezone.utc)

    print(f"🏃 Starting 10k Race Simulation for {NUM_RUNNERS} runners...")
    print(f"🏁 Race Start Time: {race_start_time.isoformat()}\n")

    # 1. Emit START line check-ins
    for runner in runners:
        event = {
            "event_id": f"evt_{fake.uuid4()[:8]}",
            "bib_number": runner["bib_number"],
            "runner_name": runner["runner_name"],
            "age_group": runner["age_group"],
            "gender": runner["gender"],
            "checkpoint_id": CHECKPOINTS[0]["id"],
            "distance_km": CHECKPOINTS[0]["distance"],
            "timestamp": race_start_time.isoformat()
        }
        producer.produce(
            KAFKA_TOPIC,
            key=str(event["bib_number"]),
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )
        runner["current_checkpoint_idx"] += 1

    producer.flush()

    # 2. Simulate course progress across remaining mats
    active_runners = list(runners)
    while active_runners:
        for runner in active_runners:
            curr_idx = runner["current_checkpoint_idx"]
            prev_dist = CHECKPOINTS[curr_idx - 1]["distance"]
            next_dist = CHECKPOINTS[curr_idx]["distance"]
            segment_dist = next_dist - prev_dist

            segment_pace = runner["base_pace"] * random.uniform(0.95, 1.08)
            runner["elapsed_seconds"] += segment_dist * segment_pace * 60

        active_runners.sort(key=lambda x: x["elapsed_seconds"])

        runner_to_emit = active_runners.pop(0)
        curr_idx = runner_to_emit["current_checkpoint_idx"]
        checkpoint = CHECKPOINTS[curr_idx]

        event_time = datetime.fromtimestamp(
            race_start_time.timestamp() + runner_to_emit["elapsed_seconds"],
            tz=timezone.utc
        )

        event = {
            "event_id": f"evt_{fake.uuid4()[:8]}",
            "bib_number": runner_to_emit["bib_number"],
            "runner_name": runner_to_emit["runner_name"],
            "age_group": runner_to_emit["age_group"],
            "gender": runner_to_emit["gender"],
            "checkpoint_id": checkpoint["id"],
            "distance_km": checkpoint["distance"],
            "timestamp": event_time.isoformat()
        }

        producer.produce(
            KAFKA_TOPIC,
            key=str(event["bib_number"]),
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)

        print(f"[{event['checkpoint_id']}] Bib #{event['bib_number']} - {event['runner_name']} @ {event['timestamp']}")

        runner_to_emit["current_checkpoint_idx"] += 1
        if runner_to_emit["current_checkpoint_idx"] < len(CHECKPOINTS):
            active_runners.append(runner_to_emit)

        time.sleep(0.01)

    producer.flush()
    print("\n🏁 All runners crossed the finish line!")

if __name__ == "__main__":
    run_race_simulation()