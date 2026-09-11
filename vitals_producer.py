import os
import io
import json
import time
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer
import fastavro
import logging

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s","level":"%(levelname)s","component":"vitals_producer","message":"%(message)s"}',
    datefmt='%Y-%m-%dT%H:%M:%S%z'
)
logger = logging.getLogger("vitals_producer")

load_dotenv()

BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

TOPIC = "vitals-stream"

# Fixed pool of patients so the daily batch join later has something to match
PATIENT_IDS = [f"P{100 + i}" for i in range(15)]  # P100 ... P114

# Load Avro schema
with open("vitals.avsc", "r") as f:
    schema_dict = json.load(f)
schema = fastavro.parse_schema(schema_dict)


def serialize_vital(reading: dict) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, reading)
    return buf.getvalue()


def generate_normal_reading(patient_id: str) -> dict:
    """Generate a realistic, healthy vitals reading."""
    return {
        "patient_id": patient_id,
        "heart_rate": random.randint(60, 100),
        "spo2": round(random.uniform(95.0, 100.0), 1),
        "systolic_bp": random.randint(100, 130),
        "diastolic_bp": random.randint(65, 85),
        "temperature": round(random.uniform(36.1, 37.2), 1),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def generate_abnormal_reading(patient_id: str) -> dict:
    """Generate an out-of-range reading simulating a concerning event."""
    return {
        "patient_id": patient_id,
        "heart_rate": random.choice([random.randint(40, 55), random.randint(120, 160)]),
        "spo2": round(random.uniform(80.0, 90.0), 1),
        "systolic_bp": random.choice([random.randint(70, 90), random.randint(150, 180)]),
        "diastolic_bp": random.choice([random.randint(40, 55), random.randint(95, 110)]),
        "temperature": round(random.uniform(38.5, 40.0), 1),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def generate_reading(patient_id: str, abnormal_probability: float = 0.1) -> dict:
    if random.random() < abnormal_probability:
        return generate_abnormal_reading(patient_id)
    return generate_normal_reading(patient_id)


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Kafka delivery failed: {err}")
    else:
        pass  # keep console clean for continuous streaming


def main():
    producer_config = {
        "bootstrap.servers": BOOTSTRAP_SERVER,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": API_KEY,
        "sasl.password": API_SECRET,
    }
    producer = Producer(producer_config)

    logger.info(f"Starting vitals stream simulator for {len(PATIENT_IDS)} patients")

    try:
        while True:
            patient_id = random.choice(PATIENT_IDS)
            reading = generate_reading(patient_id)
            avro_bytes = serialize_vital(reading)

            producer.produce(
                topic=TOPIC,
                key=patient_id,
                value=avro_bytes,
                callback=delivery_report
            )
            producer.poll(0)

            flag = "ABNORMAL" if (
                reading["heart_rate"] < 60 or reading["heart_rate"] > 100 or
                reading["spo2"] < 95 or reading["temperature"] > 37.5
            ) else "normal"

            if flag == "ABNORMAL":
                logger.warning(
                    f"Abnormal reading detected: patient={reading['patient_id']} "
                    f"hr={reading['heart_rate']} spo2={reading['spo2']} temp={reading['temperature']}"
                )
            else:
                logger.info(f"Reading sent: patient={reading['patient_id']}")

            time.sleep(2)

    except KeyboardInterrupt:
        logger.info("Stopping simulator...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()