import json
import random
import time
import uuid

from confluent_kafka import Producer

import config


class Detector:
    def run(self):
        """Generate scans forever and send them to Kafka in batches."""
        kafka_conf = {"bootstrap.servers": config.KAFKA_BOOTSTRAP}
        if config.KAFKA_USERNAME:
            kafka_conf.update({
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanism": "PLAIN",
                "sasl.username": config.KAFKA_USERNAME,
                "sasl.password": config.KAFKA_PASSWORD,
            })
        producer = Producer(kafka_conf)
        session_id = uuid.uuid4().hex  # scan_id restarts at 0 each run; (session_id, scan_id) is unique
        emitter_on = False
        scan_id = 0
        while True:
            batch = []
            for _ in range(config.BATCH_SIZE):
                if random.random() < config.TOGGLE_PROBABILITY:
                    emitter_on = not emitter_on
                power = [
                    (config.emitter_signal(ch) if emitter_on else 0.0)
                    + random.gauss(0, config.NOISE_SIGMA)
                    for ch in range(config.NUM_CHANNELS)
                ]
                batch.append({"session_id": session_id, "scan_id": scan_id, "power": power, "emitter_on": emitter_on})
                scan_id += 1

            producer.produce(config.SCANS_TOPIC, json.dumps(batch))
            producer.flush()
            time.sleep(config.BATCH_PAUSE_SECONDS)


if __name__ == "__main__":
    Detector().run()
