import math
import os

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_USERNAME = os.environ.get("KAFKA_USERNAME")  # unset = no authentication (local Kafka)
KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD")
SCANS_TOPIC = "rf-spectrum-scans"

NUM_CHANNELS = 100
BATCH_SIZE = 100  # iterations (scans) per Kafka message
BATCH_PAUSE_SECONDS = 1
TOGGLE_PROBABILITY = 0.5  # chance per iteration of flipping emitter ON <-> OFF
NOISE_SIGMA = 0.3


def emitter_signal(channel):
    """f(x): emitter contribution at a channel number (two peaks, at channels 25 and 70)."""
    return math.exp(-((channel - 25) / 5) ** 2) + 0.7 * math.exp(-((channel - 70) / 8) ** 2)
