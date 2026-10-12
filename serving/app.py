"""Emitter detector API: serves the champion ONNX model over HTTP.

The model file is baked into the image (see Dockerfile), so every replica serves exactly the same
version. Interactive docs (Swagger UI) are at /docs.
"""
import math
import os
import random
import time

import numpy as np
import onnxruntime as ort
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

NUM_CHANNELS = 100
MODEL_PATH = os.environ.get("MODEL_PATH", "/app/model/model.onnx")
MODEL_VERSION = os.environ.get("MODEL_VERSION", "unknown")
THRESHOLD = float(os.environ.get("THRESHOLD", "0.5"))
NOISE_SIGMA = 0.3  # same as simulator/config.py, so /predict/test inputs look like real scans

app = FastAPI(
    title="RF Emitter Detector",
    description="Predicts whether an RF emitter is active from one spectrum scan (100 power values).",
)

session = None
loaded_at = None
hostname = os.environ.get("HOSTNAME", "local")  # in Kubernetes: the pod name, shows which replica answered


@app.on_event("startup")
def load_model():
    """Load the model once per process. If this fails, /ready stays 503 and Kubernetes sends no traffic."""
    global session, loaded_at
    try:
        session = ort.InferenceSession(MODEL_PATH, providers=["CPUExecutionProvider"])
        loaded_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    except Exception as e:  # keep the process alive so /health and /ready can report the problem
        print(f"failed to load model from {MODEL_PATH}: {e}")


class Scan(BaseModel):
    power: list[float] = Field(..., min_length=NUM_CHANNELS, max_length=NUM_CHANNELS,
                               description="Measured power in the 100 frequency bins, in fixed order.")


class Prediction(BaseModel):
    emitter_on: bool
    probability: float
    threshold: float
    model_version: str
    served_by: str


def predict_one(power):
    if session is None:
        raise HTTPException(status_code=503, detail="model not loaded")
    x = np.asarray([power], dtype=np.float32)
    _, probabilities = session.run(None, {session.get_inputs()[0].name: x})
    p_on = float(np.asarray(probabilities)[0, 1])
    return Prediction(emitter_on=p_on >= THRESHOLD, probability=p_on, threshold=THRESHOLD,
                      model_version=MODEL_VERSION, served_by=hostname)


def emitter_signal(channel):
    """f(x) from simulator/config.py: two peaks, at channels 25 and 70."""
    return math.exp(-((channel - 25) / 5) ** 2) + 0.7 * math.exp(-((channel - 70) / 8) ** 2)


@app.get("/health", summary="Liveness: the process is up")
def health():
    return {"status": "ok"}


@app.get("/ready", summary="Readiness: the model is loaded and can serve predictions")
def ready():
    if session is None:
        raise HTTPException(status_code=503, detail="model not loaded")
    return {"status": "ready"}


@app.get("/model-info", summary="Which model this replica serves")
def model_info():
    if session is None:
        raise HTTPException(status_code=503, detail="model not loaded")
    model_input = session.get_inputs()[0]
    return {
        "model_version": MODEL_VERSION,
        "model_path": MODEL_PATH,
        "loaded_at": loaded_at,
        "input": {"name": model_input.name, "shape": model_input.shape, "type": model_input.type},
        "threshold": THRESHOLD,
        "served_by": hostname,
    }


@app.post("/predict", response_model=Prediction, summary="Predict from one scan you provide")
def predict(scan: Scan):
    return predict_one(scan.power)


@app.get("/predict/test", summary="Generate a synthetic scan and predict it")
def predict_test(
    emitter_on: bool | None = Query(None, description="Force the emitter ON or OFF; leave empty for random."),
):
    """Builds a scan like the simulator does: y = f(x) + noise when ON, y = noise when OFF."""
    actual_on = random.random() < 0.5 if emitter_on is None else emitter_on
    power = [(emitter_signal(ch) if actual_on else 0.0) + random.gauss(0, NOISE_SIGMA)
             for ch in range(NUM_CHANNELS)]
    prediction = predict_one(power)
    return {
        "generated": {"emitter_on": actual_on, "power": [round(p, 4) for p in power]},
        "prediction": prediction,
        "correct": prediction.emitter_on == actual_on,
    }
