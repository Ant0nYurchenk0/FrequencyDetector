# Image = API code + one approved model file. Build it after downloading the champion (see README).
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
COPY model/model.onnx model/model.onnx

# Recorded in the image so /model-info and every prediction report which model answered.
ARG MODEL_VERSION=unknown
ENV MODEL_VERSION=${MODEL_VERSION}

EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
