FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential gcc g++ make pkg-config libsasl2-dev libsasl2-modules libssl-dev \
  && rm -rf /var/lib/apt/lists/*

COPY services/benchmark-agent/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services /app/services

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-u", "/app/services/benchmark-agent/agent.py"]
