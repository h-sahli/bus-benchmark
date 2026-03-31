FROM python:3.12-slim

ARG TARGETARCH=amd64
ARG HELM_VERSION=v3.20.1
ARG KUBECTL_VERSION=v1.35.3

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL -o /tmp/helm.tgz "https://get.helm.sh/helm-${HELM_VERSION}-linux-${TARGETARCH}.tar.gz" \
    && tar -xzf /tmp/helm.tgz -C /tmp \
    && mv "/tmp/linux-${TARGETARCH}/helm" /usr/local/bin/helm \
    && rm -rf /tmp/helm.tgz "/tmp/linux-${TARGETARCH}" \
    && curl -fsSL -o /usr/local/bin/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl" \
    && chmod +x /usr/local/bin/kubectl

COPY services/platform/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services /app/services
COPY catalog /app/catalog
COPY deploy/kustomize /app/deploy/kustomize
COPY deploy/k8s/brokers /app/deploy/k8s/brokers
COPY deploy/k8s/operators /app/deploy/k8s/operators

ENV PYTHONPATH=/app
ENV BUS_DB_PATH=/data/bus.db

RUN mkdir -p /data

EXPOSE 8000

CMD ["uvicorn", "services.platform.app:app", "--host", "0.0.0.0", "--port", "8000"]
