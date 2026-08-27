FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc-dev \
    pkg-config \
    libvirt-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --prefix=/install \
    confluent-kafka \
    ovsdbapp \
    ovs \
    libvirt-python \
    PyYAML

FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libvirt0 \
    libsasl2-2 \
    libsasl2-modules \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local

WORKDIR /app
COPY ovn_agent.py .

ENTRYPOINT ["python3", "ovn_agent.py"]
