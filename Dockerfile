# One image per job: docker build --build-arg JOB=ota-status-tracker .
# REGISTRY lets a build pull the same pinned images through a mirror (e.g. mirror.gcr.io/library).
ARG REGISTRY=docker.io/library
ARG RUST_IMAGE=${REGISTRY}/rust:1.98.1-slim-bookworm@sha256:ff521445a372125ed4f76e1453a1f8098f2d05332d1601d30db1c1f62757e730
ARG RUNTIME_IMAGE=${REGISTRY}/debian:bookworm-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8

FROM ${RUST_IMAGE} AS chef
# librdkafka is built from source by rdkafka-sys (cmake-build feature, no SSL or SASL).
RUN apt-get update && apt-get install -y --no-install-recommends cmake g++ make \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-chef --version 0.1.78 --locked
WORKDIR /src

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG JOB
COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json --bin ${JOB}
COPY . .
RUN cargo build --release --locked --bin ${JOB} && cp target/release/${JOB} /job

FROM ${RUNTIME_IMAGE}
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin telemetry
COPY --from=builder /job /usr/local/bin/job
USER 10001
EXPOSE 9090
ENTRYPOINT ["/usr/local/bin/job"]
