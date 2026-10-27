#!/usr/bin/env bash
# Runs the legacy Spark job on run1..run3.jsonl as three AvailableNow runs sharing one checkpoint, and writes
# each run's new output to legacy-runN.jsonl. Needs a telemetry-parity checkout (for its compose file and
# spark-runner image). Usage: legacy_run.sh <telemetry-parity dir>
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
tp="$(cd "$1" && pwd)"
source "$tp/deps.lock"
export SPARK_RUNNER_IMAGE="${SPARK_RUNNER_IMAGE:-ghcr.io/reillym11/telemetry-parity/spark-runner:${TELEMETRY_SPARK_JOBS_SHA:0:12}}"
export KAFKA_PUBLISH=127.0.0.1:19094 MINIO_PUBLISH=127.0.0.1:19000
dc() { docker compose -p charging-timing -f "$tp/docker-compose.yml" "$@"; }
kafka() { dc exec -T kafka /opt/kafka/bin/"$@"; }
in=vehicle.charging.events.v1 out=legacy.charging.sessions.v1
dc --profile jobs down -v --remove-orphans >/dev/null 2>&1
dc up -d --wait kafka >/dev/null
for t in $in $out; do kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic $t --partitions 6 >/dev/null; done
seen=0
for i in 1 2 3; do
  python3 -c 'import json,sys; [print(json.loads(l)["vin"] + "\t" + l, end="") for l in sys.stdin]' < "$here/run$i.jsonl" |
    kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic $in --property parse.key=true --property key.separator=$'\t'
  dc --profile jobs run --rm -T -v charging-timing-ckpt:/ckpt spark-runner /opt/spark/bin/spark-submit --master 'local[2]' \
    --conf spark.sql.session.timeZone=UTC --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
    --conf spark.jars.ivy=/tmp/.ivy2 --class telemetry.jobs.charging.ChargingSessionBuilder \
    /opt/jobs/charging-session-builder.jar --bootstrap kafka:9092 --checkpoint /ckpt/charging --output $out \
    > "$here/legacy-run$i.log" 2>&1
  kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic $out --from-beginning --timeout-ms 8000 \
    2>/dev/null | sort > "$here/all.tmp" || true
  comm -13 <(sort "$here/seen.tmp" 2>/dev/null || true) "$here/all.tmp" > "$here/legacy-run$i.jsonl"
  mv "$here/all.tmp" "$here/seen.tmp"
  echo "run$i: $(wc -l < "$here/run$i.jsonl") in, $(wc -l < "$here/legacy-run$i.jsonl") new sessions"
done
rm -f "$here/seen.tmp" "$here"/legacy-run*.log
dc --profile jobs down -v --remove-orphans >/dev/null 2>&1
docker volume rm -f charging-timing-ckpt >/dev/null
