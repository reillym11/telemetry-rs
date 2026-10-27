"""Writes the timing-test input: three runs (batches) of vehicle.charging.events.v1 records.

run1: A stops after 00:04 while B keeps sending to 00:45 (watermark 00:35 after the run): A's timeout 00:34
      fires in the run's final no-data batch. C's timeout is exactly 00:35, which does not fire.
run2: B continues to 00:55 (watermark 00:45): C fires with no record of its own. D's charge_start at exactly
      the watermark (00:35) is dropped as late; E's at 00:35:00.001 is kept.
run3: one heartbeat per VIN at 03:00 closes everything left.
"""
import json
import sys
import uuid
from pathlib import Path

T0 = "2026-10-01T{:02d}:{:02d}:{:02d}.{:03d}Z"
VINS = {k: f"SYNTMG{k}" + "0" * 10 for k in "ABCDE"}
seq = {k: 0 for k in VINS}
n = 0


def ev(k, minute, kind, energy=0.0, power=0.0, soc=50.0, ms=0):
    global n
    n += 1
    seq[k] += 1
    hb = kind == "heartbeat"
    h, m = divmod(minute, 60)
    return {
        "event_id": str(uuid.UUID(int=n, version=4)), "vin": VINS[k], "event_time": T0.format(h, m, 0, ms),
        "event_type": kind, "producer_seq": seq[k], "schema_version": "1", "vehicle_model": "AIR",
        "fleet": "consumer", "market": "us-bay-area", "firmware_version": "2026.10.1",
        "connector_type": None if hb else "CCS1", "charger_type": None if hb else "dc_fast",
        "power_kw": None if hb else power, "energy_added_kwh": None if hb else energy,
        "soc_pct": None if hb else soc, "charger_lat": None if hb else 37.38791,
        "charger_lon": None if hb else -121.97213,
    }


run1 = [ev("A", 0, "charge_start", soc=20.0), ev("B", 0, "charge_start", soc=30.0), ev("C", 0, "charge_start")]
run1 += [ev("A", 4, "charge_progress", 3.5, 60.0, 24.0), ev("C", 5, "charge_progress", 4.25, 55.0, 53.0)]
run1 += [ev("B", m, "charge_progress", m / 2, 100.0 + m, 30.0 + m / 3) for m in range(5, 50, 5)]
run2 = [ev("B", 50, "charge_progress", 25.0, 150.5, 47.0), ev("B", 55, "charge_stop", 27.125, 0.0, 48.5)]
run2 += [ev("D", 35, "charge_start", soc=10.0), ev("E", 35, "charge_start", soc=11.0, ms=1)]
run2 += [ev("D", 38, "charge_progress", 1.0, 7.0, 12.0), ev("E", 38, "charge_progress", 1.5, 7.2, 12.5)]
run3 = [ev(k, 180, "heartbeat") for k in "ABCDE"]

out = Path(sys.argv[1])
for i, run in enumerate([run1, run2, run3], 1):
    (out / f"run{i}.jsonl").write_text("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in run))
