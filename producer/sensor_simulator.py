import json
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "engine-rul-topic")
INTERVAL_SECONDS = float(os.getenv("SIM_INTERVAL_SECONDS", "0.2"))
ENGINE_COUNT = int(os.getenv("SIM_ENGINE_COUNT", "10"))
START_RUL = int(os.getenv("SIM_START_RUL", "180"))
LOOP_FOREVER = os.getenv("SIM_LOOP_FOREVER", "true").lower() == "true"
SEED = int(os.getenv("SIM_SEED", "42"))

# CMAPSS FD001 style features
SENSOR_COLUMNS = [
    "setting_1",
    "setting_2",
    "setting_3",
    "T2",
    "T24",
    "T30",
    "T50",
    "P2",
    "P15",
    "P30",
    "Nf",
    "Nc",
    "epr",
    "Ps30",
    "phi",
    "NRf",
    "NRc",
    "BPR",
    "farB",
    "htBleed",
    "Nf_dmd",
    "PCNfR_dmd",
    "W31",
    "W32",
]


@dataclass
class EngineState:
    unit_number: int
    cycle: int = 1
    remaining_rul: int = START_RUL


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: str(v).encode("utf-8"),
        acks="all",
        retries=10,
    )


def make_sensor_values(engine: EngineState) -> dict:
    # 使用平滑趋势 + 少量噪声模拟传感器慢慢劣化。
    degradation = (START_RUL - engine.remaining_rul) / max(START_RUL, 1)
    rng = random.random

    values = {
        "setting_1": round(0.0 + 0.3 * rng(), 4),
        "setting_2": round(-0.01 + 0.02 * rng(), 5),
        "setting_3": round(100.0, 3),
        "T2": round(518.0 + 1.5 * rng(), 3),
        "T24": round(640.0 + 12.0 * degradation + 1.0 * rng(), 3),
        "T30": round(1580.0 + 30.0 * degradation + 2.0 * rng(), 3),
        "T50": round(1400.0 + 35.0 * degradation + 2.0 * rng(), 3),
        "P2": round(14.5 + 0.1 * rng(), 3),
        "P15": round(21.0 - 1.2 * degradation + 0.1 * rng(), 3),
        "P30": round(550.0 - 18.0 * degradation + rng(), 3),
        "Nf": round(2388.0 - 30.0 * degradation + 2.0 * rng(), 3),
        "Nc": round(9050.0 - 120.0 * degradation + 4.0 * rng(), 3),
        "epr": round(1.30 - 0.05 * degradation + 0.005 * rng(), 4),
        "Ps30": round(46.0 - 2.0 * degradation + 0.2 * rng(), 3),
        "phi": round(520.0 + 8.0 * degradation + rng(), 3),
        "NRf": round(2388.0 - 24.0 * degradation + 2.0 * rng(), 3),
        "NRc": round(8100.0 - 90.0 * degradation + 3.0 * rng(), 3),
        "BPR": round(8.40 + 0.2 * degradation + 0.02 * rng(), 4),
        "farB": round(0.03 + 0.015 * degradation + 0.002 * rng(), 5),
        "htBleed": round(390.0 + 10.0 * degradation + rng(), 3),
        "Nf_dmd": round(2388.0, 3),
        "PCNfR_dmd": round(100.0, 3),
        "W31": round(38.0 - 0.8 * degradation + 0.05 * rng(), 4),
        "W32": round(23.0 - 0.5 * degradation + 0.05 * rng(), 4),
    }

    for col in SENSOR_COLUMNS:
        if col not in values:
            values[col] = 0.0

    return values


def build_payload(engine: EngineState) -> dict:
    payload = {
        "unit_number": engine.unit_number,
        "time_cycles": engine.cycle,
        "actual_rul": float(max(engine.remaining_rul, 0)),
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    payload.update(make_sensor_values(engine))
    return payload


def tick_engine(engine: EngineState) -> None:
    engine.cycle += 1
    engine.remaining_rul -= 1


def main() -> None:
    random.seed(SEED)
    states = [EngineState(unit_number=i + 1) for i in range(ENGINE_COUNT)]
    producer = create_producer()

    print(
        f"[sensor-sim] start bootstrap={BOOTSTRAP}, topic={TOPIC}, engines={ENGINE_COUNT}, interval={INTERVAL_SECONDS}s"
    )

    while states:
        for engine in list(states):
            payload = build_payload(engine)
            producer.send(TOPIC, key=payload["unit_number"], value=payload).get(timeout=20)

            if payload["time_cycles"] % 20 == 0:
                print(
                    f"[sensor-sim] engine={payload['unit_number']} cycle={payload['time_cycles']} actual_rul={payload['actual_rul']}"
                )

            tick_engine(engine)
            if engine.remaining_rul <= 0 and not LOOP_FOREVER:
                states.remove(engine)

            if LOOP_FOREVER and engine.remaining_rul <= 0:
                engine.cycle = 1
                engine.remaining_rul = START_RUL

            time.sleep(INTERVAL_SECONDS)

    producer.flush()
    producer.close()
    print("[sensor-sim] finished")


if __name__ == "__main__":
    main()
