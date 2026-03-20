import json
import os
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
CONTROL_FLAG = BASE_DIR / "runtime" / "control" / "producer.enabled"
TEST_PATH = DATA_DIR / "test_FD001.txt"
RUL_PATH = DATA_DIR / "RUL_FD001.txt"

COL_NAMES = [
    "unit_number",
    "time_cycles",
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

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "engine-rul-topic")
INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "0.25"))
LOOP_FOREVER = os.getenv("LOOP_FOREVER", "false").lower() == "true"
RETRY_SECONDS = float(os.getenv("PRODUCER_RETRY_SECONDS", "3"))
CHECK_FLAG = os.getenv("STREAM_CONTROL_ENABLED", "true").lower() == "true"



def load_rows() -> pd.DataFrame:
    test = pd.read_csv(TEST_PATH, sep=r"\s+", header=None, names=COL_NAMES, engine="python")
    rul_truth = pd.read_csv(RUL_PATH, sep=r"\s+", header=None, names=["RUL"], engine="python")
    rul_truth["unit_number"] = range(1, len(rul_truth) + 1)

    last_cycles = test.groupby("unit_number")["time_cycles"].max().rename("last_observed_cycle")
    eol_cycles = last_cycles + rul_truth.set_index("unit_number")["RUL"]
    test["actual_rul"] = test.apply(lambda row: float(eol_cycles.loc[row["unit_number"]] - row["time_cycles"]), axis=1)
    test = test.sort_values(["time_cycles", "unit_number"]).reset_index(drop=True)
    return test



def build_payload(row: pd.Series) -> dict:
    payload = {key: (float(value) if isinstance(value, (int, float)) else value) for key, value in row.to_dict().items()}
    payload["unit_number"] = int(payload["unit_number"])
    payload["time_cycles"] = int(payload["time_cycles"])
    payload["event_time"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return payload



def wait_until_enabled():
    if not CHECK_FLAG:
        return
    while not CONTROL_FLAG.exists():
        print("[producer] waiting for producer.enabled flag ...")
        time.sleep(1.0)



def create_producer() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: str(v).encode("utf-8"),
                acks="all",
                retries=10,
            )
            producer.bootstrap_connected()
            print(f"[producer] connected to Kafka at {BOOTSTRAP}")
            return producer
        except Exception as exc:  # noqa: BLE001
            print(f"[producer] connect failed: {exc}; retry in {RETRY_SECONDS}s")
            time.sleep(RETRY_SECONDS)



def main():
    rows = load_rows()
    producer = create_producer()
    index = 0

    while True:
        if index >= len(rows):
            producer.flush()
            print("[producer] all rows sent.")
            if LOOP_FOREVER:
                index = 0
                continue
            break

        wait_until_enabled()
        row = rows.iloc[index]
        payload = build_payload(row)
        future = producer.send(TOPIC, key=payload["unit_number"], value=payload)
        future.get(timeout=20)

        if index % 50 == 0:
            print(
                f"[producer] topic={TOPIC} index={index} engine={payload['unit_number']} cycle={payload['time_cycles']} actual_rul={payload['actual_rul']}"
            )
            producer.flush()

        index += 1
        time.sleep(INTERVAL_SECONDS)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
