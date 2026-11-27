"""
Replays ONE day of flights_sample.csv into Kafka, in (sped-up) departure order,
so the stream behaves like "today unfolding".

Adapted from lab9/kafka-producer-game/producer.py.
Run it on your laptop; the Spark job runs on the VM.
"""
import json
import time
from datetime import date

import pandas as pd
from kafka import KafkaProducer

BOOTSTRAP = "35.255.233.56:9092"          # your VM's external IP (same as in the lab)
TOPIC = "flights"
CSV_PATH = "flights_sample.csv"
SPEEDUP = 120                    # 120 = one simulated day takes ~12 real minutes
SIMULATE_TODAY = True            # rewrite the flight date to today's date

# Column names of flights_sample.csv (checked against your header).
# NOTE: "airline" (the name) is deliberately NOT sent to Kafka - Spark looks it up
# from airlines.csv, so that step 2 (the stream-static join) is a real join.
COL = {
    "date": "flight_date",
    "airline": "airline_code",
    "flight_no": "flight_number",
    "origin": "origin",
    "dest": "dest",
    "sched_dep": "sched_dep_time",    # scheduled departure, hhmm (always present)
    "dep_delay": "dep_delay",         # minutes (negative = early), empty for cancelled flights
    "cancelled": "cancelled",         # 0/1
}


def hhmm_to_minutes(v):
    """815 -> 495, 2400 -> 0"""
    v = int(v) % 2400
    return (v // 100) * 60 + v % 100


def build_records(df, day=None):
    df = df.dropna(subset=[COL["sched_dep"], COL["airline"]]).copy()
    df[COL["date"]] = pd.to_datetime(df[COL["date"]]).dt.date
    # replay the busiest day in the file, so the scoreboard has the most to show
    day = day or df[COL["date"]].value_counts().idxmax()
    df = df[df[COL["date"]] == day].copy()

    df["sched_min"] = df[COL["sched_dep"]].map(hhmm_to_minutes)
    delay = df[COL["dep_delay"]].fillna(0).clip(lower=0)
    cancelled = df[COL["cancelled"]].fillna(0).astype(int)
    # a flight shows up in the stream when it actually leaves (or when it is cancelled)
    df["emit_min"] = df["sched_min"] + delay.where(cancelled == 0, 0)
    df = df.sort_values("emit_min")

    out_date = date.today() if SIMULATE_TODAY else day
    for r in df.itertuples(index=False):
        sched_min = int(r.sched_min)
        dd = getattr(r, COL["dep_delay"])
        yield r.emit_min, {
            "airline_code": getattr(r, COL["airline"]),
            "flight_no": str(getattr(r, COL["flight_no"])),
            "origin": getattr(r, COL["origin"]),
            "dest": getattr(r, COL["dest"]),
            # event time = SCHEDULED departure, as a string Spark can parse
            "event_time": f"{out_date} {sched_min // 60:02d}:{sched_min % 60:02d}:00",
            "dep_delay": None if pd.isna(dd) else float(dd),
            "cancelled": int(getattr(r, COL["cancelled"]) or 0),
        }


def run():
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
    df = pd.read_csv(CSV_PATH)
    prev = None
    for emit_min, rec in build_records(df):
        if prev is not None and emit_min > prev:
            producer.flush()
            time.sleep((emit_min - prev) * 60 / SPEEDUP)
        producer.send(TOPIC, json.dumps(rec).encode("utf-8"))
        print("Sending", rec)
        prev = emit_min
    producer.flush()


if __name__ == "__main__":
    run()
