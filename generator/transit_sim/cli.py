"""CLI генератора данных.

  python -m transit_sim.cli static  --out /data/gtfs
  python -m transit_sim.cli history --out /data/raw --days 30 --end 2026-11-17
  python -m transit_sim.cli stream  --bootstrap kafka:9092 --speed 30
"""
from __future__ import annotations

import argparse
import csv
import gzip
import heapq
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from .delay_model import DaySimulator, iter_positions
from .network import TZ, Network

SEED = int(os.environ.get("SIM_SEED", "42"))
N_ROUTES = int(os.environ.get("SIM_ROUTES", "40"))


def cmd_static(a):
    net = Network(a.routes, SEED)
    stats = net.write_gtfs(a.out)
    print(json.dumps(stats, ensure_ascii=False))


def _history_day(args):
    out, d_iso, routes = args
    d = date.fromisoformat(d_iso)
    net = Network(routes, SEED)
    sim = DaySimulator(net, d, SEED)
    part_dir = os.path.join(out, "vehicle_positions", f"date={d_iso}")
    os.makedirs(part_dir, exist_ok=True)
    n = 0
    with gzip.open(os.path.join(part_dir, "part-0000.jsonl.gz"), "wt", encoding="utf-8", compresslevel=3) as f:
        for trip in net.trips:
            arr = sim.simulate_trip(trip)
            for m in iter_positions(sim, trip, arr):
                f.write(json.dumps(m, separators=(",", ":")))
                f.write("\n")
                n += 1
    return d_iso, n, sim.weather, sim.incidents


def cmd_history(a):
    end = date.fromisoformat(a.end) if a.end else date.today() - timedelta(days=1)
    days = [(end - timedelta(days=i)).isoformat() for i in range(a.days)][::-1]
    os.makedirs(a.out, exist_ok=True)
    weather, incidents, total = [], [], 0
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=a.workers) as ex:
        for d_iso, n, w, inc in ex.map(_history_day, [(a.out, d, a.routes) for d in days]):
            total += n
            weather += w
            incidents += [dict(date=d_iso, **i.__dict__) for i in inc]
            print(f"{d_iso}: {n:,} GPS-отметок  (всего {total:,}, {time.time() - t0:.0f} c)", flush=True)
    _write_csv(os.path.join(a.out, "weather_hourly.csv"), weather)
    _write_csv(os.path.join(a.out, "incidents.csv"), incidents)
    print(f"Готово: {total:,} отметок за {len(days)} дн.")


def _write_csv(path, rows):
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def cmd_stream(a):
    from kafka import KafkaProducer  # kafka-python

    for attempt in range(30):
        try:
            prod = KafkaProducer(bootstrap_servers=a.bootstrap, linger_ms=50,
                                 value_serializer=lambda v: json.dumps(v).encode(),
                                 key_serializer=lambda k: k.encode())
            break
        except Exception as e:  # Kafka ещё стартует
            print("Kafka недоступна, жду…", e, flush=True)
            time.sleep(5)
    else:
        sys.exit("Kafka так и не стала доступна")

    tz = ZoneInfo(TZ)
    today = datetime.now(tz).date()
    net = Network(a.routes, SEED)
    sim = DaySimulator(net, today, SEED)
    start_sec = a.start_hour * 3600
    # эпоха симуляции: реальное «сейчас» соответствует start_sec симулированного дня
    wall0 = time.time()

    def sim_now():
        return start_sec + (time.time() - wall0) * a.speed

    # погода: отправляем все часы до текущего и далее по мере наступления
    sent_weather = set()

    def push_weather(upto_sec):
        for w in sim.weather:
            if w["hour"] * 3600 <= upto_sec and w["hour"] not in sent_weather:
                ts = sim.t0 + w["hour"] * 3600
                prod.send("weather", key=str(w["hour"]), value=dict(w, timestamp=ts))
                sent_weather.add(w["hour"])

    heap, idx = [], 0
    trips = [t for t in net.trips if t.start_sec + 3 * 3600 >= start_sec]
    print(f"Стрим: {len(trips)} рейсов, скорость x{a.speed}, старт {a.start_hour}:00", flush=True)
    sent, last_log = 0, time.time()
    while trips or heap:
        now = sim_now()
        push_weather(now)
        # подключаем рейсы, которые вот-вот выйдут на линию
        while idx < len(trips) and trips[idx].start_sec <= now + 60:
            t = trips[idx]
            gen = iter_positions(sim, t, sim.simulate_trip(t))
            first = next(gen, None)
            if first:
                heapq.heappush(heap, (first["timestamp"], idx, first, gen))
            idx += 1
        if idx >= len(trips):
            trips = []
        now_epoch = sim.t0 + now
        while heap and heap[0][0] <= now_epoch:
            _, i, m, gen = heapq.heappop(heap)
            if m["timestamp"] >= sim.t0 + start_sec:  # прошлое не шлём
                prod.send("vehicle_positions", key=m["vehicle_id"], value=m)
                sent += 1
            nxt = next(gen, None)
            if nxt:
                heapq.heappush(heap, (nxt["timestamp"], i, nxt, gen))
        if time.time() - last_log > 10:
            hh = int(now // 3600); mm = int(now % 3600 // 60)
            print(f"[сим {hh:02d}:{mm:02d}] отправлено {sent:,}, на линии {len(heap)} ТС", flush=True)
            last_log = time.time()
        if now > 26 * 3600:
            break
        time.sleep(0.2)
    prod.flush()
    print("Симулированный день закончился.")


def main(argv=None):
    p = argparse.ArgumentParser(prog="transit_sim")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("static"); s.add_argument("--out", required=True); s.add_argument("--routes", type=int, default=N_ROUTES)
    h = sub.add_parser("history"); h.add_argument("--out", required=True); h.add_argument("--days", type=int, default=30)
    h.add_argument("--end", default=None); h.add_argument("--routes", type=int, default=N_ROUTES)
    h.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    st = sub.add_parser("stream"); st.add_argument("--bootstrap", default="kafka:9092")
    st.add_argument("--speed", type=float, default=30.0); st.add_argument("--start-hour", type=int, default=7)
    st.add_argument("--routes", type=int, default=N_ROUTES)
    a = p.parse_args(argv)
    {"static": cmd_static, "history": cmd_history, "stream": cmd_stream}[a.cmd](a)


if __name__ == "__main__":
    main()
