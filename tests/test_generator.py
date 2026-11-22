import gzip
import json
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "generator"))

from transit_sim.delay_model import DaySimulator, congestion, iter_positions  # noqa: E402
from transit_sim.network import Network  # noqa: E402

NET = Network(8, seed=1)


def test_network_is_deterministic():
    other = Network(8, seed=1)
    assert [t.trip_id for t in other.trips[:50]] == [t.trip_id for t in NET.trips[:50]]
    assert len(NET.stops) == sum(len(r.stops[0]) + len(r.stops[1]) for r in NET.routes)


def test_schedule_is_monotonic():
    for t in NET.trips[:200]:
        assert all(b > a for a, b in zip(t.sched, t.sched[1:]))


def test_rush_hour_is_worse_than_midday_and_night():
    assert congestion(8.3, 1) > congestion(12, 1) > congestion(3, 1)
    assert congestion(18.2, 4) > congestion(18.2, 6)  # пятница хуже воскресенья


def test_arrivals_are_ordered_and_realistic():
    sim = DaySimulator(NET, date(2026, 11, 16), seed=1)
    delays = []
    for t in NET.trips[::5]:
        arr = sim.simulate_trip(t)
        assert [a["stop_sequence"] for a in arr] == list(range(1, len(arr) + 1))
        assert all(b["actual_sec"] > a["actual_sec"] for a, b in zip(arr, arr[1:]))
        delays += [(a["actual_sec"] - a["sched_sec"]) / 60 for a in arr]
    assert -5 < sum(delays) / len(delays) < 15
    assert min(delays) > -5  # опережение ограничено контрольными остановками


def test_positions_have_one_first_stop_event_per_stop():
    sim = DaySimulator(NET, date(2026, 11, 16), seed=1)
    t = NET.trips[10]
    arr = sim.simulate_trip(t)
    msgs = list(iter_positions(sim, t, arr))
    stopped = {}
    for m in msgs:
        if m["current_status"] == "STOPPED_AT":
            stopped.setdefault(m["current_stop_sequence"], []).append(m["timestamp"])
    assert len(stopped) == len(arr)
    # минимальная отметка STOPPED_AT == фактическое прибытие (так считает Spark)
    for a in arr:
        assert min(stopped[a["stop_sequence"]]) == sim.t0 + a["actual_sec"]
    ts = [m["timestamp"] for m in msgs]
    assert ts == sorted(ts)


def test_gtfs_export(tmp_path):
    stats = NET.write_gtfs(str(tmp_path))
    assert stats["stop_times"] > 1000
    for f in ("agency.txt", "routes.txt", "stops.txt", "trips.txt", "stop_times.txt", "calendar.txt"):
        assert (tmp_path / f).exists()
