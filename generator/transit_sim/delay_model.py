"""Модель «реальности»: как транспорт отклоняется от расписания.

Отклонение на каждом перегоне складывается из:
  * пробок (зависят от часа, дня недели и зоны города);
  * погоды (дождь/снег замедляет движение и посадку);
  * «узких мест» — отдельных проблемных перегонов;
  * инцидентов (ДТП, ремонт) — редкие, но сильные задержки на части маршрута;
  * случайного шума.
Водитель, идущий с опережением, «отстаивается» на контрольных остановках.
Модель знает только Spark-конвейер, ML-модель о ней ничего не знает и
должна выучить закономерности по данным.
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from .network import TZ, Network, Trip, haversine_m

_TZ = ZoneInfo(TZ)
ZONE_WEIGHT = {"center": 1.0, "middle": 0.65, "outer": 0.3}


def congestion(hour_f: float, dow: int) -> float:
    """Индекс загруженности дорог 0..~0.8. dow: 0=пн … 6=вс."""
    g = lambda mu, sd, a: a * math.exp(-((hour_f - mu) ** 2) / (2 * sd ** 2))
    if dow < 5:
        friday = 0.12 if dow == 4 else 0.0
        return 0.05 + g(8.3, 1.1, 0.55) + g(18.2, 1.4, 0.62 + friday) + g(13.5, 2.0, 0.12)
    return 0.04 + g(14.0, 3.0, 0.22)


@dataclass
class Incident:
    route_id: str
    direction_id: int
    start_sec: int
    end_sec: int
    first_seq: int
    last_seq: int
    extra_sec_per_seg: int
    kind: str


def day_weather(d: date, seed: int) -> list[dict]:
    """Почасовая погода: марковская цепь «сухо/дождь» + суточный ход температуры."""
    rng = random.Random(f"weather-{seed}-{d.isoformat()}")
    raining = rng.random() < 0.25
    month = d.month
    base_t = {1: -8, 2: -7, 3: -1, 4: 7, 5: 14, 6: 18, 7: 20, 8: 18, 9: 12, 10: 5, 11: -1, 12: -5}[month]
    out = []
    for h in range(24):
        if raining:
            raining = rng.random() < 0.8
        else:
            raining = rng.random() < 0.07
        rain = round(rng.expovariate(1 / 1.8), 1) if raining else 0.0
        temp = round(base_t + 4 * math.sin((h - 9) / 24 * 2 * math.pi) + rng.gauss(0, 1.2), 1)
        out.append({"date": d.isoformat(), "hour": h, "precip_mm": rain, "temp_c": temp,
                    "snow": int(rain > 0 and temp < 0)})
    return out


def day_incidents(net: Network, d: date, seed: int) -> list[Incident]:
    rng = random.Random(f"inc-{seed}-{d.isoformat()}")
    out = []
    n = rng.randint(1, max(2, len(net.routes) // 6))
    for _ in range(n):
        r = rng.choice(net.routes)
        direction = rng.randint(0, 1)
        nstops = len(r.stops[direction])
        a = rng.randint(1, max(1, nstops - 4))
        start = rng.randint(6 * 3600, 21 * 3600)
        kind = rng.choice(["ДТП", "ДТП", "ремонт дороги", "поломка ТС", "перекрытие"])
        dur = rng.randint(30, 150) * 60 if kind != "ремонт дороги" else rng.randint(4, 10) * 3600
        out.append(Incident(r.route_id, direction, start, start + dur, a, min(nstops, a + rng.randint(2, 6)),
                            rng.randint(30, 90) if kind == "ремонт дороги" else rng.randint(60, 240), kind))
    return out


def midnight_epoch(d: date) -> int:
    return int(datetime.combine(d, time(0), tzinfo=_TZ).timestamp())


class DaySimulator:
    """Считает фактическое время прибытия каждого рейса на каждую остановку за день."""

    def __init__(self, net: Network, d: date, seed: int = 42):
        self.net, self.d, self.seed = net, d, seed
        self.dow = d.weekday()
        self.weather = day_weather(d, seed)
        self.incidents = day_incidents(net, d, seed)
        self._inc_by_route: dict[tuple[str, int], list[Incident]] = {}
        for inc in self.incidents:
            self._inc_by_route.setdefault((inc.route_id, inc.direction_id), []).append(inc)
        self.t0 = midnight_epoch(d)
        # «характер» дня: иногда весь город едет хуже обычного
        self.day_factor = random.Random(f"day-{seed}-{d}").gauss(1.0, 0.05)

    def precip(self, sec: int) -> tuple[float, int]:
        w = self.weather[min(23, int(sec // 3600))]
        return w["precip_mm"], w["snow"]

    def simulate_trip(self, trip: Trip) -> list[dict]:
        """Возвращает список прибытий: stop_sequence, плановое и фактическое время (сек от полуночи)."""
        rng = random.Random(f"{self.seed}-{self.d}-{trip.trip_id}")
        route = self.net.route_by_id[trip.route_id]
        stops = route.stops[trip.direction_id]
        segs = route.seg_sched_sec[trip.direction_id]
        incs = self._inc_by_route.get((trip.route_id, trip.direction_id), [])
        driver = rng.gauss(1.0, 0.04)  # стиль водителя
        # задержка выхода на линию
        t = trip.start_sec + max(-30, int(rng.gauss(20, 50)))
        out = []
        for i, stop in enumerate(stops):
            seq = i + 1
            if i > 0:
                sched_seg = segs[i]
                run = sched_seg - 25
                hour_f = t / 3600
                c = congestion(hour_f, self.dow) * ZONE_WEIGHT[stop.zone]
                rain, snow = self.precip(t)
                weather = 0.04 * min(rain, 5) + (0.08 if snow else 0.0)
                bn = stop.bottleneck * c * 1.6
                factor = (0.9 + 0.85 * c + weather + bn) * driver * self.day_factor
                factor *= math.exp(rng.gauss(0, 0.10))
                dwell = 20 + 14 * c + 2.5 * min(rain, 4) + rng.expovariate(1 / 5)
                travel = run * factor + dwell
                for inc in incs:
                    if inc.start_sec <= t <= inc.end_sec and inc.first_seq <= seq <= inc.last_seq:
                        travel += inc.extra_sec_per_seg * rng.uniform(0.6, 1.4)
                t = t + travel
            # контрольные остановки: не уезжать раньше графика больше чем на 30 сек
            if seq % 5 == 0 and t < trip.sched[i] - 30:
                t = trip.sched[i] - 30 + rng.uniform(0, 20)
            out.append({
                "stop_sequence": seq,
                "stop_id": stop.stop_id,
                "sched_sec": trip.sched[i],
                "actual_sec": int(round(t)),
            })
        return out


def iter_positions(sim: DaySimulator, trip: Trip, arrivals: list[dict], ping_sec: int = 20):
    """Превращает прибытия в поток GPS-отметок в стиле GTFS-realtime VehiclePosition."""
    rng = random.Random(f"gps-{sim.seed}-{sim.d}-{trip.trip_id}")
    route = sim.net.route_by_id[trip.route_id]
    stops = route.stops[trip.direction_id]
    start_date = sim.d.strftime("%Y%m%d")

    def msg(ts, lat, lon, status, seq, speed):
        return {
            "vehicle_id": trip.vehicle_id,
            "trip_id": trip.trip_id,
            "route_id": trip.route_id,
            "direction_id": trip.direction_id,
            "start_date": start_date,
            "timestamp": sim.t0 + int(ts),
            "latitude": round(lat + rng.gauss(0, 0.00008), 6),
            "longitude": round(lon + rng.gauss(0, 0.00012), 6),
            "speed_kmh": round(speed, 1),
            "current_stop_sequence": seq,
            "stop_id": stops[seq - 1].stop_id,
            "current_status": status,
        }

    for i, a in enumerate(arrivals):
        s = stops[i]
        if i > 0:
            prev = stops[i - 1]
            t_dep = arrivals[i - 1]["actual_sec"] + 20
            t_arr = a["actual_sec"]
            dur = max(1, t_arr - t_dep)
            v = haversine_m(prev.lat, prev.lon, s.lat, s.lon) * 1.25 / dur * 3.6
            tt = t_dep + ping_sec
            while tt < t_arr:
                f = (tt - t_dep) / dur
                yield msg(tt, prev.lat + (s.lat - prev.lat) * f, prev.lon + (s.lon - prev.lon) * f,
                          "IN_TRANSIT_TO", i + 1, max(0, v * rng.uniform(0.6, 1.4)))
                tt += ping_sec
        yield msg(a["actual_sec"], s.lat, s.lon, "STOPPED_AT", i + 1, 0.0)
        # иногда ТС стоит дольше — вторая отметка STOPPED_AT (конвейер должен дедуплицировать)
        if rng.random() < 0.35:
            yield msg(a["actual_sec"] + rng.randint(8, 18), s.lat, s.lon, "STOPPED_AT", i + 1, 0.0)
