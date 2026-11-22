"""Синтетическая транспортная сеть города в формате GTFS.

Строим «город» вокруг центра (координаты условно московские): радиальные
и хордовые маршруты, остановки каждые 350–650 м, расписание с интервалами,
зависящими от времени суток. Результат записывается как обычный GTFS-фид,
поэтому вместо синтетики можно подставить реальный фид города.
"""
from __future__ import annotations

import csv
import math
import os
import random
from dataclasses import dataclass, field

CENTER_LAT, CENTER_LON = 55.7520, 37.6175
TZ = "Europe/Moscow"

STREETS = [
    "Тверская", "Арбат", "Покровка", "Сретенка", "Мясницкая", "Лесная", "Садовая",
    "Новослободская", "Профсоюзная", "Ленинский пр.", "Варшавское ш.", "Каширское ш.",
    "Мира пр.", "Ярославская", "Щёлковское ш.", "Энтузиастов ш.", "Рязанский пр.",
    "Волгоградский пр.", "Кутузовский пр.", "Можайское ш.", "Ленинградский пр.",
    "Дмитровское ш.", "Алтуфьевское ш.", "Вернадского пр.", "Мичуринский пр.",
    "Нахимовский пр.", "Севастопольский пр.", "Андропова пр.", "Хорошёвское ш.",
    "Народного Ополчения", "Маршала Жукова пр.", "Таганская", "Бауманская",
    "Красносельская", "Сущёвский Вал", "Бутырская", "Гиляровского", "Остоженка",
    "Пятницкая", "Большая Ордынка", "Шаболовка", "Люблинская", "Беловежская",
]
PLACES = [
    "метро", "школа", "поликлиника", "ТЦ", "парк", "дом культуры", "рынок",
    "библиотека", "стадион", "больница", "почта", "сквер", "институт", "МФЦ",
]


def haversine_m(lat1, lon1, lat2, lon2) -> float:
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def offset(lat, lon, dist_m, bearing_rad):
    dlat = dist_m * math.cos(bearing_rad) / 111_320.0
    dlon = dist_m * math.sin(bearing_rad) / (111_320.0 * math.cos(math.radians(lat)))
    return lat + dlat, lon + dlon


def zone_of(lat, lon) -> str:
    d = haversine_m(lat, lon, CENTER_LAT, CENTER_LON)
    if d < 3_000:
        return "center"
    if d < 8_000:
        return "middle"
    return "outer"


@dataclass
class Stop:
    stop_id: str
    name: str
    lat: float
    lon: float
    zone: str
    bottleneck: float = 0.0  # доп. коэффициент «узкого места» на подходе к остановке


@dataclass
class Route:
    route_id: str
    short_name: str
    route_type: int  # 3 = автобус, 0 = трамвай, 11 = троллейбус
    stops: dict[int, list[Stop]] = field(default_factory=dict)  # direction -> stops
    seg_sched_sec: dict[int, list[int]] = field(default_factory=dict)  # время в пути до i-й остановки
    base_headway_min: int = 8


@dataclass
class Trip:
    trip_id: str
    route_id: str
    direction_id: int
    start_sec: int  # секунды от полуночи
    vehicle_id: str
    sched: list[int]  # плановое время прибытия на каждую остановку (секунды от полуночи)


SCHED_SPEED_KMH = {"center": 15.0, "middle": 20.0, "outer": 25.0}
DWELL_SEC = 25


def headway_min(base: int, hour: int) -> int:
    if 7 <= hour < 10 or 17 <= hour < 20:
        return max(4, base - 3)
    if hour < 6 or hour >= 22:
        return base + 8
    return base


class Network:
    def __init__(self, n_routes: int = 40, seed: int = 42):
        self.rng = random.Random(seed)
        self.routes: list[Route] = []
        self.stops: dict[str, Stop] = {}
        self._build(n_routes)
        self.trips: list[Trip] = self._build_trips()
        self.trip_by_id = {t.trip_id: t for t in self.trips}
        self.route_by_id = {r.route_id: r for r in self.routes}

    # ---------- построение сети ----------
    def _stop_name(self) -> str:
        return f"{self.rng.choice(STREETS)}, {self.rng.choice(PLACES)}"

    def _build(self, n_routes: int):
        rng = self.rng
        for i in range(n_routes):
            rid = f"R{i + 1:03d}"
            kind = rng.random()
            rtype = 3 if kind < 0.7 else (11 if kind < 0.85 else 0)
            prefix = {3: "", 11: "Т", 0: "Тм"}[rtype]
            short = f"{prefix}{rng.randint(1, 999)}"
            route = Route(rid, short, rtype, base_headway_min=rng.choice([6, 8, 10, 12, 15]))
            # 60% радиальные (через центр), 40% хордовые (по периферии)
            if rng.random() < 0.6:
                b = rng.uniform(0, 2 * math.pi)
                start = offset(CENTER_LAT, CENTER_LON, rng.uniform(9_000, 14_000), b)
                end = offset(CENTER_LAT, CENTER_LON, rng.uniform(2_000, 12_000), b + math.pi + rng.uniform(-0.6, 0.6))
                via = offset(CENTER_LAT, CENTER_LON, rng.uniform(300, 1_500), rng.uniform(0, 2 * math.pi))
                waypoints = [start, via, end]
            else:
                b = rng.uniform(0, 2 * math.pi)
                r0 = rng.uniform(5_000, 11_000)
                arc = rng.uniform(0.7, 1.4)
                waypoints = [offset(CENTER_LAT, CENTER_LON, r0 * rng.uniform(0.9, 1.1), b + arc * k / 3) for k in range(4)]
            fwd = self._stops_along(rid, waypoints)
            back = [
                Stop(f"{s.stop_id}_B", s.name, *offset(s.lat, s.lon, 25, math.pi / 2), zone=s.zone,
                     bottleneck=s.bottleneck * rng.uniform(0.5, 1.2))
                for s in reversed(fwd)
            ]
            for direction, stops in ((0, fwd), (1, back)):
                route.stops[direction] = stops
                route.seg_sched_sec[direction] = self._sched_segments(stops)
                for s in stops:
                    self.stops[s.stop_id] = s
            self.routes.append(route)

    def _stops_along(self, rid, waypoints) -> list[Stop]:
        pts = []
        for (a_lat, a_lon), (b_lat, b_lon) in zip(waypoints, waypoints[1:]):
            d = haversine_m(a_lat, a_lon, b_lat, b_lon)
            n = max(1, int(d / self.rng.uniform(380, 620)))
            for k in range(n):
                t = k / n
                pts.append((a_lat + (b_lat - a_lat) * t, a_lon + (b_lon - a_lon) * t))
        pts.append(waypoints[-1])
        pts = pts[:40]
        stops = []
        for j, (lat, lon) in enumerate(pts):
            lat += self.rng.gauss(0, 0.0004)
            lon += self.rng.gauss(0, 0.0006)
            z = zone_of(lat, lon)
            p_bn = {"center": 0.18, "middle": 0.10, "outer": 0.04}[z]
            bn = self.rng.uniform(0.3, 0.9) if self.rng.random() < p_bn else 0.0
            stops.append(Stop(f"{rid}_S{j + 1:02d}", self._stop_name(), lat, lon, z, bn))
        return stops

    @staticmethod
    def _sched_segments(stops) -> list[int]:
        seg = [0]
        for a, b in zip(stops, stops[1:]):
            d = haversine_m(a.lat, a.lon, b.lat, b.lon) * 1.25  # извилистость улиц
            v = SCHED_SPEED_KMH[b.zone] / 3.6
            seg.append(int(round(d / v)) + DWELL_SEC)
        return seg

    def _build_trips(self) -> list[Trip]:
        trips = []
        for r in self.routes:
            for direction in (0, 1):
                t = 5 * 3600 + 30 * 60 + self.rng.randint(0, 600)
                n, veh = 0, 0
                cum = []
                acc = 0
                for s in r.seg_sched_sec[direction]:
                    acc += s
                    cum.append(acc)
                while t < 23 * 3600 + 30 * 60:
                    n += 1
                    veh = (veh % 25) + 1
                    trips.append(Trip(
                        trip_id=f"{r.route_id}_{direction}_{n:03d}",
                        route_id=r.route_id,
                        direction_id=direction,
                        start_sec=t,
                        vehicle_id=f"{r.route_id}-{direction}{veh:02d}",
                        sched=[t + c for c in cum],
                    ))
                    t += headway_min(r.base_headway_min, t // 3600) * 60
        trips.sort(key=lambda x: x.start_sec)
        return trips

    # ---------- экспорт GTFS ----------
    def write_gtfs(self, out_dir: str):
        os.makedirs(out_dir, exist_ok=True)

        def w(name, header, rows):
            with open(os.path.join(out_dir, name), "w", newline="", encoding="utf-8") as f:
                cw = csv.writer(f)
                cw.writerow(header)
                cw.writerows(rows)

        fmt = lambda s: f"{s // 3600:02d}:{s % 3600 // 60:02d}:{s % 60:02d}"
        w("agency.txt", ["agency_id", "agency_name", "agency_url", "agency_timezone"],
          [["CITY", "Городской транспорт (синтетика)", "https://example.org", TZ]])
        w("routes.txt", ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"],
          [[r.route_id, "CITY", r.short_name,
            f"{r.stops[0][0].name.split(',')[0]} — {r.stops[0][-1].name.split(',')[0]}", r.route_type]
           for r in self.routes])
        w("stops.txt", ["stop_id", "stop_name", "stop_lat", "stop_lon", "zone_id", "route_id", "direction_id"],
          [[s.stop_id, s.name, f"{s.lat:.6f}", f"{s.lon:.6f}", s.zone, r.route_id, d]
           for r in self.routes for d in (0, 1) for s in r.stops[d]])
        w("calendar.txt", ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday",
                           "saturday", "sunday", "start_date", "end_date"],
          [["ALL", 1, 1, 1, 1, 1, 1, 1, "20260101", "20271231"]])
        w("trips.txt", ["route_id", "service_id", "trip_id", "direction_id", "vehicle_id"],
          [[t.route_id, "ALL", t.trip_id, t.direction_id, t.vehicle_id] for t in self.trips])
        rows = []
        for t in self.trips:
            stops = self.route_by_id[t.route_id].stops[t.direction_id]
            for seq, (s, sec) in enumerate(zip(stops, t.sched), start=1):
                rows.append([t.trip_id, fmt(sec), fmt(sec), s.stop_id, seq])
        w("stop_times.txt", ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"], rows)
        return {"routes": len(self.routes), "stops": len(self.stops), "trips": len(self.trips),
                "stop_times": len(rows)}
