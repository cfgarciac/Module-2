# Scripts/01_data_generation.py
"""
FleetLogix - Avance 1
Genera datos sintéticos y consistentes para TODAS las tablas:
- vehicles (200)
- drivers (400)
- routes (50)
- trips (100,000)
- deliveries (400,000)
- maintenance (5,000)

Características:
- Lee credenciales desde .env (en la raíz del repo)
- Siempre limpia (TRUNCATE … RESTART IDENTITY CASCADE) antes de insertar
- Semilla fija para reproducibilidad
- Horarios 06–22 con picos 08–10 y 14–16
- Consumo de combustible por tipo de vehículo
- Pesos de paquetes con distribución lognormal truncada
- Mantenimiento cada ~10.000 km (con leve ruido)
- Validaciones básicas y resumen final

Ejecutar (desde la raíz):
    python Scripts\01_data_generation.py
"""

import os
import json
import logging
import random
import string
from datetime import datetime, timedelta

import numpy as np
import psycopg2
from dotenv import load_dotenv
from faker import Faker
from psycopg2.extras import execute_batch

# --------------------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------------------
os.makedirs("logs", exist_ok=True)
STAMP = datetime.now().strftime("%Y%m%d_%H%M")
LOG_PATH = f"logs/data_load_{STAMP}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()],
)

# --------------------------------------------------------------------------------------
# Semillas (reproducibilidad)
# --------------------------------------------------------------------------------------
SEED = 42
random.seed(SEED)
np.random.seed(SEED)
Faker.seed(SEED)
fake = Faker("es_CO")

# --------------------------------------------------------------------------------------
# Config DB desde .env
# --------------------------------------------------------------------------------------
load_dotenv()
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "fleetlogix"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
    "port": int(os.getenv("DB_PORT", "5432")),
}

# --------------------------------------------------------------------------------------
# Catálogos / Parámetros
# --------------------------------------------------------------------------------------
CITIES = ["Bogotá", "Medellín", "Villavicencio", "Barranquilla", "Bucaramanga"]

# Distancias aproximadas por carretera (km) – bidireccional
DISTANCES = {
    ("Bogotá", "Medellín"): 443,
    ("Bogotá", "Villavicencio"): 123,
    ("Bogotá", "Barranquilla"): 1001,
    ("Bogotá", "Bucaramanga"): 409,
    ("Medellín", "Bucaramanga"): 388,
    ("Medellín", "Barranquilla"): 703,
    ("Medellín", "Villavicencio"): 518,
    ("Villavicencio", "Barranquilla"): 1116,
    ("Bucaramanga", "Barranquilla"): 584,
    ("Villavicencio", "Bucaramanga"): 518,
}

# Vehículos: (tipo, cap_min, cap_max, combustible, rango_consumo_Lx100km)
VEHICLE_TYPES = [
    ("Camión Grande", 12000, 18000, "diesel", (25, 35)),
    ("Camión Mediano", 6000, 9000, "diesel", (18, 26)),
    ("Van", 1000, 1500, "gasolina", (9, 15)),
    ("Motocicleta", 50, 150, "gasolina", (2, 4)),
]

MAINTENANCE_TYPES = [
    ("Cambio de aceite", 150000, 30),
    ("Revisión de frenos", 250000, 60),
    ("Cambio de llantas", 450000, 90),
    ("Mantenimiento general", 350000, 45),
    ("Revisión de motor", 500000, 60),
    ("Alineación y balanceo", 180000, 30),
]


# --------------------------------------------------------------------------------------
# Clase principal
# --------------------------------------------------------------------------------------
class DataGenerator:
    def __init__(self, db_conf: dict):
        self.db_conf = db_conf
        self.conn = None
        self.cur = None
        self.counters = {
            "vehicles": 0,
            "drivers": 0,
            "routes": 0,
            "trips": 0,
            "deliveries": 0,
            "maintenance": 0,
        }

    # ---------------- Infra ----------------
    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.db_conf)
            self.cur = self.conn.cursor()
            logging.info("✅ Conexión a PostgreSQL establecida.")
        except Exception as e:
            logging.exception(f"❌ Error conectando a PostgreSQL: {e}")
            raise

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        logging.info("🔚 Conexión cerrada.")

    # ---------------- Helpers ----------------
    @staticmethod
    def _gen_plate() -> str:
        letters = "".join(random.choices(string.ascii_uppercase, k=3))
        digits = f"{random.randint(100, 999)}"
        return f"{letters}{digits}"

    @staticmethod
    def _toll_cost_from_distance(km: float) -> int:
        return int(km // 100) * 15000  # 15.000 por cada 100km aprox.

    def _get_distance(self, a: str, b: str) -> float:
        if a == b:
            return 0.0
        key = tuple(sorted((a, b)))
        return float(DISTANCES.get(key, 500.0))

    @staticmethod
    def _hourly_distribution_full_24():
        """Vector de 24 posiciones con prob. 06–22 y picos 08–10 y 14–16."""
        hours = list(range(6, 23))
        weights = {
            6: 4,
            7: 6,
            8: 10,
            9: 10,
            10: 8,
            11: 6,
            12: 5,
            13: 5,
            14: 9,
            15: 9,
            16: 8,
            17: 6,
            18: 6,
            19: 4,
            20: 2,
            21: 2,
            22: 1,
        }
        probs = np.array([weights.get(h, 1) for h in hours], dtype=float)
        probs = probs / probs.sum()
        full = np.zeros(24, dtype=float)
        for i, h in enumerate(hours):
            full[h] = probs[i]
        return full

    @staticmethod
    def _distribute_weight_lognormal(total_weight: float, num_packages: int):
        """Distribuye total_weight en num_packages usando lognormal truncada."""
        raw = np.random.lognormal(mean=2.5, sigma=0.6, size=num_packages)
        raw = np.clip(raw, 0.5, None)  # mínimo 0.5 kg
        scaled = raw / raw.sum() * (total_weight * 0.95)  # deja 5% como colchón
        return scaled

    # ---------------- Limpieza (TRUNCATE) ----------------
    def truncate_all(self):
        """
        Limpia TODO antes de generar (orden seguro).
        CASCADE por si hay dependencias.
        """
        logging.info("🧹 TRUNCATE de todas las tablas…")
        self.cur.execute("""
            TRUNCATE TABLE deliveries, trips, maintenance, routes, drivers, vehicles
            RESTART IDENTITY CASCADE;
        """)
        self.conn.commit()
        logging.info("✔ Tablas truncadas y secuencias reiniciadas.")

    # ---------------- Generadores: Maestras ----------------
    def generate_vehicles(self, count: int = 200):
        logging.info(f"Generando {count} vehículos…")
        rows = []
        for _ in range(count):
            vtype, cap_min, cap_max, fuel, _cons = random.choice(VEHICLE_TYPES)
            capacity = random.randint(cap_min, cap_max)
            license_plate = self._gen_plate()
            acquisition_date = fake.date_between(start_date="-5y", end_date="-1m")
            status = random.choices(["active", "maintenance"], weights=[90, 10], k=1)[0]
            rows.append(
                (license_plate, vtype, capacity, fuel, acquisition_date, status)
            )
        q = """
            INSERT INTO vehicles
                (license_plate, vehicle_type, capacity_kg, fuel_type, acquisition_date, status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        execute_batch(self.cur, q, rows, page_size=500)
        self.conn.commit()
        self.counters["vehicles"] = count
        logging.info(f"✔ {count} vehículos insertados.")

    def generate_drivers(self, count: int = 400):
        logging.info(f"Generando {count} conductores…")
        rows = []
        for i in range(count):
            employee_code = f"EMP{str(i + 1).zfill(4)}"
            first_name = fake.first_name()
            last_name = fake.last_name()
            license_number = f"{random.randint(10**9, 10**10 - 1)}"  # 10 dígitos
            license_expiry = fake.date_between(start_date="-1m", end_date="+3y")
            phone = f"3{random.randint(100000000, 999999999)}"
            hire_date = fake.date_between(start_date="-5y", end_date="-1w")
            status = random.choices(["active", "inactive"], weights=[95, 5], k=1)[0]
            rows.append(
                (
                    employee_code,
                    first_name,
                    last_name,
                    license_number,
                    license_expiry,
                    phone,
                    hire_date,
                    status,
                )
            )
        q = """
            INSERT INTO drivers
                (employee_code, first_name, last_name, license_number,
                 license_expiry, phone, hire_date, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_batch(self.cur, q, rows, page_size=500)
        self.conn.commit()
        self.counters["drivers"] = count
        logging.info(f"✔ {count} conductores insertados.")

    def generate_routes(self, count: int = 50):
        """
        Inserta EXACTAMENTE 'count' rutas.
        - Rutas simples con 2 variantes por par ordenado
        - Completa con rutas compuestas si faltan
        """
        logging.info(f"Generando {count} rutas…")
        routes = []

        def add_route(origin, destination, km):
            avg_speed = random.uniform(60, 80)
            duration = (km / avg_speed) + 1.0
            toll = self._toll_cost_from_distance(km)
            rcode = f"R{str(len(routes) + 1).zfill(3)}"
            routes.append(
                (rcode, origin, destination, round(km, 2), round(duration, 2), toll)
            )

        # 1) Simples
        VARIANTS_PER_PAIR = 2
        for o in CITIES:
            for d in CITIES:
                if o == d:
                    continue
                base_km = self._get_distance(o, d)
                if base_km <= 0:
                    continue
                for _ in range(VARIANTS_PER_PAIR):
                    if len(routes) >= count:
                        break
                    km = base_km * random.uniform(0.95, 1.05)
                    add_route(o, d, km)
            if len(routes) >= count:
                break

        # 2) Compuestas A-B-C
        if len(routes) < count:
            for a in CITIES:
                for b in CITIES:
                    if b == a:
                        continue
                    for c in CITIES:
                        if c in (a, b):
                            continue
                        if len(routes) >= count:
                            break
                        km = (
                            self._get_distance(a, b) + self._get_distance(b, c)
                        ) * random.uniform(0.97, 1.03)
                        add_route(a, c, km)
                    if len(routes) >= count:
                        break
                if len(routes) >= count:
                    break

        # 3) Compuestas A-B-C-D
        if len(routes) < count:
            for a in CITIES:
                for b in CITIES:
                    if b == a:
                        continue
                    for c in CITIES:
                        if c in (a, b):
                            continue
                        for d in CITIES:
                            if d in (a, b, c):
                                continue
                            if len(routes) >= count:
                                break
                            km = (
                                self._get_distance(a, b)
                                + self._get_distance(b, c)
                                + self._get_distance(c, d)
                            ) * random.uniform(0.97, 1.03)
                            add_route(a, d, km)
                        if len(routes) >= count:
                            break
                    if len(routes) >= count:
                        break
                if len(routes) >= count:
                    break

        routes = routes[:count]
        q = """
            INSERT INTO routes
                (route_code, origin_city, destination_city, distance_km,
                 estimated_duration_hours, toll_cost)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        execute_batch(self.cur, q, routes, page_size=200)
        self.conn.commit()
        self.counters["routes"] = len(routes)
        logging.info(f"✔ {len(routes)} rutas insertadas (solicitadas: {count}).")

    # ---------------- Generadores: Transaccionales ----------------
    def generate_trips(self, count: int = 100_000):
        """
        Genera viajes a lo largo de ~2 años.
        - Hora de salida según distribución por horas (06–22 con picos)
        - Duración ~ km/70 + 1h (ruido)
        - Consumo por tipo de vehículo (L/100km)
        """
        logging.info(f"Generando {count} trips…")

        # Catálogos desde DB
        self.cur.execute(
            "SELECT vehicle_id, capacity_kg, vehicle_type FROM vehicles WHERE status='active'"
        )
        vehicles = self.cur.fetchall()

        self.cur.execute("SELECT driver_id FROM drivers WHERE status='active'")
        drivers = [d[0] for d in self.cur.fetchall()]

        self.cur.execute(
            "SELECT route_id, distance_km, estimated_duration_hours FROM routes"
        )
        routes = [
            (rid, float(dist), float(estim))
            for (rid, dist, estim) in self.cur.fetchall()
        ]

        if not (vehicles and drivers and routes):
            raise RuntimeError("Faltan datos base (vehicles/drivers/routes).")

        start_date = datetime.now() - timedelta(days=730)  # ~2 años
        current_dt = start_date
        p_hour = self._hourly_distribution_full_24()

        cons_ranges = {
            "Camión Grande": (25, 35),
            "Camión Mediano": (18, 26),
            "Van": (9, 15),
            "Motocicleta": (2, 4),
        }

        rows = []
        minutes_step = int(2 * 365 * 24 * 60 / count)  # repartir uniformemente

        for _ in range(count):
            vehicle_id, capacity, vtype = random.choice(vehicles)
            driver_id = random.choice(drivers)
            route_id, distance, est_dur = random.choice(routes)
            distance = float(distance)

            # hora de salida con distribución
            hour = np.random.choice(range(24), p=p_hour)
            if hour < 6:
                hour = 6
            departure = current_dt.replace(
                hour=int(hour), minute=random.randint(0, 59), second=0, microsecond=0
            )

            # duración real
            base_dur = (distance / 70.0) + 1.0
            realism = random.uniform(0.85, 1.15)
            actual_dur = max(est_dur, base_dur) * realism
            arrival = departure + timedelta(hours=actual_dur)

            # consumo
            l100 = random.uniform(*cons_ranges.get(vtype, (10, 20)))
            fuel = distance * (l100 / 100.0) * random.uniform(0.95, 1.05)

            # peso total 40–90% de capacidad
            total_weight = float(capacity) * random.uniform(0.4, 0.9)

            status = "completed" if arrival < datetime.now() else "in_progress"

            rows.append(
                (
                    vehicle_id,
                    driver_id,
                    route_id,
                    departure,
                    arrival if status == "completed" else None,
                    round(fuel, 2),
                    round(total_weight, 2),
                    status,
                )
            )

            current_dt += timedelta(minutes=minutes_step)

        q = """
            INSERT INTO trips
                (vehicle_id, driver_id, route_id, departure_datetime,
                 arrival_datetime, fuel_consumed_liters, total_weight_kg, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        batch = 2000
        for i in range(0, len(rows), batch):
            execute_batch(self.cur, q, rows[i : i + batch], page_size=500)
            self.conn.commit()
        self.counters["trips"] = count
        logging.info(f"✔ {count} trips insertados.")

    def generate_deliveries(self, target: int = 400_000):
        """
        Genera EXACTAMENTE 'target' entregas.
        - Por trip: 2–6 entregas, con 4 como lo más probable (ajustado para sumar 'target').
        - Si el trip está 'completed', se programan y marcan delivered (10% con retraso).
        - Si está 'in_progress', quedan 'pending'.
        """
        logging.info(f"Generando {target} deliveries…")

        # Traer trips con su info necesaria
        self.cur.execute("""
            SELECT t.trip_id,
                   t.departure_datetime,
                   t.arrival_datetime,
                   t.total_weight_kg,
                   r.destination_city
            FROM trips t
            JOIN routes r ON r.route_id = t.route_id
            WHERE t.status IN ('completed','in_progress')
            ORDER BY t.trip_id
        """)
        trips = self.cur.fetchall()
        n_trips = len(trips)
        if n_trips == 0:
            raise RuntimeError("No hay trips para generar deliveries.")

        # 1) Muestra inicial 2..6 con 4 como más probable
        choices = np.array([2, 3, 4, 5, 6])
        probs = np.array([0.10, 0.20, 0.40, 0.20, 0.10])  # E[n] = 4.0
        per_trip = np.random.choice(choices, size=n_trips, p=probs)

        # 2) Ajustar para que sum(per_trip) == target sin pasar de 6
        current_total = int(per_trip.sum())
        diff = target - current_total

        if diff > 0:
            # capacidad para subir hasta 6 por trip
            capacity = (6 - per_trip).sum()
            if capacity < diff:
                raise RuntimeError(
                    "No hay capacidad para alcanzar el target con máximo 6 por trip."
                )
            # incrementa secuencialmente evitando pasar de 6
            i = 0
            while diff > 0:
                if per_trip[i] < 6:
                    per_trip[i] += 1
                    diff -= 1
                i = (i + 1) % n_trips

        elif diff < 0:
            # bajar hasta 2 por trip
            need = -diff
            capacity = (per_trip - 2).sum()
            if capacity < need:
                raise RuntimeError(
                    "No es posible reducir hasta el target manteniendo mínimo 2 por trip."
                )
            i = 0
            while need > 0:
                if per_trip[i] > 2:
                    per_trip[i] -= 1
                    need -= 1
                i = (i + 1) % n_trips

        # ahora sum(per_trip) == target y 2 <= per_trip[i] <= 6
        rows = []
        counter = 0

        for (trip_id, departure, arrival, total_weight, dest_city), n in zip(
            trips, per_trip
        ):
            # pesos por entrega (lognormal, escalado al 95% del total_weight)
            total_weight = float(total_weight)
            weights = self._distribute_weight_lognormal(total_weight, int(n))

            # tiempo entre entregas
            if arrival:
                total_hours = max((arrival - departure).total_seconds() / 3600.0, 1.0)
                gap = total_hours / n
            else:
                gap = 0.5  # 30 minutos aprox.

            for i in range(int(n)):
                counter += 1
                tracking = f"FL{datetime.now().year}{str(counter).zfill(8)}"
                customer_name = f"{fake.first_name()} {fake.last_name()}"
                address = f"{fake.street_address()}, {dest_city}"
                pkg_w = round(float(weights[i]), 2)

                scheduled = departure + timedelta(hours=gap * (i + 0.5))

                if arrival:
                    # 90% a tiempo (+/- 30 min), 10% retraso 60–180 min
                    if random.random() < 0.9:
                        delivered = scheduled + timedelta(
                            minutes=random.randint(-30, 30)
                        )
                    else:
                        delivered = scheduled + timedelta(
                            minutes=random.randint(60, 180)
                        )
                    status = "delivered"
                    signature = random.random() < 0.95
                else:
                    delivered = None
                    status = "pending"
                    signature = False

                rows.append(
                    (
                        trip_id,
                        tracking,
                        customer_name,
                        address,
                        pkg_w,
                        scheduled,
                        delivered,
                        status,
                        signature,
                    )
                )

        # Inserción por lotes
        q = """
            INSERT INTO deliveries
              (trip_id, tracking_number, customer_name, delivery_address,
               package_weight_kg, scheduled_datetime, delivered_datetime,
               delivery_status, recipient_signature)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        batch = 2000
        for i in range(0, len(rows), batch):
            execute_batch(self.cur, q, rows[i : i + batch], page_size=500)
            self.conn.commit()

        self.counters["deliveries"] = len(rows)
        logging.info(f"✔ {len(rows)} deliveries insertados (objetivo: {target}).")

    def generate_maintenance(self, target: int = 5000):
        """
        Genera mantenimientos por vehículo cada ~10.000 km (±300).
        Las fechas se distribuyen entre primer y último viaje del vehículo.
        """
        logging.info(f"Generando {target} registros de maintenance…")

        self.cur.execute("""
            SELECT v.vehicle_id,
                   v.vehicle_type,
                   COUNT(t.trip_id) AS trip_count,
                   MIN(t.departure_datetime) AS first_trip,
                   MAX(COALESCE(t.arrival_datetime, t.departure_datetime)) AS last_trip,
                   COALESCE(SUM(r.distance_km), 0) AS total_km
            FROM vehicles v
            LEFT JOIN trips t ON t.vehicle_id = v.vehicle_id
            LEFT JOIN routes r ON r.route_id = t.route_id
            GROUP BY v.vehicle_id, v.vehicle_type
            ORDER BY v.vehicle_id
        """)
        stats = self.cur.fetchall()
        rows = []

        for vehicle_id, vtype, trip_count, first_trip, last_trip, total_km in stats:
            total_km = float(total_km or 0)
            if not first_trip or not last_trip or total_km <= 0:
                continue

            # puntos cada ~10.000 km
            km_cursor = 0.0
            next_threshold = 10000 + random.uniform(-300, 300)

            while (km_cursor + next_threshold) <= total_km and len(rows) < target:
                km_cursor += next_threshold
                next_threshold = 10000 + random.uniform(-300, 300)

                # fecha interpolada entre first_trip y last_trip según % km
                span_days = max((last_trip - first_trip).days, 1)
                frac = km_cursor / total_km
                day_offset = int(frac * span_days)
                m_date = (first_trip + timedelta(days=day_offset)).date()

                maint_type, base_cost, days_next = random.choice(MAINTENANCE_TYPES)
                cost = round(base_cost * random.uniform(0.85, 1.20), 2)
                next_m = m_date + timedelta(days=days_next)
                performed_by = f"{fake.first_name()} {fake.last_name()}"
                desc = f"{maint_type} programado"

                rows.append(
                    (vehicle_id, m_date, maint_type, desc, cost, next_m, performed_by)
                )
                if len(rows) >= target:
                    break

            if len(rows) >= target:
                break

        q = """
            INSERT INTO maintenance
                (vehicle_id, maintenance_date, maintenance_type,
                 description, cost, next_maintenance_date, performed_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        execute_batch(self.cur, q, rows[:target], page_size=500)
        self.conn.commit()
        self.counters["maintenance"] = min(len(rows), target)
        logging.info(
            f"✔ {self.counters['maintenance']} maintenance insertados (objetivo: {target})."
        )

    # ---------------- QA & Resumen ----------------
    def validate_data_quality(self) -> bool:
        logging.info("🔍 Validando calidad de datos…")
        validations = {
            "Trips sin vehículo válido": """
                SELECT COUNT(*) FROM trips t
                LEFT JOIN vehicles v ON v.vehicle_id = t.vehicle_id
                WHERE v.vehicle_id IS NULL
            """,
            "Deliveries sin trip válido": """
                SELECT COUNT(*) FROM deliveries d
                LEFT JOIN trips t ON t.trip_id = d.trip_id
                WHERE t.trip_id IS NULL
            """,
            "arrival <= departure": """
                SELECT COUNT(*) FROM trips
                WHERE arrival_datetime IS NOT NULL
                  AND arrival_datetime <= departure_datetime
            """,
            "Peso excede capacidad": """
                SELECT COUNT(*) FROM trips t
                JOIN vehicles v ON v.vehicle_id = t.vehicle_id
                WHERE t.total_weight_kg > v.capacity_kg
            """,
            "Licencia vencida vs fecha viaje": """
                SELECT COUNT(*) FROM trips t
                JOIN drivers d ON d.driver_id = t.driver_id
                WHERE d.license_expiry IS NOT NULL
                  AND t.departure_datetime::date > d.license_expiry
            """,
            "Entregas sin tracking": """
                SELECT COUNT(*) FROM deliveries
                WHERE tracking_number IS NULL OR tracking_number = ''
            """,
        }
        ok = True
        for desc, sql in validations.items():
            self.cur.execute(sql)
            c = self.cur.fetchone()[0]
            if c > 0:
                logging.warning(f"⚠ {desc}: {c} registros")
                ok = False
            else:
                logging.info(f"✔ {desc}: OK")
        return ok

    def summary(self):
        logging.info("📊 Resumen de tablas:")
        total = 0
        for t in [
            "vehicles",
            "drivers",
            "routes",
            "trips",
            "deliveries",
            "maintenance",
        ]:
            self.cur.execute(f"SELECT COUNT(*) FROM {t}")
            c = self.cur.fetchone()[0]
            logging.info(f"  - {t}: {c:,}")
            total += c
        logging.info(f"TOTAL filas: {total:,}")

        valid = self.validate_data_quality()
        summary = {
            "generation_date": datetime.now().isoformat(),
            "table_counts": self.counters,
            "validations_passed": valid,
        }
        with open("generation_summary.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        logging.info("📝 Resumen guardado en generation_summary.json")


# --------------------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------------------
def main():
    logging.info("FLEETLOGIX – Avance 1 (COMPLETO)")
    gen = DataGenerator(DB_CONFIG)
    try:
        gen.connect()
        gen.truncate_all()

        gen.generate_vehicles(200)
        gen.generate_drivers(400)
        gen.generate_routes(50)

        gen.generate_trips(100_000)
        gen.generate_deliveries(400_000)
        gen.generate_maintenance(5_000)

        gen.summary()
        logging.info(f"✔ Counters: {gen.counters}")
    except Exception:
        logging.exception("❌ Fallo durante la generación; se aplicará rollback.")
        if gen.conn:
            gen.conn.rollback()
        raise
    finally:
        gen.close()


if __name__ == "__main__":
    main()
