"""
FleetLogix - Pipeline ETL Automático
Extrae de PostgreSQL, Transforma y Carga en Snowflake
Ejecución diaria automatizada
"""

import os
import psycopg2
import snowflake.connector
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import schedule
import time
import json
from typing import Dict

from dotenv import load_dotenv

# =====================================================
# Cargar variables de entorno (.env)
# =====================================================

load_dotenv()

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("etl_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

# =====================================================
# Configuración de conexiones
# =====================================================

POSTGRES_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "fleetlogix"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
    "port": int(os.getenv("DB_PORT", "5432")),
}

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER", "your_user"),
    "password": os.getenv("SNOWFLAKE_PASSWORD", "your_password"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT", "your_account"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "FLEETLOGIX_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "FLEETLOGIX_DW"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
}


class FleetLogixETL:
    def __init__(self):
        self.pg_conn = None
        self.sf_conn = None
        self.batch_id = int(datetime.now().timestamp())
        self.metrics: Dict[str, int] = {
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "errors": 0,
        }

    # ---------------------------------------------
    # Conexiones
    # ---------------------------------------------
    def connect_databases(self):
        """Establecer conexiones con PostgreSQL y Snowflake"""
        try:
            # PostgreSQL
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            logging.info("Conectado a PostgreSQL")

            # Snowflake
            self.sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            logging.info("Conectado a Snowflake")

            return True
        except Exception as e:
            logging.error(f"Error en conexión: {e}")
            self.metrics["errors"] += 1
            return False

    # ---------------------------------------------
    # Extracción
    # ---------------------------------------------
    def extract_daily_data(self) -> pd.DataFrame:
        """Extraer datos completos desde PostgreSQL (carga histórica inicial)"""
        logging.info("Iniciando extracción de datos...")

        # NOTA: En el enunciado se habla de "día anterior", pero dado que la base
        # ya está totalmente poblada y queremos demostrar la carga al DWH,
        # extraemos TODAS las entregas completadas.
        query = """
        SELECT
            d.delivery_id,
            d.trip_id,
            d.tracking_number,
            d.package_weight_kg,
            d.delivery_status,
            d.scheduled_datetime,
            d.delivered_datetime,
            d.recipient_signature,

            t.vehicle_id,
            t.driver_id,
            t.route_id,
            t.departure_datetime,
            t.arrival_datetime,
            t.fuel_consumed_liters,

            r.distance_km,
            r.toll_cost,
            r.destination_city,
            d.customer_name
        FROM deliveries d
        JOIN trips t ON d.trip_id = t.trip_id
        JOIN routes r ON t.route_id = r.route_id
        WHERE d.delivered_datetime IS NOT NULL;
        """

        try:
            df = pd.read_sql(query, self.pg_conn)
            self.metrics["records_extracted"] = len(df)
            logging.info(f"Extraídos {len(df)} registros")
            return df
        except Exception as e:
            logging.error(f"Error en extracción: {e}")
            self.metrics["errors"] += 1
            return pd.DataFrame()

    # ---------------------------------------------
    # Transformación
    # ---------------------------------------------
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformar datos para el modelo dimensional"""
        logging.info("Iniciando transformación de datos...")

        try:
            # Asegurar tipos datetime
            df["scheduled_datetime"] = pd.to_datetime(df["scheduled_datetime"])
            df["delivered_datetime"] = pd.to_datetime(df["delivered_datetime"])
            df["departure_datetime"] = pd.to_datetime(df["departure_datetime"])
            df["arrival_datetime"] = pd.to_datetime(df["arrival_datetime"])

            # Calcular métricas
            df["delivery_time_minutes"] = (
                (df["delivered_datetime"] - df["scheduled_datetime"]).dt.total_seconds()
                / 60
            ).round(2)

            df["delay_minutes"] = df["delivery_time_minutes"].apply(
                lambda x: max(0, x) if pd.notnull(x) else 0
            )

            df["is_on_time"] = df["delay_minutes"] <= 30

            # Calcular duración de viaje en horas
            df["trip_duration_hours"] = (
                (df["arrival_datetime"] - df["departure_datetime"]).dt.total_seconds()
                / 3600
            ).round(2)

            # Evitar divisiones por cero
            df["trip_duration_hours"] = df["trip_duration_hours"].replace(0, 0.1)

            # Agrupar entregas por trip para calcular entregas/hora
            deliveries_per_trip = df.groupby("trip_id").size()
            df["deliveries_in_trip"] = df["trip_id"].map(deliveries_per_trip)
            df["deliveries_per_hour"] = (
                df["deliveries_in_trip"] / df["trip_duration_hours"]
            ).round(2)

            # Eficiencia de combustible
            df["fuel_efficiency_km_per_liter"] = np.where(
                df["fuel_consumed_liters"] > 0,
                (df["distance_km"] / df["fuel_consumed_liters"]).round(2),
                np.nan,
            )

            # Costo estimado por entrega
            df["cost_per_delivery"] = (
                (df["fuel_consumed_liters"] * 5000 + df["toll_cost"])
                / df["deliveries_in_trip"]
            ).round(2)

            # Revenue estimado (ejemplo: $20,000 base + $500 por kg)
            df["revenue_per_delivery"] = (20000 + df["package_weight_kg"] * 500).round(
                2
            )

            # Validaciones de calidad
            df = df[df["delivery_time_minutes"] >= 0]
            df = df[(df["package_weight_kg"] > 0) & (df["package_weight_kg"] < 10000)]

            # Manejar cambios históricos (SCD Type 2 para conductor/vehículo)
            df["valid_from"] = df["scheduled_datetime"].dt.date
            # OJO: 9999-12-31 revienta pandas, usamos 2099-12-31 (misma idea)
            df["valid_to"] = pd.to_datetime("2099-12-31")
            df["is_current"] = True

            self.metrics["records_transformed"] = len(df)
            logging.info(f"Transformados {len(df)} registros")

            return df

        except Exception as e:
            logging.error(f"Error en transformación: {e}")
            self.metrics["errors"] += 1
            return pd.DataFrame()

    # ---------------------------------------------
    # Carga de dimensiones
    # ---------------------------------------------
    def load_dimensions(self, df: pd.DataFrame):
        """Cargar o actualizar dimensiones en Snowflake"""
        logging.info("Cargando dimensiones...")

        cursor = self.sf_conn.cursor()

        try:
            # dim_customer (nuevos clientes)
            customers = df[["customer_name", "destination_city"]].drop_duplicates()

            for _, row in customers.iterrows():
                customer_name = row["customer_name"]
                destination_city = row["destination_city"]

                cursor.execute(
                    """
                    MERGE INTO dim_customer c
                    USING (SELECT %s AS customer_name) s
                    ON c.customer_name = s.customer_name
                    WHEN NOT MATCHED THEN
                        INSERT (
                            customer_name,
                            customer_type,
                            city,
                            first_delivery_date,
                            total_deliveries,
                            customer_category
                        )
                        VALUES (
                            %s,
                            'Individual',
                            %s,
                            CURRENT_DATE(),
                            0,
                            'Regular'
                        )
                    """,
                    (customer_name, customer_name, destination_city),
                )

            # Actualizar dimensiones SCD Type 2 si hay cambios
            # (Ejemplo simplificado para dim_driver)
            # Para no romper el flujo si no existe staging_daily_load,
            # dejamos la sentencia como concepto, pero la "apagamos".
            cursor.execute(
                """
                UPDATE dim_driver 
                SET valid_to = CURRENT_DATE() - 1, 
                    is_current = FALSE
                WHERE driver_id IN (
                    SELECT DISTINCT driver_id 
                    FROM dim_driver
                )
                AND is_current = TRUE
                AND 1 = 0
                """
            )

            self.sf_conn.commit()
            logging.info("Dimensiones actualizadas")

        except Exception as e:
            logging.error(f"Error cargando dimensiones: {e}")
            self.sf_conn.rollback()
            self.metrics["errors"] += 1

        finally:
            cursor.close()

    # ---------------------------------------------
    # Carga de hechos
    # ---------------------------------------------
    def load_facts(self, df: pd.DataFrame):
        """Cargar hechos en Snowflake"""
        logging.info("Cargando tabla de hechos...")

        cursor = self.sf_conn.cursor()

        try:
            fact_data = []

            for _, row in df.iterrows():
                date_key = int(row["scheduled_datetime"].strftime("%Y%m%d"))
                scheduled_time_key = row["scheduled_datetime"].hour * 100
                delivered_time_key = row["delivered_datetime"].hour * 100

                fact_data.append(
                    (
                        date_key,
                        scheduled_time_key,
                        delivered_time_key,
                        row["vehicle_id"],  # Simplificado, debería buscar vehicle_key
                        row["driver_id"],  # Simplificado, debería buscar driver_key
                        row["route_id"],  # Simplificado, debería buscar route_key
                        1,  # customer_key placeholder
                        row["delivery_id"],
                        row["trip_id"],
                        row["tracking_number"],
                        float(row["package_weight_kg"]),
                        float(row["distance_km"]),
                        float(row["fuel_consumed_liters"]),
                        float(row["delivery_time_minutes"]),
                        float(row["delay_minutes"]),
                        float(row["deliveries_per_hour"]),
                        float(row["fuel_efficiency_km_per_liter"])
                        if not pd.isnull(row["fuel_efficiency_km_per_liter"])
                        else None,
                        float(row["cost_per_delivery"]),
                        float(row["revenue_per_delivery"]),
                        bool(row["is_on_time"]),
                        False,  # is_damaged
                        bool(row["recipient_signature"]),
                        row["delivery_status"],
                        self.batch_id,
                    )
                )

            if not fact_data:
                logging.warning("No hay registros para cargar en fact_deliveries")
                return

            cursor.executemany(
                """
                INSERT INTO fact_deliveries (
                    date_key, scheduled_time_key, delivered_time_key,
                    vehicle_key, driver_key, route_key, customer_key,
                    delivery_id, trip_id, tracking_number,
                    package_weight_kg, distance_km, fuel_consumed_liters,
                    delivery_time_minutes, delay_minutes, deliveries_per_hour,
                    fuel_efficiency_km_per_liter, cost_per_delivery, revenue_per_delivery,
                    is_on_time, is_damaged, has_signature, delivery_status,
                    etl_batch_id
                )
                VALUES (
                    %s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,%s
                )
                """,
                fact_data,
            )

            self.sf_conn.commit()
            self.metrics["records_loaded"] = len(fact_data)
            logging.info(f"Cargados {len(fact_data)} registros en fact_deliveries")

        except Exception as e:
            logging.error(f"Error cargando hechos: {e}")
            self.sf_conn.rollback()
            self.metrics["errors"] += 1

        finally:
            cursor.close()

    # ---------------------------------------------
    # Totales diarios (TO DO original)
    # ---------------------------------------------
    def _calculate_daily_totals(self):
        """Pre-calcular totales para reportes rápidos"""
        cursor = self.sf_conn.cursor()

        try:
            # Crear tabla de totales si no existe
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS fact_daily_metrics (
                    date_key INT,
                    total_deliveries INT,
                    on_time_deliveries INT,
                    avg_delay_minutes DECIMAL(10,2),
                    total_revenue DECIMAL(18,2),
                    total_fuel_liters DECIMAL(18,2),
                    etl_batch_id INT,
                    etl_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """
            )

            # Insertar totales del batch actual
            cursor.execute(
                """
                INSERT INTO fact_daily_metrics (
                    date_key,
                    total_deliveries,
                    on_time_deliveries,
                    avg_delay_minutes,
                    total_revenue,
                    total_fuel_liters,
                    etl_batch_id
                )
                SELECT
                    date_key,
                    COUNT(*) AS total_deliveries,
                    SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) AS on_time_deliveries,
                    AVG(delay_minutes) AS avg_delay_minutes,
                    SUM(revenue_per_delivery) AS total_revenue,
                    SUM(fuel_consumed_liters) AS total_fuel_liters,
                    %s AS etl_batch_id
                FROM fact_deliveries
                WHERE etl_batch_id = %s
                GROUP BY date_key
                """,
                (self.batch_id, self.batch_id),
            )

            self.sf_conn.commit()
            logging.info("Totales diarios calculados")

        except Exception as e:
            logging.error(f"Error calculando totales: {e}")
            self.metrics["errors"] += 1
            self.sf_conn.rollback()

        finally:
            cursor.close()

    # ---------------------------------------------
    # Cerrar conexiones
    # ---------------------------------------------
    def close_connections(self):
        """Cerrar conexiones a bases de datos"""
        if self.pg_conn:
            self.pg_conn.close()
        if self.sf_conn:
            self.sf_conn.close()
        logging.info("Conexiones cerradas")

    # ---------------------------------------------
    # Orquestación ETL
    # ---------------------------------------------
    def run_etl(self):
        """Ejecutar pipeline ETL completo"""
        start_time = datetime.now()
        logging.info(f"Iniciando ETL - Batch ID: {self.batch_id}")

        try:
            if not self.connect_databases():
                logging.error("No se pudieron establecer las conexiones.")
                return

            df = self.extract_daily_data()
            if not df.empty:
                df_transformed = self.transform_data(df)
                if not df_transformed.empty:
                    self.load_dimensions(df_transformed)
                    self.load_facts(df_transformed)

            self._calculate_daily_totals()
            self.close_connections()

            duration = (datetime.now() - start_time).total_seconds()
            logging.info(f"ETL completado en {duration:.2f} segundos")
            logging.info(f"Métricas: {json.dumps(self.metrics, indent=2)}")

        except Exception as e:
            logging.error(f"Error fatal en ETL: {e}")
            self.metrics["errors"] += 1
            self.close_connections()


# =====================================================
# Scheduler / main (estructura original)
# =====================================================


def job():
    """Función para programar con schedule"""
    etl = FleetLogixETL()
    etl.run_etl()


def main():
    """Función principal - Automatización diaria"""
    logging.info("Pipeline ETL FleetLogix iniciado")

    # Programar ejecución diaria a las 2:00 AM
    schedule.every().day.at("02:00").do(job)

    logging.info("ETL programado para ejecutarse diariamente a las 2:00 AM")
    logging.info("Presiona Ctrl+C para detener")

    # Ejecutar una vez al inicio (para pruebas)
    job()

    # Loop infinito esperando la hora programada
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
