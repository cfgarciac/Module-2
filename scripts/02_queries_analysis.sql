/* ===== Q1 – Vehículos activos con su última fecha de mantenimiento ===== */

CREATE OR REPLACE VIEW vw_q1_vehiculos_activos_ult_mant AS
SELECT
    v.vehicle_id,
    v.license_plate,
    v.vehicle_type,
    MAX(m.maintenance_date) AS last_maintenance
FROM vehicles v
LEFT JOIN maintenance m
       ON m.vehicle_id = v.vehicle_id
WHERE v.status = 'active'
GROUP BY v.vehicle_id, v.license_plate, v.vehicle_type
ORDER BY last_maintenance DESC NULLS LAST;

/* Baseline: medir con EXPLAIN (ANALYZE, BUFFERS) */
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM vw_q1_vehiculos_activos_ult_mant;


/* ===== Q2 – Licencias por vencer (30 días) ===== */

CREATE OR REPLACE VIEW vw_q2_licencias_por_vencer AS
SELECT
    d.driver_id,
    CONCAT(d.first_name, ' ', d.last_name) AS driver,
    d.license_number,
    d.license_expiry
FROM drivers d
WHERE d.status = 'active'
  AND d.license_expiry < CURRENT_DATE + INTERVAL '30 days'
ORDER BY d.license_expiry ASC;

/* Medición baseline */
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM vw_q2_licencias_por_vencer;


/* ===== Q3 – Entregas por estado (% del total) ===== */

CREATE OR REPLACE VIEW vw_q3_entregas_por_estado AS
SELECT
    delivery_status,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje
FROM deliveries
GROUP BY delivery_status
ORDER BY total DESC;

/* Medición baseline */
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM vw_q3_entregas_por_estado;


/* ===== Q4 – Promedio de entregas por viaje, por ruta ===== */

CREATE OR REPLACE VIEW vw_q4_avg_entregas_por_ruta AS
WITH por_trip AS (
    SELECT t.trip_id, t.route_id, COUNT(d.delivery_id) AS d_cnt
    FROM trips t
    LEFT JOIN deliveries d ON d.trip_id = t.trip_id
    GROUP BY t.trip_id, t.route_id
)
SELECT
    r.route_id,
    r.route_code,
    r.origin_city,
    r.destination_city,
    ROUND(AVG(p.d_cnt)::numeric, 2) AS avg_deliveries_per_trip
FROM por_trip p
JOIN routes r ON r.route_id = p.route_id
GROUP BY r.route_id, r.route_code, r.origin_city, r.destination_city
ORDER BY avg_deliveries_per_trip DESC;

-- Medición baseline
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM vw_q4_avg_entregas_por_ruta;


/* ===== Q5 – Promedio de entregas por conductor (últimos 6 meses) ===== */

CREATE OR REPLACE VIEW vw_q5_prom_entregas_conductor_6m AS
WITH recent AS (
    SELECT t.trip_id, t.driver_id
    FROM trips t
    WHERE t.departure_datetime >= (CURRENT_DATE - INTERVAL '6 months')
),
del_counts AS (
    SELECT r.driver_id, COUNT(*) AS deliveries_6m
    FROM recent r
    JOIN deliveries d ON d.trip_id = r.trip_id
    GROUP BY r.driver_id
),
trip_counts AS (
    SELECT driver_id, COUNT(*) AS trips_6m
    FROM recent
    GROUP BY driver_id
)
SELECT
    dr.driver_id,
    CONCAT(dr.first_name, ' ', dr.last_name) AS driver,
    COALESCE(dc.deliveries_6m, 0) AS deliveries_6m,
    COALESCE(tc.trips_6m, 0) AS trips_6m,
    COALESCE((dc.deliveries_6m::numeric / NULLIF(tc.trips_6m, 0)), 0)::numeric(10,2) AS avg_deliveries_per_trip_6m
FROM drivers dr
LEFT JOIN del_counts dc ON dc.driver_id = dr.driver_id
LEFT JOIN trip_counts tc ON tc.driver_id = dr.driver_id
WHERE dr.status = 'active'
ORDER BY avg_deliveries_per_trip_6m DESC, driver;

/* Medición baseline */
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM vw_q5_prom_entregas_conductor_6m;


/* ===== Q6 – Consumo promedio (L/100km) por tipo de vehículo ===== */

CREATE OR REPLACE VIEW vw_q6_consumo_promedio_l100km AS
SELECT
    v.vehicle_type,
    ROUND(AVG((t.fuel_consumed_liters / NULLIF(r.distance_km, 0)) * 100)::numeric, 2) AS avg_l_per_100km
FROM trips t
JOIN vehicles v ON v.vehicle_id = t.vehicle_id
JOIN routes   r ON r.route_id   = t.route_id
WHERE t.status = 'completed'
GROUP BY v.vehicle_type
ORDER BY avg_l_per_100km;

/* Medición baseline */
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM vw_q6_consumo_promedio_l100km;


/* ===== Q7 – Top 10 rutas por entregas/hora ===== */

CREATE OR REPLACE VIEW vw_q7_top_rutas_entregas_por_hora AS
WITH por_trip AS (
    SELECT
        t.trip_id,
        t.route_id,
        GREATEST(EXTRACT(EPOCH FROM (t.arrival_datetime - t.departure_datetime)) / 3600.0, 0.1) AS hours,
        COUNT(d.delivery_id) AS deliveries
    FROM trips t
    LEFT JOIN deliveries d ON d.trip_id = t.trip_id
    GROUP BY t.trip_id, t.route_id, hours
),
por_route AS (
    SELECT
        route_id,
        SUM(deliveries)::numeric / NULLIF(SUM(hours), 0) AS deliveries_per_hour
    FROM por_trip
    GROUP BY route_id
)
SELECT
    r.route_id,
    r.route_code,
    r.origin_city,
    r.destination_city,
    ROUND(pr.deliveries_per_hour, 2) AS deliveries_per_hour
FROM por_route pr
JOIN routes r USING (route_id)
ORDER BY deliveries_per_hour DESC
LIMIT 10;

/* Medición baseline */
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM vw_q7_top_rutas_entregas_por_hora;


/* ===== Q8 – Puntualidad mensual (últimos 12 meses) ===== */

CREATE OR REPLACE VIEW vw_q8_puntualidad_mensual AS
WITH recientes AS (
    SELECT *
    FROM deliveries
    WHERE scheduled_datetime >= (CURRENT_DATE - INTERVAL '12 months')
)
SELECT
    TO_CHAR(DATE_TRUNC('month', scheduled_datetime), 'YYYY-MM') AS mes,
    ROUND(
        100.0 * SUM(
            CASE 
                WHEN delivered_datetime <= scheduled_datetime THEN 1 
                ELSE 0 
            END
        ) / COUNT(*), 
    2) AS porcentaje_a_tiempo,
    COUNT(*) AS total_entregas
FROM recientes
GROUP BY DATE_TRUNC('month', scheduled_datetime)
ORDER BY mes;

/* Medición baseline */
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM vw_q8_puntualidad_mensual;


/* ===== Q9 – Costo de mantenimiento por 1.000 km ===== */
CREATE OR REPLACE VIEW vw_q9_coste_mant_por_1000km AS
WITH trip_km AS (
    SELECT t.vehicle_id, SUM(r.distance_km) AS km_traveled
    FROM trips t
    JOIN routes r ON r.route_id = t.route_id
    GROUP BY t.vehicle_id
),
maint_cost AS (
    SELECT vehicle_id, SUM(cost) AS maintenance_cost
    FROM maintenance
    GROUP BY vehicle_id
)
SELECT
    v.vehicle_id,
    v.license_plate,
    v.vehicle_type,
    COALESCE(m.maintenance_cost, 0) AS maintenance_cost,
    COALESCE(k.km_traveled, 0)      AS km_traveled,
    CASE
        WHEN COALESCE(k.km_traveled,0) > 0
        THEN ROUND( (m.maintenance_cost / (k.km_traveled / 1000.0))::numeric, 2 )
        ELSE NULL
    END AS cost_per_1000_km
FROM vehicles v
LEFT JOIN maint_cost m ON m.vehicle_id = v.vehicle_id
LEFT JOIN trip_km   k ON k.vehicle_id = v.vehicle_id
ORDER BY cost_per_1000_km NULLS LAST;

/* Medición baseline */
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM vw_q9_coste_mant_por_1000km;


/* ===== Q10 – Ranking de eficiencia de conductores ===== */
CREATE OR REPLACE VIEW vw_q10_ranking_eficiencia_conductores AS
WITH metrics AS (
    SELECT
        d.driver_id,
        COUNT(DISTINCT del.delivery_id) AS total_entregas,
        AVG(del.package_weight_kg) AS peso_promedio,
        AVG(t.fuel_consumed_liters / NULLIF(r.distance_km, 0)) AS consumo_relativo,
        ROUND(
            100.0 * SUM(
                CASE WHEN del.delivered_datetime <= del.scheduled_datetime THEN 1 ELSE 0 END
            ) / COUNT(*), 2
        ) AS puntualidad
    FROM drivers d
    JOIN trips t ON t.driver_id = d.driver_id
    JOIN routes r ON r.route_id = t.route_id
    JOIN deliveries del ON del.trip_id = t.trip_id
    WHERE d.status = 'active'
    GROUP BY d.driver_id
)
SELECT
    dr.first_name || ' ' || dr.last_name AS conductor,
    m.total_entregas,
    ROUND(m.peso_promedio, 2) AS peso_promedio,
    ROUND(m.consumo_relativo * 100, 2) AS consumo_por_km,
    m.puntualidad,
    RANK() OVER (ORDER BY (m.puntualidad - m.consumo_relativo) DESC) AS ranking_eficiencia
FROM metrics m
JOIN drivers dr ON dr.driver_id = m.driver_id
ORDER BY ranking_eficiencia
LIMIT 15;

/* Medición baseline */
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM vw_q10_ranking_eficiencia_conductores;


/* ===== Q11 – Histograma horario de entregas ===== */

CREATE OR REPLACE VIEW vw_q11_histograma_horario AS
SELECT
    (EXTRACT(HOUR FROM delivered_datetime)::int / 2) * 2 AS hora_inicio,
    COUNT(*) AS total_entregas,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje
FROM deliveries
WHERE delivered_datetime IS NOT NULL
GROUP BY hora_inicio
ORDER BY hora_inicio;

/* Medición baseline */
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM vw_q11_histograma_horario;


/* ===== Q12 – Ranking por destino (eficiencia compuesta) ===== */
CREATE OR REPLACE VIEW vw_q12_ranking_destinos AS
WITH por_trip AS (
    SELECT
        t.trip_id,
        r.destination_city,
        GREATEST(EXTRACT(EPOCH FROM (t.arrival_datetime - t.departure_datetime)) / 3600.0, 0.1) AS hours,
        t.fuel_consumed_liters / NULLIF(r.distance_km, 0) AS fuel_per_km
    FROM trips t
    JOIN routes r ON r.route_id = t.route_id
),
agg_dest AS (
    SELECT
        pt.destination_city,
        SUM(pt.hours)                                          AS total_hours,
        AVG(pt.fuel_per_km)                                    AS avg_fuel_per_km,
        COUNT(d.delivery_id)                                   AS total_deliveries,
        SUM(CASE WHEN d.delivered_datetime <= d.scheduled_datetime THEN 1 ELSE 0 END) AS deliveries_on_time
    FROM por_trip pt
    LEFT JOIN deliveries d ON d.trip_id = pt.trip_id
    GROUP BY pt.destination_city
),
scores AS (
    SELECT
        destination_city,
        (total_deliveries::numeric / NULLIF(total_hours, 0))                   AS deliveries_per_hour,
        (100.0 * deliveries_on_time::numeric / NULLIF(total_deliveries, 0))    AS punctuality_pct,
        avg_fuel_per_km                                                         AS fuel_per_km
    FROM agg_dest
)
SELECT
    destination_city,
    ROUND(deliveries_per_hour, 2) AS deliveries_per_hour,
    ROUND(punctuality_pct, 2)     AS punctuality_pct,
    ROUND(fuel_per_km * 100, 2)   AS fuel_per_100km,
    RANK() OVER (
        ORDER BY (COALESCE(punctuality_pct,0) + 2 * COALESCE(deliveries_per_hour,0) - 100 * COALESCE(fuel_per_km,0)) DESC
    ) AS efficiency_rank
FROM scores
ORDER BY efficiency_rank;

/* Medición baseline */
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM vw_q12_ranking_destinos;
