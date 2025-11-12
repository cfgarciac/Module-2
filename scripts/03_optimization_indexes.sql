-- =====================================================
-- FLEETLOGIX - ÍNDICES DE OPTIMIZACIÓN (Avance 2)
-- Basados en los planes de ejecución de Q1–Q12
-- Objetivo: mejoras 20%+ donde aplique
-- =====================================================

-- Limpieza previa (idempotente)
DROP INDEX IF EXISTS idx_trips_composite_joins;
DROP INDEX IF EXISTS idx_deliveries_scheduled_datetime;
DROP INDEX IF EXISTS idx_maintenance_vehicle_cost;
DROP INDEX IF EXISTS idx_drivers_status_license;
DROP INDEX IF EXISTS idx_routes_metrics;

-- =====================================================
-- ÍNDICE 1: JOINs frecuentes en trips
-- Beneficia: Q4, Q5, Q6, Q7, Q8, Q10, Q12
-- =====================================================
CREATE INDEX idx_trips_composite_joins
ON trips (vehicle_id, driver_id, route_id, departure_datetime)
WHERE status = 'completed';

-- =====================================================
-- ÍNDICE 2: Análisis temporal de deliveries
-- Beneficia: Q3 (conteos por estado), Q8 (mensual), Q12
-- =====================================================
CREATE INDEX idx_deliveries_scheduled_datetime
ON deliveries (scheduled_datetime, delivery_status)
WHERE delivery_status = 'delivered';

-- =====================================================
-- ÍNDICE 3: Mantenimiento por vehículo
-- Beneficia: Q1, Q9
-- =====================================================
CREATE INDEX idx_maintenance_vehicle_cost
ON maintenance (vehicle_id, cost);

-- =====================================================
-- ÍNDICE 4: Conductores activos / licencias
-- Beneficia: Q2, Q5, Q10
-- =====================================================
CREATE INDEX idx_drivers_status_license
ON drivers (status, license_expiry)
WHERE status = 'active';

-- =====================================================
-- ÍNDICE 5: Métricas de rutas
-- Beneficia: Q4, Q6, Q7, Q9, Q12
-- =====================================================
CREATE INDEX idx_routes_metrics
ON routes (route_id, distance_km, destination_city);

-- =====================================================
-- ÍNDICE 6 (Adicional): Foreign Key deliveries → trips
-- Beneficia: Q4, Q5, Q7, Q10, Q12
-- =====================================================
CREATE INDEX idx_deliveries_trip_id
ON deliveries (trip_id);


-- Estadística para el planner
ANALYZE vehicles;
ANALYZE drivers;
ANALYZE routes;
ANALYZE trips;
ANALYZE deliveries;
ANALYZE maintenance;

-- Verificación
SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public' AND indexname LIKE 'idx_%'
ORDER BY tablename, indexname;
