# Data Project – FleetLogix
Proyecto Académico

## Descripción General

El proyecto FleetLogix es una simulación integral de un entorno real de ingeniería de datos, que evoluciona desde una base transaccional local hacia un ecosistema analítico completo en la nube.

A lo largo de 4 avances, se construyó:
- Una base transaccional PostgreSQL con más de 500,000 registros simulados.
- Consultas SQL optimizadas para mejorar el rendimiento operativo.
- Un Data Warehouse en Snowflake basado en un modelo estrella.
- Un pipeline ETL automático en Python para extracción, transformación y carga.
- Una arquitectura en AWS diseñada para ingesta, almacenamiento y procesamiento en tiempo real mediante servicios serverless.

El proyecto combina fundamentos de SQL, optimización, modelado dimensional, ETL, arquitectura cloud y automatización, simulando el flujo completo de datos en una empresa logística moderna.

---

## Objetivos del Proyecto

- Simular el ecosistema de datos de una empresa logística real (FleetLogix).
- Construir una base transaccional robusta para consultas operativas.
- Diseñar un modelo dimensional escalable para analítica histórica.
- Implementar un pipeline ETL profesional usando Python + Snowflake.
- Diseñar una arquitectura serverless en AWS para ingesta y análisis en tiempo real.

---

## Estructura del Proyecto
```
ProyectoM2_CristianGarcia/
│
├── Scripts/
│   ├── 00_create_database.sql           # Generación DB en PostgreSQL
│   ├── 01_data_generation.py           # Generación de 505k+ registros
│   ├── 02_queries_analysis.sql         # 12 consultas analíticas operativas
│   ├── 03_optimization_indexes.sql     # Índices de optimización en PostgreSQL
│   ├── 04_dimensional_model.sql        # DDL del Data Warehouse en Snowflake
│   ├── 05_etl_pipeline.py              # Pipeline ETL automático
│
├── Documentación/
│   ├── README.pdf
│   ├── 00_ERD_FleetLogix.png
│   ├── 01_Analisis_del_modelo_proporcionado.pdf
│   ├── 02_Manual_Consultas_SQL.pdf
│   ├── 03_Analisis_Snowflake_ETL.pdf
│   ├── 04_AWS_Analisis_Arquitectura.pdf
│   └── 05_aws_architecture_diagram.html
│
└── requirements.txt
```
---

## Configuración del Entorno

### Clonar el repositorio

git clone https://github.com/cfgarciac/Module-2.git
cd Module-2


### Crear entorno virtual

  python -m venv .venv

### Activar entorno
  ..venv\Scripts\activate (en PowerShell / Windows)

### Instalar dependencias
  pip install -r requirements.txt

---

## Desarrollo por Fases

Avance 1 – Generación de Datos & Modelo Transaccional
Generación de más de 500,000 registros realistas (vehículos, conductores, rutas, viajes y entregas).
Creación del modelo transaccional en PostgreSQL.
Carga masiva y validación de integridad.

Avance 2 – Consultas Operativas e Índices
Construcción de 12 consultas operativas (KPIs diarios de entrega, eficiencia de combustible, retrasos, ranking de conductores).
Optimización con índices compuestos y ejecución verificando reducción de tiempos.

Avance 3 – Data Warehouse en Snowflake + Pipeline ETL
Diseño completo del modelo estrella (fact_deliveries + dimensiones).
Implementación del Data Warehouse en Snowflake.
Construcción del pipeline ETL automático:
Extracción de PostgreSQL
Transformaciones avanzadas:
entregas por hora
tiempos y retrasos
revenue por entrega
SCD básico
Carga en Snowflake
Pre-cálculo de métricas diarias.

Avance 4 – Arquitectura Cloud en AWS
Diseño completo de arquitectura serverless:
API Gateway (ingesta desde la app móvil)
Lambdas para:
verificar entrega completada
calcular ETA
detectar desvíos de ruta
S3 como data lake
DynamoDB como estado actual de entregas
RDS para base transaccional migrada
Diagrama de arquitectura AWS.

--- 

## Principales Resultados

- 223,051 entregas válidas cargadas en la tabla de hechos.
- Transformaciones exitosas para métricas avanzadas (>8 indicadores nuevos).
- Data Warehouse funcional en Snowflake, accesible para BI.
- Arquitectura AWS modelada para soporte en tiempo real.
- Pipeline ETL automatizado y modular en Python.

---

## Conclusiones

- FleetLogix pasa de un sistema puramente operativo a un ecosistema analítico completo.
- El Data Warehouse permite análisis histórico, comparativo y por segmentos.
- Las Lambdas propuestas habilitan monitoreo en tiempo real.
- El diseño general sienta las bases para:
    machine learning (predicción de retrasos)
    optimización de rutas
    escalabilidad cloud
- El proyecto demuestra la integración práctica de data engineering, modelado, análisis y arquitectura cloud.
---

## Próximos Pasos

- Implementar procesos CI/CD con GitHub Actions.
- Agregar orquestación con AWS Step Functions o Airflow.
- Desarrollar dashboards ejecutivos en Power BI o Looker Studio.
- Añadir ML para detección temprana de anomalías de ruta.

---

## Tecnologías Utilizadas

- Python: pandas, numpy, schedule, snowflake-connector
- Bases de Datos: PostgreSQL, Snowflake, DynamoDB
- Cloud: AWS (API Gateway, Lambda, S3, RDS)
- Modelado: SQL, diseño dimensional
- Control de versiones: Git + GitHub

---

## Autor

Cristian García
Correo: cfgarciac@unal.edu.co
LinkedIn: https://www.linkedin.com/in/cfgarciac/

Versión: 1.0 – Noviembre 2025