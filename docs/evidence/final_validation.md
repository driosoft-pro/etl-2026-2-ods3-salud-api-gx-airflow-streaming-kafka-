# Validación integrada final

Fecha: 18 de noviembre de 2026, America/Bogota. Se conserva la estructura del proyecto.

## Resultado comprobado

| Comprobación | Resultado real |
|---|---|
| Pruebas | **203 passed in 97.20s**, sin warnings, en la imagen final de Airflow |
| Dependencias Airflow / Streamlit | `pip check`: No broken requirements found en ambos entornos |
| Requests en Airflow | Import limpio; chardet 5.2.0 compatible con requests 2.32.5 y gcloud-aio-auth |
| DAG fiable manual | `final_capacity_fix_20261118`: success, 8/8 tareas, 12/12 reglas |
| DAG fiable programado | `scheduled__2026-11-18T06:00:00+00:00`: success |
| Programación | `ETL_SCHEDULE=0 6 * * *`, 06:00 UTC / 01:00 Bogotá; DAG no pausado |
| Fallo crítico | `final_validation_bad_20261118`: failed, auditoría BLOCKED_QUALITY 4/5; carga upstream_failed |
| Protección del DW | Cero batches de carga asociados al DAG de falla; el DAG válido carga desde sus snapshots verificados |
| GX real | versión 1.25.0, 11/11 + 6/6 + 10/10 = 27 expectativas |
| Notebook | 45 celdas de código ejecutadas, cero outputs de error, tres figuras; incluye API |
| Superset | 8 datasets y 7 gráficos; 7 consultas HTTP 200 / success; sin errores de página |
| Streamlit | AppTest sin excepciones; cambio de departamento sin excepciones; API agent status ready antes y después del filtro |
| Evento inválido | Dos rechazos persistidos, enviados a salud.metric.dlq; consumidor siguió activo |
| Replay | 2 ciclos × 67 métricas = 134 eventos identificados como simulación |
| Delta | Sigue en 0 al repetir el mismo snapshot; la alerta de demostración no lo altera |
| Tasa en reposo | `events_per_minute = 0` y consulta de últimos 60 s = 0 |
| Interrupción | OFFLINE visible tras detener el consumidor, heartbeat stopped con edad 19 s; luego ONLINE con edad 2 s |
| Contenedores | Conservan nombres y puertos; PostgREST sigue en localhost:3220 |

La prueba final se ejecutó copiando tests a `/tmp/etl_final_clean/tests` del contenedor
`airflow_scheduler_salud`, con `GX_EVIDENCE_DIR=/tmp/etl_final_clean/gx`,
`PYTHONDONTWRITEBYTECODE=1` y `python -m pytest -q -p no:cacheprovider tests`.
Así los casos negativos no sobrescribieron los JSON de la corrida exitosa.

Salida final registrada:

```text
203 passed in 97.20s (0:01:37)
```

Antes del ajuste específico de chardet hubo una corrida de 195 pruebas con un warning de
requests. La imagen final ya no presenta esa advertencia; no se ocultó con filtros de warnings.

La última corrida volvió a ejecutar las ocho tareas después de corregir la capacidad `1,000` → `1000`, y obtuvo auditoría `SUCCESS 12/12`. El productor snapshot detectó el cambio real de capacidad y publicó un evento adicional sin simulación.

## DW e indicadores

| Tabla | Filas |
|---|---:|
| dim_time | 2 |
| dim_geografia | 2.240 |
| dim_regime | 4 |
| dim_facility | 10.921 |
| dim_capacity_type | 63 |
| fact_affiliates | 3.369 |
| fact_facility_capacity | 31.496 |

Totales: 51.182.238 afiliados; 219.758 unidades de capacidad. R3 ahora cuenta IPS por clave:
Privada 9.632; Pública 1.274; Mixta 15. Norte de Santander = Andina, Vichada = Orinoquia,
Guainía = Amazonia, consistentes con la API tras normalizar tildes.

## Evidencias trazables

- [Carga, constraints, auditoría y tareas](dw/dimensional_dw_state.log).
- [Reportes GX finales](gx/): primarios 11/11, API 6/6, preparados 10/10.
- [Perfilado de CSV y API](profiling/data_profiling_evidence.json).
- [Airflow: corridas](screenshots/airflow_dags.png) y [ocho tareas exitosas](screenshots/airflow_dag_detail.png).
- [Dashboard Superset R1–R7](screenshots/superset_dashboard.png).
- [Rechazo y alerta](streaming/01_contract_violation_and_alert.log), [mensajes del DLQ](streaming/02_dlq_topic_messages.log).
- [Estado streaming y tasa en reposo](streaming/03_database_state.log).
- [Replay y configuración del tópico](streaming/05_replay_and_topic_listen.log).
- [Interrupción y recuperación](streaming/04_consumer_interruption.log).
- [Dashboard tiempo real](screenshots/streamlit_realtime.png), [OFFLINE](screenshots/streamlit_consumer_offline.png), [recuperado](screenshots/streamlit_consumer_recovered.png).

Los timestamps de los logs están en UTC; UTC-5 corresponde a America/Bogota. Los contadores del
banner de Streamlit son desde el último arranque del consumidor; los KPIs acumulan la historia de
PostgreSQL. Después de los experimentos hay 203 eventos válidos históricos, 2 inválidos y 1 alerta.
El consumidor se dejó encendido y el DAG fiable con su programación activa.

## Cobertura contra el PDF

- 2.1: objetivos, preguntas analíticas y distinción batch/streaming documentados.
- 2.2: CSV + API y extracción diaria; snapshot/replay explícitos.
- 2.3: EDA de todas las fuentes, visualizaciones, perfilado y relación con las reglas.
- 2.4: extracción, transformación, GX obligatorio y carga dimensional demostradas; fallo crítico visible.
- 2.5: dashboard BI conectado al DW, con conteo de IPS y regiones corregidos.
- 2.6: productor/consumidor Python, contrato, métricas y experimentos de inválido e interrupción.
- 2.7: dashboard separado, métricas actualizadas, filtro coherente por departamento y refresco de 3 s.
- 3: código/configuración/scripts/diagramas/evidencias y README conservan la estructura. Los secretos
  siguen excluidos. La publicación en GitHub requiere revisar y subir estos cambios locales.

No se certifica retroalimentación docente no aportada ni se modifican repositorios de entregas previas.
La prueba funcional no convierte esta demostración de un broker en un sistema de alta disponibilidad.
Kafka conserva mensajes en `kafka_data`; PostgreSQL conserva `.pgdata`; no borrar esos recursos al parar.

## Cómo revisar los cambios

Para reproducir el entorno y los experimentos, usa la sección 10 del README. El informe de auditoría
anterior describe el estado previo; los resultados de este documento sustituyen sus pendientes técnicos.
