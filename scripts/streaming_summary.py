"""Read-only summary of persisted streaming history and Kafka consumer lag."""
from __future__ import annotations

import os

import psycopg2
from kafka import KafkaConsumer, TopicPartition

from src.config import DB_CONFIG


def show_table(title, headers, rows):
    print(f"\n{title}")
    if not rows:
        print("Sin registros.")
        return
    print(" | ".join(headers))
    for row in rows:
        # Keep stored rejection reasons/keys from injecting terminal control characters.
        print(" | ".join("-" if item is None else
                         "".join(c if c.isprintable() else " " for c in str(item))
                         for item in row))


def show_history(conn):
    conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT count(*), count(*) FILTER (WHERE event_type = 'threshold_alert'),
                   count(*) FILTER (WHERE simulation), min(received_at), max(received_at)
            FROM streaming.event_log
        """)
        processed, alerts, simulated, first, last = cur.fetchone()
        cur.execute("SELECT count(*), min(received_at), max(received_at) FROM streaming.invalid_events")
        rejected, first_rejected, last_rejected = cur.fetchone()
        print("RESUMEN DE EVENTOS STREAMING (historial disponible, todas las ejecuciones)")
        print(f"Total registrado: {processed + rejected}")
        print(f"PROCESADOS: {processed} (incluye {alerts} alertas validas)")
        print(f"RECHAZADOS: {rejected}")
        print(f"Procesados de simulacion/replay: {simulated}; sin simulacion: {processed - simulated}")
        dates = [date for date in (first, first_rejected) if date is not None]
        ends = [date for date in (last, last_rejected) if date is not None]
        print(f"Periodo (UTC): {min(dates) if dates else '-'} -> {max(ends) if ends else '-'}")
        cur.execute("""
            SELECT event_type, metric_name, simulation, count(*)
            FROM streaming.event_log GROUP BY event_type, metric_name, simulation
            ORDER BY event_type, metric_name, simulation
        """)
        show_table("PROCESADOS POR TIPO Y METRICA", ("Tipo", "Metrica", "Simulacion", "Cantidad"), cur.fetchall())
        cur.execute("""
            SELECT reason, count(*) FROM streaming.invalid_events
            GROUP BY reason ORDER BY count(*) DESC, reason
        """)
        show_table("RECHAZADOS POR MOTIVO", ("Motivo", "Cantidad"), cur.fetchall())
        cur.execute("""
            SELECT consumer_id, status,
                   CASE WHEN status <> 'running' THEN 'DETENIDO/ERROR'
                        WHEN now() - last_heartbeat > interval '60 seconds' THEN 'SIN SENAL RECIENTE'
                        ELSE 'ACTIVO' END,
                   last_heartbeat, events_processed, invalid_events
            FROM streaming.consumer_heartbeat ORDER BY consumer_id
        """)
        show_table("CONSUMIDORES (contadores desde su ultimo arranque)",
                   ("Consumidor", "Estado guardado", "Senal", "Heartbeat", "Procesados", "Invalidos"), cur.fetchall())
        cur.execute("""
            SELECT received_at, state, identifier, event_key, detail FROM (
                SELECT received_at, 'PROCESADO' AS state, event_id::text AS identifier,
                       event_key, event_type || ' / ' || metric_name AS detail
                FROM streaming.event_log
                UNION ALL
                SELECT received_at, 'RECHAZADO', 'invalid:' || invalid_id::text,
                       message_key, reason FROM streaming.invalid_events
            ) history ORDER BY received_at DESC, identifier DESC LIMIT 20
        """)
        show_table("ULTIMOS 20 REGISTROS (los totales incluyen todo el historial)",
                   ("Fecha UTC", "Estado", "ID", "Clave", "Detalle"), cur.fetchall())
    print("\nLos validos se deduplican por event_id; los rechazos cuentan registros guardados.")
    print("No incluye datos eliminados al vaciar volumenes ni envios que aun no se guardaron.")
    print("Un rechazo guardado no confirma que su copia haya llegado al topico DLQ.")


def show_lag():
    topic = os.getenv("KAFKA_TOPIC", "salud.metric.updates")
    group = os.getenv("STREAMING_CONSUMER_GROUP", "health-metrics-consumers")
    consumer = None
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            group_id=group, enable_auto_commit=False, request_timeout_ms=10000,
            api_version_auto_timeout_ms=5000, session_timeout_ms=6000,
            heartbeat_interval_ms=2000,
        )
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            print("\nKafka: topico sin particiones disponibles; pendientes desconocidos.")
            return
        tps = [TopicPartition(topic, p) for p in sorted(partitions)]
        ends = consumer.end_offsets(tps)
        rows = []
        for tp in tps:
            offset = consumer.committed(tp)
            rows.append((tp.partition, offset, ends[tp],
                         max(0, ends[tp] - offset) if offset is not None else "DESCONOCIDO"))
        show_table(f"KAFKA: {topic} / grupo {group}",
                   ("Particion", "Offset confirmado", "Offset final", "Lag"), rows)
        print("Lag = offsets aun no confirmados; puede incluir eventos guardados antes del commit.")
        print("Sin offset confirmado no se puede determinar el pendiente. No se consumen ni confirman mensajes.")
    except Exception as exc:
        print(f"\nKafka no disponible ({type(exc).__name__}); pendientes desconocidos. Historial conservado arriba.")
    finally:
        if consumer is not None:
            consumer.close()


def main():
    conn = None
    try:
        conn = psycopg2.connect(**{**DB_CONFIG, "connect_timeout": 5,
                                   "options": "-c timezone=UTC -c statement_timeout=15000"})
        show_history(conn)
    except psycopg2.Error as exc:
        print(f"No se pudo consultar el historial ({type(exc).__name__}). Comprueba PostgreSQL y el esquema streaming.")
        return 1
    finally:
        if conn is not None:
            conn.close()
    show_lag()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
