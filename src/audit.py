"""Operational audit trail for the Health Colombia ETL pipeline.

Persiste la trazabilidad de cada corrida en el esquema ``audit`` de la base
``salud_colombia``:

    audit.etl_run          -> una fila por corrida completa (CLI o Airflow)
    audit.etl_run_step     -> pasos/fases dentro de una corrida
    audit.dq_check_result  -> resultado por regla DQ-01..DQ-12 evaluada
    audit.etl_batch_log    -> una fila por lote de carga al DW (batch_id)

Diseño defensivo: la auditoría NUNCA rompe el pipeline. Si la base no está
disponible o falla una escritura, se emite un warning y el ETL continúa.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import numpy as np
import psycopg2
from psycopg2.extras import Json

from .config import DB_CONFIG
from .quality.rules import RULES

logger = logging.getLogger(__name__)

STATUS_RUNNING = 'RUNNING'
STATUS_SUCCESS = 'SUCCESS'
STATUS_FAILED = 'FAILED'
STATUS_BLOCKED = 'BLOCKED_QUALITY'


def _json_default(obj):
    """Convierte tipos numpy (int64, float64, bool_) a primitivos de Python."""
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def _jsonb(value: Optional[Dict[str, Any]]):
    """Adapter JSONB tolerante a tipos numpy."""
    if value is None:
        return None
    return Json(value, dumps=lambda v: json.dumps(v, default=_json_default, ensure_ascii=False))


def _connect():
    return psycopg2.connect(**DB_CONFIG)


def ensure_run(run_key: Optional[str] = None,
               run_type: str = 'CLI',
               dag_id: Optional[str] = None,
               trigger_source: Optional[str] = None,
               source_counts: Optional[Dict[str, Any]] = None) -> Optional[int]:
    """Crea (o recupera) la fila de corrida para ``run_key`` y retorna su run_id.

    Idempotente por UNIQUE(run_key): en Airflow, reintentos de tareas reutilizan
    el mismo run_id en lugar de duplicar corridas.
    """
    key = run_key or str(uuid.uuid4())
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO audit.etl_run
                    (run_key, dag_id, run_type, trigger_source, status, source_counts)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_key) DO UPDATE
                    SET source_counts = COALESCE(EXCLUDED.source_counts, audit.etl_run.source_counts)
                RETURNING run_id
            """, (key, dag_id, run_type, trigger_source, STATUS_RUNNING, _jsonb(source_counts)))
            run_id = cur.fetchone()[0]
        logger.info(f"Audit: run registrado run_id={run_id} run_key={key}")
        return run_id
    except Exception as e:
        logger.warning(f"Audit: no se pudo registrar la corrida ({e})")
        return None


def log_step(run_id: Optional[int],
             phase: str,
             step_name: str,
             status: str,
             rows_in: Optional[int] = None,
             rows_out: Optional[int] = None,
             message: Optional[str] = None,
             started_at: Optional[datetime] = None,
             finished_at: Optional[datetime] = None) -> None:
    """Registra un paso/fase de la corrida en audit.etl_run_step."""
    if run_id is None:
        return
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO audit.etl_run_step
                    (run_id, phase, step_name, status, rows_in, rows_out, started_at, finished_at, message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (run_id, phase, step_name, status, rows_in, rows_out,
                  started_at or datetime.now(timezone.utc),
                  finished_at or datetime.now(timezone.utc),
                  message))
    except Exception as e:
        logger.warning(f"Audit: no se pudo registrar el paso {phase}.{step_name} ({e})")


def log_dq_results(run_id: Optional[int], report: Dict[str, Any]) -> None:
    """Persiste un reporte de Great Expectations (gate) en audit.dq_check_result."""
    if run_id is None or not report:
        return
    gate = report.get('gate', 'unknown')
    try:
        with _connect() as conn, conn.cursor() as cur:
            for evaluation in report.get('evaluations', []):
                rule_id = evaluation.get('rule_id')
                details = evaluation.get('details') or {}
                cur.execute("""
                    INSERT INTO audit.dq_check_result
                        (run_id, rule_id, gate, layer, severity, status,
                         observed_value, threshold, details)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, rule_id) DO UPDATE
                        SET status = EXCLUDED.status,
                            observed_value = EXCLUDED.observed_value,
                            details = EXCLUDED.details,
                            checked_at = now()
                """, (
                    run_id,
                    rule_id,
                    gate,
                    RULES.get(rule_id, {}).get('layer'),
                    evaluation.get('severity'),
                    'PASS' if evaluation.get('success') else 'FAIL',
                    json.dumps(details, ensure_ascii=False, default=_json_default)[:500],
                    RULES.get(rule_id, {}).get('threshold'),
                    _jsonb(details),
                ))
            cur.execute("""
                UPDATE audit.etl_run
                SET dq_rules_passed = %s, dq_rules_total = %s
                WHERE run_id = %s
            """, (report.get('passed_rules'), report.get('total_rules'), run_id))
        logger.info(f"Audit: {report.get('total_rules', 0)} resultados DQ persistidos (run_id={run_id})")
    except Exception as e:
        logger.warning(f"Audit: no se pudieron persistir resultados DQ ({e})")


def finish_run(run_id: Optional[int],
               status: str,
               stage_reached: Optional[str] = None,
               rows_loaded: Optional[Dict[str, Any]] = None,
               error_message: Optional[str] = None) -> None:
    """Cierra la corrida con su estado final y duración calculada."""
    if run_id is None:
        return
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("""
                UPDATE audit.etl_run
                SET status = %s,
                    stage_reached = COALESCE(%s, stage_reached),
                    rows_loaded = COALESCE(%s, rows_loaded),
                    error_message = %s,
                    finished_at = now(),
                    duration_seconds = ROUND(EXTRACT(EPOCH FROM (now() - started_at))::numeric, 3)
                WHERE run_id = %s
            """, (status, stage_reached, _jsonb(rows_loaded),
                  error_message, run_id))
            # Agrega el conteo DQ total de la corrida desde las reglas persistidas
            cur.execute("""
                UPDATE audit.etl_run
                SET dq_rules_total = agg.total,
                    dq_rules_passed = agg.passed
                FROM (
                    SELECT COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE status = 'PASS') AS passed
                    FROM audit.dq_check_result
                    WHERE run_id = %s
                ) agg
                WHERE audit.etl_run.run_id = %s AND agg.total > 0
            """, (run_id, run_id))
        logger.info(f"Audit: corrida run_id={run_id} finalizada con estado {status}")
    except Exception as e:
        logger.warning(f"Audit: no se pudo finalizar la corrida run_id={run_id} ({e})")


def start_batch(run_id: Optional[int] = None,
                dag_id: Optional[str] = None,
                strategy: str = 'full_replace',
                target_rows_before: Optional[Dict[str, Any]] = None,
                notes: Optional[str] = None,
                conn=None) -> Optional[int]:
    """Abre un lote de carga al DW y retorna su batch_id.

    El batch_id devuelto debe estamparse en cada fila insertada en dim_* y
    fact_* para dejar procedencia de la carga (ver load_all).

    Si se pasa ``conn``, la fila del lote se inserta dentro de la transacción
    del llamador (con SAVEPOINT defensivo: un fallo aquí nunca rompe la carga);
    si no, se usa una conexión propia autocommit.
    """
    own = conn is None
    try:
        c = conn or _connect()
        with c.cursor() as cur:
            if not own:
                cur.execute("SAVEPOINT audit_start_batch")
            cur.execute("""
                INSERT INTO audit.etl_batch_log
                    (run_id, dag_id, strategy, status, target_rows_before, notes)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING batch_id
            """, (run_id, dag_id, strategy, STATUS_RUNNING,
                  _jsonb(target_rows_before), notes))
            batch_id = cur.fetchone()[0]
        if own:
            c.commit()
            c.close()
        logger.info(f"Audit: batch abierto batch_id={batch_id} (run_id={run_id})")
        return batch_id
    except Exception as e:
        if not own:
            try:
                with conn.cursor() as cur:
                    cur.execute("ROLLBACK TO SAVEPOINT audit_start_batch")
            except Exception:
                pass
        logger.warning(f"Audit: no se pudo abrir el batch ({e})")
        return None


def finish_batch(batch_id: Optional[int],
                 status: str,
                 target_rows_after: Optional[Dict[str, Any]] = None,
                 notes: Optional[str] = None,
                 conn=None) -> None:
    """Cierra el lote de carga con su estado final, conteos y duración."""
    if batch_id is None:
        return
    own = conn is None
    try:
        c = conn or _connect()
        with c.cursor() as cur:
            if not own:
                cur.execute("SAVEPOINT audit_finish_batch")
            cur.execute("""
                UPDATE audit.etl_batch_log
                SET status = %s,
                    target_rows_after = COALESCE(%s, target_rows_after),
                    notes = COALESCE(%s, notes),
                    finished_at = now(),
                    duration_seconds = ROUND(EXTRACT(EPOCH FROM (now() - started_at))::numeric, 3)
                WHERE batch_id = %s
            """, (status, _jsonb(target_rows_after), notes, batch_id))
        if own:
            c.commit()
            c.close()
        logger.info(f"Audit: batch batch_id={batch_id} finalizado con estado {status}")
    except Exception as e:
        if not own:
            try:
                with conn.cursor() as cur:
                    cur.execute("ROLLBACK TO SAVEPOINT audit_finish_batch")
            except Exception:
                pass
        logger.warning(f"Audit: no se pudo finalizar el batch batch_id={batch_id} ({e})")
