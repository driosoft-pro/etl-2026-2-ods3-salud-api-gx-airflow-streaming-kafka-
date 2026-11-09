"""Great Expectations 1.x backend para los gates de calidad (DQ-01 .. DQ-12).

Ejecuta suites GX *reales* (DataContext ephemeral -> ExpectationSuite ->
ValidationDefinition) sobre DataFrames en memoria, serializa los resultados en
``report["gx"]`` y mapea cada expectativa a la regla DQ que respalda.

Diseño:
  * Los gates construyen specs declarativas: ``(rule_id, NombreExpectativa, kwargs)``.
  * :func:`run_gx_suite` instancia las expectativas, corre la validación y
    devuelve un reporte compacto (una entrada por expectativa, con observed values).
  * :func:`combine_reports` fusiona los suites de un mismo gate.
  * :func:`apply_gx_to_results` combina el resultado GX con el chequeo nativo de
    pandas: la regla solo pasa si **ambos** lo hacen (política conservadora).

Fallback: si ``great_expectations`` no está instalado o la validación lanza una
excepción, se devuelve ``{"available": False, ...}`` y los gates siguen con sus
chequeos nativos; el motivo queda documentado en la evidencia.
"""

from __future__ import annotations

import contextlib
import io
import logging
import math
import os
import re
import uuid
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# La barra de progreso de GX ("Calculating Metrics ...it/s") se deshabilita y,
# además, se redirige stderr durante vd.run() para no ensuciar los logs de Airflow.
os.environ.setdefault("TQDM_DISABLE", "1")

try:
    import great_expectations as gx

    GX_AVAILABLE = True
    GX_VERSION = str(getattr(gx, "__version__", "unknown"))
    GX_IMPORT_ERROR = ""
except Exception as _gx_exc:  # pragma: no cover - sólo sin great_expectations instalado
    gx = None
    GX_AVAILABLE = False
    GX_VERSION = None
    GX_IMPORT_ERROR = str(_gx_exc)

# Especificación declarativa de una expectativa dentro de un suite.
#   (rule_id, nombre de la clase GX, kwargs de la expectativa)
GxSpec = Tuple[str, str, Dict[str, Any]]


# ---------------------------------------------------------------------------
# Estado / utilidades
# ---------------------------------------------------------------------------
def gx_status() -> Dict[str, Any]:
    """Estado del backend para incluirlo en la evidencia de cada gate."""
    if GX_AVAILABLE:
        return {
            "available": True,
            "library": "great_expectations",
            "version": GX_VERSION,
            "mode": "ephemeral",
        }
    return {
        "available": False,
        "library": "great_expectations",
        "reason": f"great_expectations no instalado: {GX_IMPORT_ERROR}",
    }


def _snake(class_name: str) -> str:
    """ExpectColumnValuesToBeInSet -> expect_column_values_to_be_in_set"""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", class_name).lower()


def _primitive(value: Any) -> Any:
    """Convierte numpy/NaN/inf a primitivos JSON-safe (los JSONB de Postgres
    rechazan los literales NaN/Infinity)."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if hasattr(value, "item") and callable(value.item):
        try:
            value = value.item()
        except Exception:  # noqa: BLE001 - escalar raro: se serializa como texto
            return str(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (list, tuple)):
        return [_primitive(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _primitive(v) for k, v in value.items()}
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    return str(value)


def _kwargs_snapshot(exp: Any) -> Dict[str, Any]:
    """Kwargs de la expectativa para la evidencia (value_set acotado)."""
    try:
        data = exp.model_dump() if hasattr(exp, "model_dump") else dict(exp)
    except Exception:  # noqa: BLE001
        data = {}
    skip = {"meta", "notes", "id", "gecko_id", "row_condition", "condition_parser"}
    snapshot: Dict[str, Any] = {}
    for key, value in data.items():
        if key in skip or value is None:
            continue
        if isinstance(value, (list, tuple)) and len(value) > 20:
            value = {"size": len(value), "sample": [_primitive(v) for v in list(value)[:10]]}
        else:
            value = _primitive(value)
        snapshot[key] = value
    return snapshot


def _serialize_expectation(rule_id: str, exp: Any, ev: Any) -> Dict[str, Any]:
    """Serializa una ExpectationValidationResult a un dict de evidencia."""
    entry: Dict[str, Any] = {
        "rule_id": rule_id,
        "expectation": _snake(type(exp).__name__),
        "kwargs": _kwargs_snapshot(exp),
    }
    if ev is None:
        # La columna referenciada no existe en el DataFrame: la expectativa
        # no pudo correr y se registra como fallo (la regla queda bloqueada).
        entry["success"] = False
        entry["error"] = "column_not_present_in_dataframe"
        return entry

    entry["success"] = bool(ev.success)
    result = getattr(ev, "result", None) or {}
    observed: Dict[str, Any] = {}
    for key in (
        "observed_value",
        "element_count",
        "value_count",
        "unexpected_count",
        "unexpected_percent",
        "unexpected_percent_total",
        "missing_count",
        "sum",
        "min",
        "max",
    ):
        if key in result:
            observed[key] = _primitive(result[key])
    if observed:
        entry["observed"] = observed
    for key in ("missing_columns", "unexpected_columns"):
        if key in result:
            entry[key] = _primitive(result[key])
    for key in ("partial_unexpected_list", "partial_unexpected_counts"):
        if key in result and result[key]:
            entry[key] = _primitive(result[key])[:10]
    if not ev.success:
        info = getattr(ev, "exception_info", None)
        if isinstance(info, dict) and info.get("raised_exception"):
            entry["exception"] = str(info.get("exception_message", ""))[:300]
    return entry


def _pair_results(expectations: Sequence[Any], results: Sequence[Any]) -> List[Any]:
    """Parea cada expectativa con su ExpectationValidationResult (tipo + columna)."""
    pool = list(results)
    paired: List[Any] = []
    for exp in expectations:
        want_type = _snake(type(exp).__name__)
        want_column = getattr(exp, "column", None)
        match = None
        for idx, ev in enumerate(pool):
            cfg = getattr(ev, "expectation_config", None)
            if (
                cfg is not None
                and cfg.type == want_type
                and cfg.kwargs.get("column") == want_column
            ):
                match = pool.pop(idx)
                break
        paired.append(match)
    return paired


def _unavailable(reason: str) -> Dict[str, Any]:
    status = gx_status()
    status.update({"reason": reason, "expectations": [], "failed_rules": []})
    status["available"] = False
    return status


# ---------------------------------------------------------------------------
# Columnas derivadas (espejo exacto de los chequeos nativos)
# ---------------------------------------------------------------------------
def with_numeric(
    df: pd.DataFrame,
    source: str,
    target: str,
    *,
    strip_thousands: bool = False,
    fillna: Optional[float] = None,
) -> pd.DataFrame:
    """Copia de ``df`` con ``target`` = columna ``source`` convertida a numérica.

    Se usa para que las expectativas ``expect_column_values_to_be_between`` /
    ``expect_column_values_to_not_be_null`` de GX evalúen el mismo valor que el
    chequeo nativo de pandas (p.ej. ``num_persons`` viene con puntos de miles).
    """
    if source not in df.columns:
        return df
    series = df[source]
    if strip_thousands:
        series = series.astype(str).str.replace(".", "", regex=False)
    series = pd.to_numeric(series, errors="coerce")
    if fillna is not None:
        series = series.fillna(fillna)
    return df.assign(**{target: series})


def with_key(df: pd.DataFrame, target: str, columns: Sequence[str]) -> pd.DataFrame:
    """Copia de ``df`` con ``target`` = clave compuesta de ``columns`` (para
    ``expect_column_values_to_be_unique`` sobre granos multi-columna)."""
    if any(c not in df.columns for c in columns):
        return df
    key = df[columns[0]].astype(str)
    for column in columns[1:]:
        key = key + "|" + df[column].astype(str)
    return df.assign(**{target: key})


def value_set(series: pd.Series) -> List[Any]:
    """Valores únicos de una columna como lista JSON-safe (para in_set / FK)."""
    values = series.dropna().unique().tolist()
    return [_primitive(v) for v in values]


# ---------------------------------------------------------------------------
# Ejecución de suites
# ---------------------------------------------------------------------------
def run_gx_suite(suite_name: str, df: pd.DataFrame, specs: Sequence[GxSpec]) -> Dict[str, Any]:
    """Ejecuta un suite GX real sobre ``df``.

    Devuelve un reporte con ``available=True`` y una entrada por expectativa
    (expectation, kwargs, success, observed values, rule_id), o bien
    ``available=False`` con el motivo del fallback.
    """
    if not GX_AVAILABLE:
        return _unavailable(f"great_expectations no instalado: {GX_IMPORT_ERROR}")

    try:
        ctx = gx.get_context(mode="ephemeral")
        data_source = ctx.data_sources.add_pandas(f"mem_{uuid.uuid4().hex[:8]}")
        asset = data_source.add_dataframe_asset(name="frame")
        batch_definition = asset.add_batch_definition_whole_dataframe("whole")

        # 1) Instanciar expectativas; descartar las que apuntan a columnas ausentes.
        prepared: List[Dict[str, Any]] = []
        runnable: List[Dict[str, Any]] = []
        for rule_id, type_name, kwargs in specs:
            item: Dict[str, Any] = {"rule_id": rule_id, "type": type_name, "ev": None}
            exp_cls = getattr(gx.expectations, type_name, None)
            if exp_cls is None:
                item["drop"] = f"expectation_type_not_found: {type_name}"
                prepared.append(item)
                continue
            exp = exp_cls(**kwargs)
            item["exp"] = exp
            column = kwargs.get("column")
            if column is not None and column not in df.columns:
                item["drop"] = "column_not_present_in_dataframe"
            else:
                runnable.append(item)
            prepared.append(item)

        # 2) Correr la validación real con GX (una sola pasada por suite).
        if runnable:
            suite = gx.ExpectationSuite(name=f"{suite_name}_{uuid.uuid4().hex[:6]}")
            for item in runnable:
                suite.add_expectation(item["exp"])
            ctx.suites.add(suite)
            validation_definition = ctx.validation_definitions.add(
                gx.ValidationDefinition(
                    name=f"{suite_name}_vd_{uuid.uuid4().hex[:6]}",
                    data=batch_definition,
                    suite=suite,
                )
            )
            buffer = io.StringIO()
            with contextlib.redirect_stderr(buffer):  # barra de progreso de GX
                validation = validation_definition.run(
                    batch_parameters={"dataframe": df}
                )
            paired = _pair_results(
                [item["exp"] for item in runnable], list(validation.results)
            )
            for item, ev in zip(runnable, paired):
                item["ev"] = ev

        # 3) Serializar en el mismo orden en que se declararon las specs.
        entries: List[Dict[str, Any]] = []
        for item in prepared:
            if "drop" in item:
                entries.append(
                    {
                        "rule_id": item["rule_id"],
                        "expectation": item["type"],
                        "success": False,
                        "error": item["drop"],
                    }
                )
            else:
                entries.append(
                    _serialize_expectation(item["rule_id"], item["exp"], item["ev"])
                )

        failed = [e for e in entries if not e.get("success")]
        return {
            "available": True,
            "library": "great_expectations",
            "version": GX_VERSION,
            "mode": "ephemeral",
            "suite": suite_name,
            "dataframe_rows": int(len(df)),
            "expectations_total": len(entries),
            "expectations_passed": len(entries) - len(failed),
            "expectations_failed": len(failed),
            "failed_rules": sorted({e["rule_id"] for e in failed}),
            "expectations": entries,
            "success": len(failed) == 0,
        }
    except Exception as exc:  # noqa: BLE001 - GX degrada a fallback nativo
        logger.warning("GX suite '%s' no pudo ejecutarse; fallback nativo: %s", suite_name, exc)
        return _unavailable(f"error al ejecutar el suite '{suite_name}': {exc}")


# ---------------------------------------------------------------------------
# Combining / merge con los chequeos nativos
# ---------------------------------------------------------------------------
def combine_reports(reports: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Fusiona los reportes de suites de un mismo gate en ``report["gx"]``."""
    reports = [r for r in reports if r]
    ran = [r for r in reports if r.get("available")]
    summary: Dict[str, Any] = {
        "available": bool(ran),
        "library": "great_expectations",
        "version": GX_VERSION if ran else None,
        "mode": "ephemeral",
        "suites_requested": len(reports),
        "suites_run": len(ran),
        "expectations_total": sum(r.get("expectations_total", 0) for r in ran),
        "expectations_passed": sum(r.get("expectations_passed", 0) for r in ran),
        "expectations_failed": sum(r.get("expectations_failed", 0) for r in ran),
        "failed_rules": sorted({rid for r in ran for rid in r.get("failed_rules", [])}),
        "suites": [
            {
                "suite": r.get("suite"),
                "rows": r.get("dataframe_rows"),
                "available": bool(r.get("available")),
                "expectations_total": r.get("expectations_total", 0),
                "expectations_passed": r.get("expectations_passed", 0),
                "expectations_failed": r.get("expectations_failed", 0),
                "expectations": r.get("expectations", []),
                **({"reason": r.get("reason")} if not r.get("available") else {}),
            }
            for r in reports
        ],
    }
    if not ran:
        summary["fallback_reason"] = (
            reports[0].get("reason") if reports else "no se ejecutó ninguna suite GX"
        )
    return summary


def apply_gx_to_results(
    results: List[Dict[str, Any]], reports: Sequence[Dict[str, Any]]
) -> Set[str]:
    """Combina los resultados GX con los chequeos nativos (AND conservador).

    * Adjunta a cada regla sus expectativas GX (``res["gx"]``) como evidencia.
    * Si alguna expectativa de una regla falla, la regla queda en ``success=False``
      aunque el chequeo nativo la diera por buena.
    Devuelve el conjunto de reglas que GX marcó como fallidas.
    """
    by_rule: Dict[str, List[Dict[str, Any]]] = {}
    failed_rules: Set[str] = set()
    for report in reports:
        if not report or not report.get("available"):
            continue
        for entry in report.get("expectations", []):
            by_rule.setdefault(entry["rule_id"], []).append(entry)
            if not entry.get("success"):
                failed_rules.add(entry["rule_id"])

    for res in results:
        entries = by_rule.get(res["rule_id"])
        if not entries:
            continue
        res["gx"] = entries
        failed_entries = [e for e in entries if not e.get("success")]
        # Resumen compacto en `details` (se persiste en audit.dq_check_result).
        res.setdefault("details", {})["gx"] = {
            "expectations": len(entries),
            "failed": len(failed_entries),
            "failed_expectations": [
                f"{e['expectation']}({(e.get('kwargs') or {}).get('column', 'table')})"
                for e in failed_entries
            ],
        }
        if res.get("success") and res["rule_id"] in failed_rules:
            res["success"] = False
            res.setdefault("details", {})["gx_expectation_failed"] = True

    return failed_rules
