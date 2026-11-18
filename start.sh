#!/usr/bin/env bash
# Linux/macOS: bash start.sh [start|stop|validate|reset|wipe|steps|auto|restart|setup]
set -Eeuo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
cd "$ROOT"
dc() { docker compose -f "$ROOT/docker-compose.yml" "$@"; }
prepare() {
  command -v docker >/dev/null || { echo 'Instala Docker con Compose v2.' >&2; return 1; }
  [[ -f .env ]] || cp .env.example .env
  mkdir -p config
  [[ -f config/simple_auth_manager_passwords.json ]] || printf '%s\n' '{"airflow":"airflow"}' > config/simple_auth_manager_passwords.json
  docker info >/dev/null 2>&1 || { echo 'Inicia el motor de Docker.' >&2; return 1; }
  dc config --quiet
}
confirm_erase() {
  echo 'Se borraran contenedores y volumenes de ESTE proyecto, .pgdata y data/batches.'
  echo 'Se conservan codigo, .env, fuentes originales y evidencias. Otros proyectos no se tocan.'
  local answer
  read -r -p 'Escribe ELIMINAR para continuar: ' answer
  [[ "$answer" == ELIMINAR ]] || { echo 'Operacion cancelada.' >&2; return 1; }
}
erase_data() {
  local configured="${PGDATA_HOST_DIR:-}" line value target resolved
  if [[ -z "$configured" ]]; then
    while IFS= read -r line || [[ -n "$line" ]]; do
      if [[ "$line" == PGDATA_HOST_DIR=* ]]; then
        value="${line#*=}"; value="${value%$'\r'}"
        value="${value#\"}"; value="${value%\"}"; value="${value#\'}"; value="${value%\'}"
        configured="$value"
      fi
    done < .env
  fi
  case "${configured:-./.pgdata}" in ./.pgdata|.pgdata|"$ROOT/.pgdata") ;;
    *) echo 'El borrado automatico requiere PGDATA_HOST_DIR=./.pgdata.' >&2; return 1;; esac
  # Check every target/ancestor before stopping or removing anything.
  for target in "$ROOT/.pgdata" "$ROOT/data" "$ROOT/data/batches"; do
    [[ ! -L "$target" ]] || { echo "Se rechaza un enlace: $target" >&2; return 1; }
    if [[ -d "$target" ]]; then
      resolved="$(cd -- "$target" && pwd -P)"
      [[ "$resolved" == "$ROOT/"* ]] || return 1
    fi
  done
  dc --profile tools --profile bootstrap down --volumes --remove-orphans
  for target in "$ROOT/.pgdata" "$ROOT/data/batches"; do
    if [[ -d "$target" ]]; then
      # A container may own PostgreSQL files as uid 999. Empty only this verified bind.
      docker run --rm --mount "type=bind,source=$target,target=/data" alpine:3.20 \
        sh -c 'find /data -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +'
      rmdir -- "$target"
    fi
  done
}
project_port() {
  local name="$1" value="${!1:-}" line
  if [[ -z "$value" && -f .env ]]; then
    while IFS= read -r line || [[ -n "$line" ]]; do
      if [[ "$line" == "$name="* ]]; then
        value="${line#*=}"; value="${value%$'\r'}"
        value="${value#\"}"; value="${value%\"}"; value="${value#\'}"; value="${value%\'}"
      fi
    done < .env
  fi
  value="${value:-$2}"
  [[ "$value" =~ ^[0-9]+$ && ${#value} -le 5 ]] && ((10#$value >= 1 && 10#$value <= 65535)) || { echo "Puerto invalido: $name" >&2; return 1; }
  printf '%s' "$value"
}
show_links() {
  local airflow superset api table rt pg kafka
  airflow="http://localhost:$(project_port AIRFLOW_WEBSERVER_PORT 8080)"
  superset="http://localhost:$(project_port SUPERSET_PORT 8088)"
  api="http://localhost:$(project_port POSTGREST_PORT 3220)"
  rt="$(project_port REALTIME_DASHBOARD_PORT 8501)"
  pg="$(project_port POSTGRES_PORT 5432)"; kafka="$(project_port KAFKA_EXTERNAL_PORT 19092)"
  printf '\nENLACES DEL PROYECTO (requieren servicios iniciados)\n'
  echo "Airflow: $airflow"
  echo "DAG principal: $airflow/dags/reliable_health_pipeline"
  echo "DAG de fallo controlado: $airflow/dags/bad_health_pipeline"
  echo "Salud de Airflow: $airflow/api/v2/monitor/health"
  echo "Superset: $superset"
  echo "Dashboard BI: $superset/superset/dashboard/ods3-salud-colombia/"
  echo "Dashboards: $superset/dashboard/list/"
  echo "Graficos: $superset/chart/list/"
  echo "Datasets: $superset/tablemodelview/list/"
  echo "SQL Lab: $superset/sqllab/"
  echo "Dashboard tiempo real: http://localhost:$rt"
  echo "API PostgREST / esquema OpenAPI: $api/"
  for table in dim_time dim_geografia dim_regime dim_facility dim_capacity_type fact_affiliates fact_facility_capacity; do
    echo "API $table: $api/$table?limit=10"
  done
  echo "PostgreSQL (DBeaver/pgAdmin, no navegador): localhost:$pg"
  echo "Kafka (cliente Kafka, no navegador): localhost:$kafka"
  echo 'El dashboard BI requiere haber completado setup. Usa tus credenciales configuradas.'
}
show_kafka() {
  printf '\nESTADO Y RECURSOS DE KAFKA\n'
  docker inspect --format 'Contenedor={{.Name}} Estado={{.State.Status}} Salud={{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}} Reinicios={{.RestartCount}}' kafka_salud || { echo 'Kafka no esta creado o Docker no esta disponible.'; return; }
  echo; echo 'Topicos y particiones:'
  docker exec kafka_salud /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
  echo; echo 'Detalle del topico salud.metric.updates:'
  docker exec kafka_salud /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic salud.metric.updates
  echo; echo 'Grupos de consumidores:'
  docker exec kafka_salud /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
  echo; echo 'Lag de health-metrics-consumers (si existe):'
  docker exec kafka_salud /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group health-metrics-consumers || true
  echo; echo 'Uso de disco de logs Kafka:'
  docker exec kafka_salud sh -c 'du -sh /var/lib/kafka/data 2>/dev/null || true'
  echo; echo 'Uso de CPU y memoria del contenedor:'
  docker stats kafka_salud --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}'
}
show_kafka_live() {
  echo 'Monitoreo en tiempo real de Kafka. Presiona Ctrl+C para salir.'
  while true; do
    clear
    printf 'KAFKA EN TIEMPO REAL - %s\n' "$(date '+%Y-%m-%d %H:%M:%S')"
    echo 'Actualizacion cada 5 segundos. Ctrl+C para volver.'
    docker inspect --format 'Contenedor={{.Name}} Estado={{.State.Status}} Salud={{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}} Reinicios={{.RestartCount}}' kafka_salud
    echo; echo 'CPU / memoria / red / disco:'
    docker stats kafka_salud --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}'
    echo; echo 'Topico salud.metric.updates:'
    docker exec kafka_salud /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic salud.metric.updates
    echo; echo 'Lag del consumidor:'
    docker exec kafka_salud /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group health-metrics-consumers 2>/dev/null || true
    echo; echo 'Tamano de logs:'
    docker exec kafka_salud sh -c 'du -sh /var/lib/kafka/data 2>/dev/null || true'
    sleep 5
  done
}
start_project() { dc up -d --build --wait --wait-timeout 600; show_links; }
load_project() {
  local run_id="launcher_$(date -u +%Y%m%dT%H%M%S)_$$" ready=0 state query
  for ((i=0; i<60; i++)); do
    if dc exec -T airflow-scheduler airflow dags trigger reliable_health_pipeline --run-id "$run_id"; then ready=1; break; fi
    sleep 5
  done
  [[ "$ready" == 1 ]] || { echo 'No se pudo disparar el DAG.' >&2; return 1; }
  query="from airflow.settings import Session; from airflow.models.dagrun import DagRun; s=Session(); r=s.query(DagRun).filter(DagRun.run_id=='$run_id',DagRun.dag_id=='reliable_health_pipeline').one_or_none(); print('LAUNCH_STATE='+str(r.state if r else 'pending')); s.close()"
  for ((i=0; i<180; i++)); do
    state="$(dc exec -T airflow-scheduler python -c "$query")"
    case "$state" in *LAUNCH_STATE=success*) echo 'Carga Airflow exitosa.'; return;;
      *LAUNCH_STATE=failed*) echo "El DAG $run_id fallo; se cancela el bootstrap." >&2; return 1;; esac
    echo 'Esperando las ocho tareas de Airflow...'; sleep 5
  done
  echo "Tiempo agotado: consulta $run_id en Airflow." >&2; return 1
}
bootstrap() {
  dc --profile bootstrap run --rm --build superset-bootstrap
  dc exec -T superset /app/.venv/bin/python /app/scripts/superset_charts_bootstrap.py
  show_links
}
validate() {
  dc config --quiet
  dc ps -a
  dc exec -T airflow-scheduler python -m pip check
  dc --profile tools run --rm --build -e GX_EVIDENCE_DIR=/tmp/etl-test-gx etl python -m pytest -q -p no:cacheprovider
}
step() {
  local answer
  read -r -p "$1 [Enter=continuar / q=cancelar]: " answer
  [[ "$answer" != q ]] || { echo 'Cancelado; se conservan pasos completados.'; return 1; }
}
execute() {
  case "$1" in help|--help|-h)
    echo 'bash start.sh [start|stop|validate|reset|wipe|steps|auto|restart|setup|links|kafka|kafka-live]'
    echo 'reset/restart conservan datos; wipe/steps/auto piden ELIMINAR y borran datos.'
    return;; esac
  case "$1" in
    10|links) show_links; return;;
    11|kafka) show_kafka; return;;
    12|kafka-live|live) show_kafka_live; return;;
  esac
  prepare
  case "$1" in
    1|start) start_project;;
    2|stop) dc stop;;
    3|validate) validate;;
    4|reset) dc down --remove-orphans; start_project;;
    5|wipe) confirm_erase; erase_data;;
    6|steps)
      confirm_erase
      step '1/5 Eliminar datos'; erase_data
      step '2/5 Construir e iniciar'; start_project
      step '3/5 Cargar y esperar Airflow'; load_project
      step '4/5 Crear dashboard y graficos'; bootstrap
      step '5/5 Validar'; validate;;
    7|auto) confirm_erase; erase_data; start_project; load_project; bootstrap; validate;;
    8|restart) dc restart; show_links;;
    9|setup) start_project; load_project; bootstrap;;
    *) echo 'Opcion desconocida.' >&2; return 1;;
  esac
}
if [[ "${1:-menu}" != menu ]]; then
  execute "$1"
else
  while true; do
    printf '\n1 Iniciar | 2 Detener | 3 Validar | 4 Resetear contenedores (conserva datos)\n'
    echo '5 Vaciar volumenes/datos | 6 Recrear paso a paso | 7 Reconstruccion automatica completa'
    echo '8 Reiniciar servicios | 9 Instalar/cargar dashboards sin borrar | 0 Salir'
    echo '10 Mostrar todos los enlaces del proyecto'
    echo '11 Mostrar estado, topicos, lag y recursos de Kafka'
    echo '12 Monitorear Kafka en tiempo real (Ctrl+C para salir)'
    read -r -p 'Opcion: ' choice || break
    [[ "$choice" != 0 ]] || break
    # Run in its own shell: failures stop the sequence and return to the menu.
    if ! bash "$ROOT/start.sh" "$choice"; then echo 'Operacion fallida o cancelada; revisa el mensaje anterior.' >&2; fi
  done
fi
