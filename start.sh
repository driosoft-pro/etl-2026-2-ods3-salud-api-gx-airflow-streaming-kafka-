#!/usr/bin/env bash
# Linux/macOS: bash start.sh [start|stop|validate|reset|wipe|steps|auto|restart|setup]
set -Eeuo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
cd "$ROOT"
dc() { docker compose -f "$ROOT/docker-compose.yml" "$@"; }
# podman-compose no implementa `up --wait` ni `ps -a` (docker compose si).
# Se consulta la ayuda del subcomando para que el mismo script sirva a los dos.
dc_supports() {
  local help
  help="$(dc "$1" --help 2>&1 || true)"
  [[ "$help" == *"$2"* ]]
}
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
      # Sin `cd`: .pgdata es 700 y lo creo PostgreSQL con el uid del contenedor,
      # asi que el usuario del host no puede entrar y el cd abortaba el borrado
      # con "Permission denied". realpath resuelve enlaces sin leer el dir.
      resolved="$(realpath -m -- "$target" 2>/dev/null || printf '%s' "$target")"
      [[ "$resolved" == "$ROOT/"* ]] || { echo "Ruta fuera del proyecto: $target" >&2; return 1; }
    fi
  done
  dc --profile tools --profile bootstrap down --volumes --remove-orphans
  for target in "$ROOT/.pgdata" "$ROOT/data/batches"; do
    if [[ -d "$target" ]]; then
      # A container may own PostgreSQL files as uid 999. Empty only this verified bind.
      docker run --rm --mount "type=bind,source=$target,target=/data" docker.io/library/alpine:3.20 \
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
# Equivalente portable de `up --wait` para motores sin esa opcion (podman-compose):
# espera a que cada contenedor del proyecto este running y healthy (o sin
# healthcheck) o haya terminado bien, ya que airflow-init/superset-init salen con 0.
# Cada 15 s se imprimen los que bloquean; si un contenedor acaba en fallo
# (salida != 0) durante 15 s seguidos se aborta enseguida en vez de agotar los
# 600 s en silencio.
wait_services() {
  local deadline=$((SECONDS + 600)) ids id entry name status health code ready
  local report now_bad bad_ticks=0 last=0
  echo 'Esperando a que los servicios esten listos (max 600 s)...'
  while (( SECONDS < deadline )); do
    ids="$(dc ps -q 2>/dev/null || true)"
    ready=1; report=""; now_bad=""
    if [[ -z "$ids" ]]; then ready=0; report=" sin contenedores todavia"; fi
    while IFS= read -r id; do
      [[ -n "$id" ]] || continue
      entry="$(docker inspect --format '{{.Name}}|{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}|{{.State.ExitCode}}' "$id" 2>/dev/null || true)"
      [[ -n "$entry" ]] || continue
      IFS='|' read -r name status health code <<< "$entry"
      name="${name#/}"
      case "$status" in
        running) [[ "$health" == none || "$health" == healthy ]] || { ready=0; report+=" $name=$health"; };;
        exited)
          if [[ "$code" != 0 ]]; then ready=0; report+=" $name=fallo($code)"; now_bad+="$name "; fi;;
        *) ready=0; report+=" $name=$status";;
      esac
    done <<< "$ids"
    if (( ready == 1 )); then echo 'Servicios listos.'; return 0; fi
    if [[ -n "$now_bad" ]]; then bad_ticks=$((bad_ticks + 1)); else bad_ticks=0; fi
    if (( bad_ticks >= 3 )); then
      echo "Fallo al iniciar, contenedor(es) en error ->$report" >&2
      echo 'Revisa sus logs con: docker logs <nombre>  (o podman logs <nombre>)' >&2
      return 1
    fi
    if (( SECONDS - last >= 15 )); then echo "Esperando:$report"; last=$SECONDS; fi
    sleep 5
  done
  echo "Tiempo agotado, seguian sin estar listos ->$report" >&2
  return 1
}
start_project() {
  if dc_supports up --wait-timeout; then
    dc up -d --build --wait --wait-timeout 600
  else
    # sin --wait (podman-compose): levantar y esperar con la misma regla.
    dc up -d --build
    wait_services
  fi
  show_links
}
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
  # --no-deps es obligatorio con podman-compose: su `run` llama a `up` de las
  # dependencias con --remove-orphans fijo, y su hash del compose se calcula
  # sobre los servicios filtrados por --profile, asi que con otro perfil todo
  # contenedor existente parece obsoleto -> `down` de TODO el proyecto y luego
  # solo se recrean las dependencias (el resto de servicios queda borrado).
  # docker compose no hace eso, pero --no-deps existe en los dos motores.
  dc --profile bootstrap run --rm --build --no-deps superset-bootstrap
  dc exec -T superset /app/.venv/bin/python /app/scripts/superset_charts_bootstrap.py
  show_links
}
validate() {
  dc config --quiet
  # podman-compose no admite `-a` pero su `ps` ya incluye los detenidos.
  if dc_supports ps --all; then dc ps -a; else dc ps; fi
  dc exec -T airflow-scheduler python -m pip check
  # --no-deps: mismo motivo que en bootstrap; los servicios ya estan en marcha.
  dc --profile tools run --rm --build --no-deps -e GX_EVIDENCE_DIR=/tmp/etl-test-gx etl python -m pytest -q -p no:cacheprovider
}
step() {
  local answer
  read -r -p "$1 [Enter=continuar / q=cancelar]: " answer
  [[ "$answer" != q ]] || { echo 'Cancelado; se conservan pasos completados.'; return 1; }
}
read_demo_count() {
  local default_count="$1" answer
  while true; do
    read -r -p "Cantidad de eventos [1-1000] (Enter=$default_count): " answer || return 1
    answer="${answer%$'\r'}"
    if [[ -z "$answer" ]]; then printf '%s' "$default_count"; return; fi
    if [[ "$answer" =~ ^[1-9][0-9]{0,3}$ ]] && ((answer <= 1000)); then
      printf '%s' "$answer"; return
    fi
    echo 'Cantidad invalida: escribe un entero entre 1 y 1000.' >&2
  done
}
send_streaming_demo() {
  local flag="$1" engine="${2:-}"
  shift 2
  if [[ -z "$engine" ]]; then
    read -r -p 'Motor [1=Docker / 2=Podman] (Enter=1): ' engine
    engine="${engine:-1}"
  fi
  case "$engine" in
    1|docker) engine=docker;;
    2|podman) engine=podman;;
    *) echo 'Motor invalido: selecciona 1 (Docker) o 2 (Podman).' >&2; return 1;;
  esac
  command -v "$engine" >/dev/null || { echo "No se encontro $engine en PATH." >&2; return 1; }
  echo "Ejecutando demo: $engine exec streaming_producer_salud python -u -m scripts.streaming_demo $flag $*"
  if ! "$engine" exec streaming_producer_salud python -u -m scripts.streaming_demo "$flag" "$@"; then
    echo "La demo fallo. Comprueba que $engine y streaming_producer_salud esten iniciados y Kafka este disponible." >&2
    return 1
  fi
  if [[ "$flag" == --summary ]]; then echo 'Consulta terminada (solo lectura).'
  else echo 'Demo terminada. Revisa el dashboard en tiempo real y los logs del consumidor para comprobar su procesamiento.'; fi
}
execute() {
  local count
  case "$1" in help|--help|-h)
    echo 'bash start.sh [start|stop|validate|reset|wipe|steps|auto|restart|setup|links|kafka|kafka-live]'
    echo 'reset/restart conservan datos; wipe/steps/auto piden ELIMINAR y borran datos.'
    echo 'Demos: bash start.sh [inject-json-invalid|inject-alert|inject-invalid] [1|2]'
    echo 'Mas demos: bash start.sh [simulate-valid|simulate-burst|simulate-mixed|listen-events] [1|2]'
    echo 'Resumen: bash start.sh events-summary [1|2] (opcion 20, solo lectura)'
    echo 'Opciones 16/17/18 preguntan la cantidad de eventos (1-1000; Enter=60/300/60).'
    echo 'Motor: 1=Docker, 2=Podman. Opciones 13/14/15 publican eventos de prueba. Si omites el motor, se pregunta (Enter=1).'
    return;; esac
  case "$1" in
    10|links) show_links; return;;
    11|kafka) show_kafka; return;;
    12|kafka-live|live) show_kafka_live; return;;
    13|inject-json-invalid) send_streaming_demo --inject-json-invalid "${2:-}"; return;;
    14|inject-alert) send_streaming_demo --inject-alert "${2:-}"; return;;
    15|inject-invalid) send_streaming_demo --inject-invalid "${2:-}"; return;;
    16|simulate-valid) count=$(read_demo_count 60); send_streaming_demo --simulate "${2:-}" valid --count "$count" --interval 1; return;;
    17|simulate-burst) count=$(read_demo_count 300); send_streaming_demo --simulate "${2:-}" valid --count "$count" --interval 0.05; return;;
    18|simulate-mixed) count=$(read_demo_count 60); send_streaming_demo --simulate "${2:-}" mixed --count "$count" --interval 1; return;;
    19|listen-events) send_streaming_demo --listen "${2:-}" 20; return;;
    20|events-summary) send_streaming_demo --summary "${2:-}"; return;;
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
  execute "$1" "${2:-}"
else
  while true; do
    printf '\nMENU DEL PROYECTO\n'
    printf '  1  Iniciar servicios\n'
    printf '  2  Detener servicios\n'
    printf '  3  Validar instalacion\n'
    printf '  4  Resetear contenedores (conserva datos)\n'
    printf '  5  Vaciar volumenes y datos\n'
    printf '  6  Recrear paso a paso\n'
    printf '  7  Reconstruccion automatica completa\n'
    printf '  8  Reiniciar servicios\n'
    printf '  9  Instalar/cargar dashboards sin borrar\n'
    printf ' 10  Mostrar todos los enlaces del proyecto\n'
    printf ' 11  Mostrar estado, topicos, lag y recursos de Kafka\n'
    printf ' 12  Monitorear Kafka en tiempo real (Ctrl+C para salir)\n'
    printf ' 13  Demo: enviar JSON invalido (Docker/Podman)\n'
    printf ' 14  Demo: enviar alerta valida (Docker/Podman)\n'
    printf ' 15  Demo: enviar evento que incumple el contrato (Docker/Podman)\n'
    printf ' 16  Demo: eventos validos, uno por segundo (cantidad a elegir)\n'
    printf ' 17  Demo: rafaga de eventos validos (cantidad a elegir)\n'
    printf ' 18  Demo: eventos mixtos, metricas/alertas/invalidos (cantidad a elegir)\n'
    printf ' 19  Escuchar 20 mensajes nuevos de Kafka (abrir antes de simular)\n'
    printf ' 20  Resumen de eventos, estados y consumidor (solo lectura)\n'
    printf '  0  Salir\n'
    read -r -p 'Opcion: ' choice || break
    [[ "$choice" != 0 ]] || break
    # Run in its own shell: failures stop the sequence and return to the menu.
    if ! bash "$ROOT/start.sh" "$choice" "${2:-}"; then echo 'Operacion fallida o cancelada; revisa el mensaje anterior.' >&2; fi
  done
fi
