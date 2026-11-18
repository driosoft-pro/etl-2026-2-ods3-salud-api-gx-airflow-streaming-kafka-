param([string]$Action = 'menu', [string]$Engine = '')
$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..')).TrimEnd('\')
Set-Location -LiteralPath $projectRoot

function DC {
    & docker compose -f (Join-Path $projectRoot 'docker-compose.yml') @args
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose fallo (codigo $LASTEXITCODE)." }
}
function Get-ComposeHelp([string]$Sub) {
    # La ayuda viaja por stdout, pero el proveedor externo de podman avisa por
    # stderr y en Windows PowerShell eso puede lanzar: se silencia y se captura.
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'SilentlyContinue'
        $lines = @(docker compose -f (Join-Path $projectRoot 'docker-compose.yml') $Sub --help 2>&1)
        return ($lines | Out-String)
    } catch { return '' }
    finally { $ErrorActionPreference = $previous }
}
function Test-ComposeFlag([string]$Sub, [string]$Flag) {
    # podman-compose no implementa `up --wait` ni `ps -a`; docker compose si.
    return (Get-ComposeHelp $Sub).Contains($Flag)
}
function Get-ContainerState([string]$Id) {
    # Devuelve "nombre|estado|salud|codigo_de_salida" (una sola inspeccion).
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'SilentlyContinue'
        $lines = @(docker inspect --format '{{.Name}}|{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}|{{.State.ExitCode}}' $Id 2>&1)
        return (($lines | Out-String).Trim())
    } catch { return '' }
    finally { $ErrorActionPreference = $previous }
}
function Wait-Services([int]$TimeoutSec = 600) {
    # Equivalente portable de `up --wait` cuando el proveedor no lo admite:
    # running y healthy (o sin healthcheck), o one-shot terminado con codigo 0.
    # Cada 15 s imprime lo que bloquea; un contenedor en fallo 15 s seguidos
    # aborta enseguida en lugar de agotar los 600 s en silencio.
    Write-Host "Esperando a que los servicios esten listos (max $TimeoutSec s)..."
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    $last = Get-Date
    $badTicks = 0
    $report = ''
    do {
        $ready = $true
        $report = ''
        $nowBad = ''
        $ids = @((DC ps -q) | ForEach-Object { "$_".Trim() } | Where-Object { $_ })
        if ($ids.Count -eq 0) { $ready = $false; $report = ' sin contenedores todavia' }
        foreach ($id in $ids) {
            $state = Get-ContainerState $id
            $parts = @($state -split '\|')
            if (!$state -or $parts.Count -lt 4) { $ready = $false; continue }
            $name = $parts[0].TrimStart('/')
            $status = $parts[1]
            $health = $parts[2]
            $code = $parts[3]
            if ($status -eq 'running') {
                if ($health -ne 'none' -and $health -ne 'healthy') { $ready = $false; $report += " $name=$health" }
            } elseif ($status -eq 'exited') {
                if ($code -ne '0') { $ready = $false; $report += " $name=fallo($code)"; $nowBad += "$name " }
            } else { $ready = $false; $report += " $name=$status" }
        }
        if ($ready) { Write-Host 'Servicios listos.'; return }
        if ($nowBad) { $badTicks++ } else { $badTicks = 0 }
        if ($badTicks -ge 3) {
            throw "Fallo al iniciar, contenedor(es) en error ->$report Revisa sus logs con: docker logs <nombre>"
        }
        if (((Get-Date) - $last).TotalSeconds -ge 15) { Write-Host "Esperando:$report"; $last = Get-Date }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $deadline)
    throw "Tiempo agotado, seguian sin estar listos ->$report"
}
function Prepare {
    if (!(Test-Path -LiteralPath '.env')) { Copy-Item -LiteralPath '.env.example' -Destination '.env' }
    New-Item -ItemType Directory -Force -Path 'config' | Out-Null
    if (!(Test-Path -LiteralPath 'config/simple_auth_manager_passwords.json')) {
        '{"airflow":"airflow"}' | Set-Content -LiteralPath 'config/simple_auth_manager_passwords.json' -Encoding ascii
        Write-Host 'Acceso inicial Airflow: airflow / airflow. Revisa .env antes de exponer servicios.'
    }
    # Windows PowerShell can turn native stderr into a terminating exception.
    try {
        $null = & docker info 2>&1
        $dockerReady = $LASTEXITCODE -eq 0
    } catch { $dockerReady = $false }
    if (!$dockerReady) { throw 'Docker no esta disponible. Abre Docker Desktop, espera a que el motor Linux este listo y vuelve a intentarlo.' }
    DC config --quiet
}
function Confirm-Erase {
    Write-Host 'Se eliminaran contenedores y volumenes Compose de ESTE proyecto, PostgreSQL y lotes locales.' -ForegroundColor Yellow
    Write-Host 'Se conservan codigo, .env, CSV originales, notebook y evidencias. Otros proyectos no se tocan.'
    if ((Read-Host 'Escribe ELIMINAR para continuar') -cne 'ELIMINAR') { throw 'Operacion cancelada.' }
}
function Safe-DataPath([string]$Path) {
    $absolute = [IO.Path]::GetFullPath($Path)
    if (!$absolute.StartsWith($projectRoot + '\', [StringComparison]::OrdinalIgnoreCase)) {
        throw "Ruta de datos fuera del proyecto: $absolute. No se eliminara automaticamente."
    }
    # OneDrive cloud placeholders also have ReparsePoint; only known cloud tags
    # are allowed. Symbolic links, junctions and unknown reparse tags stay blocked.
    $current = $absolute
    while ($current.Length -gt $projectRoot.Length) {
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force
            if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) {
                if ($item.LinkType -or $item.Target) { throw "La ruta contiene un enlace o junction: $current" }
                $tagOutput = & fsutil reparsepoint query $current 2>&1 | Out-String
                if ($LASTEXITCODE -ne 0) { throw "No se pudo verificar la marca de la ruta: $current" }
                $match = [regex]::Match($tagOutput, '0x[0-9a-fA-F]{8}')
                if (!$match.Success) { throw "Marca de ruta desconocida: $current" }
                $tag = [Convert]::ToUInt32($match.Value.Substring(2), 16)
                $cloudMask = [Convert]::ToUInt32('FFFF0FFF', 16)
                $cloudTag = [Convert]::ToUInt32('9000001A', 16)
                if (($tag -band $cloudMask) -ne $cloudTag) {
                    throw "La ruta contiene una marca no permitida: $current"
                }
            }
        }
        $current = Split-Path -Parent $current
    }
    return $absolute
}
function Erase-Data {
    # Resolve configuration before down; PostgreSQL uses a bind mount, not only a named volume.
    $config = (DC config --format json | Out-String | ConvertFrom-Json)
    $mount = @($config.services.postgres.volumes | Where-Object { $_.target -eq '/var/lib/postgresql/data' })
    if ($mount.Count -ne 1 -or $mount[0].type -ne 'bind') { throw 'Montaje PostgreSQL inesperado; se cancela el borrado.' }
    $pgPath = Safe-DataPath $mount[0].source
    $batchPath = Safe-DataPath (Join-Path $projectRoot 'data/batches')
    # Only these approved data directories may be removed; never accept arbitrary workspace folders.
    if ($pgPath -ne (Join-Path $projectRoot '.pgdata')) { throw 'El borrado automatico requiere PGDATA_HOST_DIR=./.pgdata.' }
    DC --profile tools --profile bootstrap down --volumes --remove-orphans
    foreach ($target in @($pgPath, $batchPath)) {
        if (Test-Path -LiteralPath $target) { Remove-Item -LiteralPath $target -Recurse -Force }
    }
}
function Project-Port([string]$Name, [string]$Default) {
    $value = [Environment]::GetEnvironmentVariable($Name)
    if (!$value -and (Test-Path -LiteralPath '.env')) {
        foreach ($line in Get-Content -LiteralPath '.env') {
            if ($line -match ('^\s*' + [regex]::Escape($Name) + '\s*=(.*)$')) {
                $value = $Matches[1].Trim().Trim('"').Trim("'")
            }
        }
    }
    if (!$value) { $value = $Default }
    if ($value -notmatch '^\d+$' -or [int]$value -lt 1 -or [int]$value -gt 65535) { throw "Puerto invalido: $Name" }
    return $value
}
function Show-Links {
    $airflow = 'http://localhost:' + (Project-Port 'AIRFLOW_WEBSERVER_PORT' '8080')
    $superset = 'http://localhost:' + (Project-Port 'SUPERSET_PORT' '8088')
    $api = 'http://localhost:' + (Project-Port 'POSTGREST_PORT' '3220')
    Write-Host "`nENLACES DEL PROYECTO (requieren servicios iniciados)"
    Write-Host "Airflow: $airflow"
    Write-Host "DAG principal: $airflow/dags/reliable_health_pipeline"
    Write-Host "DAG de fallo controlado: $airflow/dags/bad_health_pipeline"
    Write-Host "Salud de Airflow: $airflow/api/v2/monitor/health"
    Write-Host "Superset: $superset"
    Write-Host "Dashboard BI: $superset/superset/dashboard/ods3-salud-colombia/"
    Write-Host "Dashboards: $superset/dashboard/list/"
    Write-Host "Graficos: $superset/chart/list/"
    Write-Host "Datasets: $superset/tablemodelview/list/"
    Write-Host "SQL Lab: $superset/sqllab/"
    Write-Host ('Dashboard tiempo real: http://localhost:' + (Project-Port 'REALTIME_DASHBOARD_PORT' '8501'))
    Write-Host "API PostgREST / esquema OpenAPI: $api/"
    foreach ($table in @('dim_time','dim_geografia','dim_regime','dim_facility','dim_capacity_type','fact_affiliates','fact_facility_capacity')) {
        Write-Host "API ${table}: $api/${table}?limit=10"
    }
    Write-Host ('PostgreSQL (DBeaver/pgAdmin, no navegador): localhost:' + (Project-Port 'POSTGRES_PORT' '5432'))
    Write-Host ('Kafka (cliente Kafka, no navegador): localhost:' + (Project-Port 'KAFKA_EXTERNAL_PORT' '19092'))
    Write-Host 'El dashboard BI requiere haber completado setup. Usa tus credenciales configuradas.'
}
function Show-Kafka {
    Write-Host "`nESTADO Y RECURSOS DE KAFKA"
    & docker inspect --format 'Contenedor={{.Name}} Estado={{.State.Status}} Salud={{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}} Reinicios={{.RestartCount}}' kafka_salud
    if ($LASTEXITCODE -ne 0) { Write-Host 'Kafka no esta creado o Docker no esta disponible.'; return }
    Write-Host "`nTopicos y particiones:"
    & docker exec kafka_salud /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
    Write-Host "`nDetalle del topico salud.metric.updates:"
    & docker exec kafka_salud /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic salud.metric.updates
    Write-Host "`nGrupos de consumidores:"
    & docker exec kafka_salud /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
    Write-Host "`nLag del consumidor health-metrics-consumers (si existe):"
    & docker exec kafka_salud /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group health-metrics-consumers
    Write-Host "`nUso de disco de los logs Kafka:"
    & docker exec kafka_salud sh -c 'du -sh /var/lib/kafka/data 2>/dev/null || true'
    Write-Host "`nUso de CPU y memoria del contenedor:"
    & docker stats kafka_salud --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}'
}
function Show-KafkaLive {
    Write-Host 'Monitoreo en tiempo real de Kafka. Presiona Ctrl+C para salir.' -ForegroundColor Cyan
    while ($true) {
        Clear-Host
        Write-Host ('KAFKA EN TIEMPO REAL - ' + (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')) -ForegroundColor Cyan
        Write-Host 'Actualizacion cada 5 segundos. Ctrl+C para volver.'
        & docker inspect --format 'Contenedor={{.Name}} Estado={{.State.Status}} Salud={{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}} Reinicios={{.RestartCount}}' kafka_salud
        Write-Host "`nCPU / memoria / red / disco:"
        & docker stats kafka_salud --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}'
        Write-Host "`nTopico salud.metric.updates:"
        & docker exec kafka_salud /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic salud.metric.updates
        Write-Host "`nLag del consumidor:"
        & docker exec kafka_salud /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group health-metrics-consumers 2>$null
        Write-Host "`nTamano de logs:"
        & docker exec kafka_salud sh -c 'du -sh /var/lib/kafka/data 2>/dev/null || true'
        Start-Sleep -Seconds 5
    }
}
function Start-Project {
    if (Test-ComposeFlag 'up' '--wait-timeout') {
        DC up -d --build --wait --wait-timeout 600
    } else {
        # sin --wait (podman-compose): levantar y esperar aqui mismo.
        DC up -d --build
        Wait-Services
    }
    Show-Links
}
function Load-Project {
    $runId = 'launcher_' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfff')
    # DAG discovery can take a short time after a fresh metadata database is created.
    $ready = $false
    for ($i = 0; $i -lt 60; $i++) {
        & docker compose exec -T airflow-scheduler airflow dags trigger reliable_health_pipeline --run-id $runId
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        Start-Sleep -Seconds 5
    }
    if (!$ready) { throw 'Airflow no pudo registrar/disparar el DAG. Revisa sus logs.' }
    $query = "from airflow.settings import Session; from airflow.models.dagrun import DagRun; s=Session(); r=s.query(DagRun).filter(DagRun.run_id=='$runId',DagRun.dag_id=='reliable_health_pipeline').one_or_none(); print('LAUNCH_STATE='+str(r.state if r else 'pending')); s.close()"
    $deadline = (Get-Date).AddMinutes(15)
    do {
        $state = DC exec -T airflow-scheduler python -c $query | Out-String
        if ($state -match 'LAUNCH_STATE=success') { Write-Host 'Carga Airflow exitosa.'; return }
        if ($state -match 'LAUNCH_STATE=failed') { throw "El DAG $runId fallo. Se cancela la creacion del dashboard." }
        Write-Host 'Esperando las ocho tareas de Airflow...'
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $deadline)
    throw "Tiempo de espera agotado para $runId. Consulta Airflow; no se borraran datos."
}
function Bootstrap {
    # --no-deps es obligatorio con podman-compose: su `run` llama a `up` de las
    # dependencias con --remove-orphans fijo y su hash del compose depende de
    # --profile, asi que considera obsoletos todos los contenedores, hace `down`
    # de TODO el proyecto y luego solo recrea las dependencias.
    DC --profile bootstrap run --rm --build --no-deps superset-bootstrap
    DC exec -T superset /app/.venv/bin/python /app/scripts/superset_charts_bootstrap.py
    Show-Links
}
function Validate {
    DC config --quiet
    # podman-compose no admite `-a` pero su `ps` ya incluye los detenidos.
    if (Test-ComposeFlag 'ps' '--all') { DC ps -a } else { DC ps }
    DC exec -T airflow-scheduler python -m pip check
    # --no-deps: mismo motivo que en Bootstrap; los servicios ya estan en marcha.
    DC --profile tools run --rm --build --no-deps -e GX_EVIDENCE_DIR=/tmp/etl-test-gx etl python -m pytest -q -p no:cacheprovider
}
function Step([string]$Text) {
    if ((Read-Host "$Text [Enter=continuar / q=cancelar]") -eq 'q') { throw 'Operacion cancelada; los pasos completados se conservan.' }
}
function Send-StreamingDemo([string]$Flag) {
    $selectedEngine = $Engine
    if (!$selectedEngine) { $selectedEngine = Read-Host 'Motor para esta demo [docker/podman] (Enter=docker)' }
    if (!$selectedEngine) { $selectedEngine = 'docker' }
    $selectedEngine = $selectedEngine.Trim().ToLowerInvariant()
    if ($selectedEngine -notin 'docker','podman') { throw 'Motor invalido: usa docker o podman.' }
    if (!(Get-Command $selectedEngine -ErrorAction SilentlyContinue)) { throw "No se encontro $selectedEngine en PATH." }
    Write-Host "Publicando evento de demostracion: $selectedEngine exec streaming_producer_salud python -m scripts.streaming_demo $Flag"
    & $selectedEngine exec streaming_producer_salud python -m scripts.streaming_demo $Flag
    if ($LASTEXITCODE -ne 0) { throw "La demo fallo (codigo $LASTEXITCODE). Comprueba que $selectedEngine y streaming_producer_salud esten iniciados y Kafka este disponible." }
    Write-Host 'Demo enviada. Revisa el dashboard en tiempo real y los logs del consumidor para comprobar su procesamiento.'
}
function Execute([string]$Choice) {
    if ($Choice -in 'help','--help','-h') {
        Write-Host 'start.bat [start|stop|validate|reset|wipe|steps|auto|restart|setup|links|kafka|kafka-live]'
        Write-Host 'reset/restart conservan datos; wipe/steps/auto piden ELIMINAR y borran datos.'
        Write-Host 'Demos: start.bat [inject-json-invalid|inject-alert|inject-invalid] [docker|podman]'
        Write-Host 'Opciones 13/14/15 publican eventos de prueba. Si omites el motor, se pregunta (Enter=docker).'
        return
    }
    if ($Choice -in '10','links') { Show-Links; return }
    if ($Choice -in '11','kafka') { Show-Kafka; return }
    if ($Choice -in '12','kafka-live','live') { Show-KafkaLive; return }
    if ($Choice -in '13','inject-json-invalid') { Send-StreamingDemo '--inject-json-invalid'; return }
    if ($Choice -in '14','inject-alert') { Send-StreamingDemo '--inject-alert'; return }
    if ($Choice -in '15','inject-invalid') { Send-StreamingDemo '--inject-invalid'; return }
    Prepare
    switch ($Choice) {
        { $_ -in '1','start' } { Start-Project }
        { $_ -in '2','stop' } { DC stop }
        { $_ -in '3','validate' } { Validate }
        { $_ -in '4','reset' } { DC down --remove-orphans; Start-Project }
        { $_ -in '5','wipe' } { Confirm-Erase; Erase-Data }
        { $_ -in '6','steps' } {
            Confirm-Erase
            Step '1/5: eliminar datos y contenedores'; Erase-Data
            Step '2/5: construir y levantar servicios'; Start-Project
            Step '3/5: ejecutar carga y esperar Airflow'; Load-Project
            Step '4/5: registrar dashboard y graficos'; Bootstrap
            Step '5/5: ejecutar validaciones'; Validate
        }
        { $_ -in '7','auto' } { Confirm-Erase; Erase-Data; Start-Project; Load-Project; Bootstrap; Validate }
        { $_ -in '8','restart' } { DC restart; Show-Links }
        { $_ -in '9','setup' } { Start-Project; Load-Project; Bootstrap }
        default { throw 'Opcion desconocida.' }
    }
}
try {
    if ($Action -eq 'menu') {
        do {
            Write-Host "`nMENU DEL PROYECTO"
            Write-Host '  1  Iniciar servicios'
            Write-Host '  2  Detener servicios'
            Write-Host '  3  Validar instalacion'
            Write-Host '  4  Resetear contenedores (conserva datos)'
            Write-Host '  5  Vaciar volumenes y datos'
            Write-Host '  6  Recrear paso a paso'
            Write-Host '  7  Reconstruccion automatica completa'
            Write-Host '  8  Reiniciar servicios'
            Write-Host '  9  Instalar/cargar dashboards sin borrar'
            Write-Host ' 10  Mostrar todos los enlaces del proyecto'
            Write-Host ' 11  Mostrar estado, topicos, lag y recursos de Kafka'
            Write-Host ' 12  Monitorear Kafka en tiempo real (Ctrl+C para salir)'
            Write-Host ' 13  Demo: enviar JSON invalido (Docker/Podman)'
            Write-Host ' 14  Demo: enviar alerta valida (Docker/Podman)'
            Write-Host ' 15  Demo: enviar evento que incumple el contrato (Docker/Podman)'
            Write-Host '  0  Salir'
            $choice = Read-Host 'Opcion'
            if ([string]::IsNullOrWhiteSpace($choice) -or $choice -eq '0') { break }
            try { Execute $choice } catch { Write-Host $_.Exception.Message -ForegroundColor Red }
        } while ($true)
    } else { Execute $Action }
} catch { Write-Error $_; exit 1 }
