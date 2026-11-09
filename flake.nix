###############################################################
# flake.nix — ETL 2026-2: Health Colombia ODS 3 (entorno dev)
# Batch ETL (Airflow + Great Expectations) + Streaming (Kafka)
# + Dashboards (Superset batch / Streamlit tiempo real)
#
# Python 3.12 (Nix) + uv (.venv) + Podman/Docker Compose
#
# Uso:  nix develop            (entorno completo)
#       podman-compose up -d   (infraestructura, ver docker-compose.yml)
#       docker compose up -d   (alternativa equivalente si usas Docker)
###############################################################
{
  description = "ETL 2026-2 — Health Colombia ODS 3: ETL batch (Airflow, Great Expectations, DW) + streaming (Kafka) + dashboards";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs =
    { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };

      # El intérprete viene de Nix; los paquetes Python vienen de requirements.txt
      # y los instala uv al entrar al shell (ver shellHook).
      pythonEnv = pkgs.python312;
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          pythonEnv
          pkgs.uv
          # --- Contenedores: Podman (rootless) o Docker -------------------
          # En este equipo `docker` es el shim de podman. NO se incluye
          # pkgs.docker-compose (Go): si está en el PATH toma el control de
          # `docker compose` y no ve los contenedores de podman. Sin él,
          # `docker compose ...` delega en podman-compose y funciona igual.
          pkgs.podman
          pkgs.podman-compose
          # --- Utilidades de inspección y librerías del pipeline ---
          pkgs.rdkafka     # cliente nativo C para confluent-kafka
          pkgs.pkg-config
          pkgs.kcat        # kcat -L -b localhost:19092  (listar topics Kafka)
          pkgs.jq          # inspeccionar payloads JSON
          pkgs.postgresql  # cliente psql para consultar PostgreSQL
          pkgs.git
        ];

        LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
          pkgs.stdenv.cc.cc.lib
          pkgs.zlib
          pkgs.glib
          pkgs.libGL
          pkgs.openssl
        ];

        shellHook = ''
          PY_CURRENT="$(readlink -f "${pythonEnv}/bin/python")"
          REQ_FILE="$PWD/requirements.txt"
          REQ_HASH=""
          [ -f "$REQ_FILE" ] && REQ_HASH="$(sha256sum "$REQ_FILE" | cut -d' ' -f1)"

          # Socket Podman / Docker en NixOS (rootless)
          USER_PODMAN_SOCK="/run/user/$(id -u)/podman/podman.sock"
          if [ -S "$USER_PODMAN_SOCK" ]; then
            export CONTAINER_HOST="unix://$USER_PODMAN_SOCK"
            [ -z "$DOCKER_HOST" ] && export DOCKER_HOST="unix://$USER_PODMAN_SOCK"
            alias podman-remote="podman --remote"
          fi

          # 1) .venv contra el intérprete actual de Nix
          if [ ! -d .venv ] || [ "$(readlink -f .venv/bin/python 2>/dev/null)" != "$PY_CURRENT" ]; then
            echo "→ (re)creando .venv contra $PY_CURRENT"
            rm -rf .venv
            uv venv
          fi
          source .venv/bin/activate
          export PYTHONUNBUFFERED=1
          export PYTHONPATH="$PWD"

          # 2) dependencias de requirements.txt (solo si cambió el archivo)
          if [ -n "$REQ_HASH" ] && [ "$(cat .venv/.req-hash 2>/dev/null)" != "$REQ_HASH" ]; then
            echo "→ instalando requirements.txt con uv ..."
            if uv pip install -r "$REQ_FILE"; then
              printf '%s\n' "$REQ_HASH" > .venv/.req-hash
            else
              echo "⚠  No se pudo instalar requirements.txt (¿sin conexión?)."
              echo '   Vuelve a entrar con "nix develop" o corre: uv pip install -r requirements.txt'
            fi
          fi

          # 3) kernel de Jupyter (notebooks/01_data_validation_cleanup.ipynb)
          if [ ! -d "$HOME/.local/share/jupyter/kernels/etl-ods3" ]; then
            python -m ipykernel install --user --name=etl-ods3 \
              --display-name "Python (etl-ods3 .venv)" > /dev/null 2>&1 || true
          fi

          echo ""
          echo "╔══════════════════════════════════════════════════════════╗"
          echo "║  ETL 2026-2 — Health Colombia ODS 3 (batch + streaming)  ║"
          echo "╚══════════════════════════════════════════════════════════╝"
          echo ""
          echo "Python:  $(python --version 2>&1)"
          echo "uv:      $(uv --version)"
          echo "podman:  $(podman --version 2>&1)"
          echo "psql:    $(psql --version 2>&1 | cut -d' ' -f1-2)"
          echo ""
          echo "Quick start (paso a paso completo en README.md):"
          echo "  cp .env.example .env                          # configuración local"
          echo "  podman-compose up -d                          # postgres+airflow+superset+kafka+streaming"
          echo "  podman-compose run --rm etl python -m src.etl_main   # ETL batch manual"
          echo "  pytest -q                                     # suite de tests"
          echo "  python -m src.streaming.producer --mode replay --loop # simular eventos"
          echo "  python -m scripts.streaming_demo --listen 5   # ver eventos del topic"
          echo "  open http://localhost:8080 (Airflow) · 8088 (Superset) · 8501 (tiempo real)"
          echo ""
          echo "Extras:  uv pip install <pkg>  ·  jupyter lab  ·  kcat -L -b localhost:19092"
          echo ""
        '';
      };
    };
}
