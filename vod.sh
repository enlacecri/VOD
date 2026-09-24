#!/bin/bash

# VOD MVP Management Script
# ==============================================================================

# Cambiar al directorio donde reside el script (raíz del proyecto)
cd "$(dirname "$0")" || exit 1

# ==============================================================================
# Configuración y Variables
# ==============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

RUN_DIR="storage/run"
LOGS_DIR="storage/logs/services"
API_PID_FILE="$RUN_DIR/api.pid"
WORKER_PID_FILE="$RUN_DIR/worker.pid"
WORKER_PRIORITY_PID_FILE="$RUN_DIR/worker_priority.pid"
WORKER_INGEST_PID_FILE="$RUN_DIR/worker_ingest.pid"
WORKER_BATCH_PID_FILE="$RUN_DIR/worker_batch.pid"
WORKER_BACKUP_PID_FILE="$RUN_DIR/worker_backup.pid"
WORKER_SUBTITLES_PID_FILE="$RUN_DIR/worker_subtitles.pid"
WORKER_SYNC_PID_FILE="$RUN_DIR/worker_sync.pid"

API_LOG_FILE="$LOGS_DIR/api.log"
WORKER_LOG_FILE="$LOGS_DIR/worker.log"
WORKER_PRIORITY_LOG_FILE="$LOGS_DIR/worker_priority.log"
WORKER_INGEST_LOG_FILE="$LOGS_DIR/worker_ingest.log"
WORKER_BATCH_LOG_FILE="$LOGS_DIR/worker_batch.log"
WORKER_BACKUP_LOG_FILE="$LOGS_DIR/worker_backup.log"
WORKER_SUBTITLES_LOG_FILE="$LOGS_DIR/worker_subtitles.log"
WORKER_SYNC_LOG_FILE="$LOGS_DIR/worker_sync.log"

ALLOWED_EXTENSIONS=(".mp4" ".mov" ".mkv" ".mxf" ".avi" ".m4v")

# Extraer INGEST_ROOT, VOD_NEW_INGEST_ROOT, VOD_API_PORT y VOD_NGINX_PORT del .env o default
if [ -f .env ]; then
    INGEST_ROOT=$(grep -E "^INGEST_ROOT=" .env | cut -d '=' -f2 | tr -d '"' | tr -d "'")
    VOD_NEW_INGEST_ROOT=$(grep -E "^VOD_NEW_INGEST_ROOT=" .env | cut -d '=' -f2 | tr -d '"' | tr -d "'")
    VOD_API_PORT=$(grep -E "^VOD_API_PORT=" .env | cut -d '=' -f2 | tr -d '"' | tr -d "'")
    VOD_NGINX_PORT=$(grep -E "^VOD_NGINX_PORT=" .env | cut -d '=' -f2 | tr -d '"' | tr -d "'")
fi
INGEST_ROOT=${INGEST_ROOT:-storage/input}
INGEST_ROOT=$(echo "$INGEST_ROOT" | sed 's/^\.\///') # Limpiar ./ inicial si existe
VOD_NEW_INGEST_ROOT=${VOD_NEW_INGEST_ROOT:-storage/new_input}
VOD_NEW_INGEST_ROOT=$(echo "$VOD_NEW_INGEST_ROOT" | sed 's/^\.\///')
VOD_API_PORT=${VOD_API_PORT:-8005}
VOD_NGINX_PORT=${VOD_NGINX_PORT:-8085}

# ==============================================================================
# Funciones Auxiliares
# ==============================================================================
info() { echo -e "${CYAN}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; }
die() { error "$1"; exit 1; }

check_deps() {
    # Check docker & docker compose
    if ! command -v docker >/dev/null 2>&1; then
        die "Docker no está instalado o no está en el PATH."
    fi
    if ! docker compose version >/dev/null 2>&1 && ! docker-compose --version >/dev/null 2>&1; then
        die "Docker Compose no está instalado."
    fi
    if ! command -v ffmpeg >/dev/null 2>&1; then
        die "FFmpeg no está instalado."
    fi
    if ! command -v ffprobe >/dev/null 2>&1; then
        die "FFprobe no está instalado."
    fi
    if ! command -v python3 >/dev/null 2>&1; then
        die "Python3 no está instalado."
    fi
    if ! command -v curl >/dev/null 2>&1; then
        die "curl no está instalado."
    fi

    # Check venv
    if [ -d ".venv" ]; then
        VENV_BIN=".venv/bin"
        VENV_PYTHON=".venv/bin/python"
        export PATH="$PWD/$VENV_BIN:$PATH"
    elif [ -d "venv" ]; then
        VENV_BIN="venv/bin"
        VENV_PYTHON="venv/bin/python"
        export PATH="$PWD/$VENV_BIN:$PATH"
    else
        die "No se encontró entorno virtual (.venv o venv). Cree uno e instale dependencias antes de iniciar."
    fi
}

get_docker_compose_cmd() {
    if docker compose version >/dev/null 2>&1; then
        echo "docker compose"
    else
        echo "docker-compose"
    fi
}

check_pid_running() {
    local pid_file=$1
    local process_pattern=$2
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            if ps -p "$pid" -o command= | grep -E "$process_pattern" >/dev/null 2>&1; then
                return 0 # Está corriendo
            fi
        fi
        # PID obsoleto
        rm -f "$pid_file"
    fi
    return 1 # No está corriendo
}

graceful_kill() {
    local pid_file=$1
    local process_pattern=$2
    if check_pid_running "$pid_file" "$process_pattern"; then
        local pid=$(cat "$pid_file")
        info "Deteniendo proceso $pid ($process_pattern)..."
        kill -TERM "$pid"
        
        # Wait up to 5 seconds
        for i in {1..5}; do
            if ! kill -0 "$pid" 2>/dev/null; then
                success "Proceso $pid detenido correctamente."
                rm -f "$pid_file"
                return 0
            fi
            sleep 1
        done
        
        warning "Proceso $pid no respondió a SIGTERM. Enviando SIGKILL..."
        kill -KILL "$pid" 2>/dev/null
        rm -f "$pid_file"
        success "Proceso $pid eliminado forzosamente."
    else
        # info "El proceso de $pid_file no está activo."
        rm -f "$pid_file"
    fi
}

wait_for_db_redis() {
    local dc_cmd=$(get_docker_compose_cmd)
    local max_retries=${VOD_HEALTH_RETRIES:-15}
    local wait_seconds=${VOD_HEALTH_INTERVAL:-1}
    local db_ok=0
    local redis_ok=0
    
    for ((i=1; i<=max_retries; i++)); do
        if [ "$db_ok" -eq 0 ]; then
            if $dc_cmd exec -T db pg_isready -U vod_user -d vod_db >/dev/null 2>&1; then
                db_ok=1
            fi
        fi
        if [ "$redis_ok" -eq 0 ]; then
            if $dc_cmd exec -T redis redis-cli ping >/dev/null 2>&1; then
                redis_ok=1
            fi
        fi
        
        if [ "$db_ok" -eq 1 ] && [ "$redis_ok" -eq 1 ]; then
            return 0
        fi
        sleep $wait_seconds
    done
    return 1
}

# ==============================================================================
# Comandos principales
# ==============================================================================
cmd_start() {
    check_deps
    
    mkdir -p "$RUN_DIR" "$LOGS_DIR" "$INGEST_ROOT" "$VOD_NEW_INGEST_ROOT"
    
    local api_started_this_run=0
    local worker_started_this_run=0
    local worker_priority_started_this_run=0
    local worker_ingest_started_this_run=0
    local worker_batch_started_this_run=0
    local worker_backup_started_this_run=0
    local worker_subtitles_started_this_run=0
    local worker_sync_started_this_run=0

    # Docker Compose (Always ensure containers are up)
    info "Iniciando contenedores Docker..."
    local dc_cmd=$(get_docker_compose_cmd)
    $dc_cmd up -d || die "Error iniciando contenedores."
    
    info "Esperando disponibilidad de PostgreSQL y Redis..."
    if ! wait_for_db_redis; then
        die "Tiempo de espera agotado para DB/Redis."
    fi
    success "Bases de datos listas."

    # Migraciones siempre corren para asegurar esquema fresco
    echo -n "Ejecutando migraciones... "
    if alembic upgrade head > "$LOGS_DIR/alembic.log" 2>&1; then
        success "Migraciones completadas"
    else
        echo -e "${RED}[ERROR]${NC} Fallaron las migraciones."
        return 1
    fi

    # Iniciar API si no corre
    if check_pid_running "$API_PID_FILE" "uvicorn"; then
        warning "La API ya estaba iniciada (PID: $(cat $API_PID_FILE))."
    else
        echo -n "Iniciando API... "
        uvicorn src.main:app --host 0.0.0.0 --port "$VOD_API_PORT" > "$API_LOG_FILE" 2>&1 &
        echo $! > "$API_PID_FILE"
        api_started_this_run=1
        success "API iniciada (PID: $(cat $API_PID_FILE))"
    fi

    # --- Transcode Workers ---
    if check_pid_running "$WORKER_PID_FILE" "run_worker"; then
        warning "El Legacy Worker ya estaba iniciado (PID: $(cat $WORKER_PID_FILE))."
    else
        echo -n "Iniciando Legacy Worker (vod_tasks)... "
        "$VENV_PYTHON" src/scripts/run_worker.py --queue vod_tasks --name vod-legacy-worker > "$WORKER_LOG_FILE" 2>&1 &
        echo $! > "$WORKER_PID_FILE"
        worker_started_this_run=1
        success "Legacy Worker iniciado (PID: $(cat $WORKER_PID_FILE))"
    fi

    if check_pid_running "$WORKER_PRIORITY_PID_FILE" "run_worker"; then
        warning "El Priority Worker ya estaba iniciado (PID: $(cat $WORKER_PRIORITY_PID_FILE))."
    else
        echo -n "Iniciando Priority Worker (vod_priority)... "
        "$VENV_PYTHON" src/scripts/run_worker.py --queue vod_priority --name vod-priority-worker > "$WORKER_PRIORITY_LOG_FILE" 2>&1 &
        echo $! > "$WORKER_PRIORITY_PID_FILE"
        worker_priority_started_this_run=1
        success "Priority Worker iniciado (PID: $(cat $WORKER_PRIORITY_PID_FILE))"
    fi

    if check_pid_running "$WORKER_INGEST_PID_FILE" "run_worker"; then
        warning "El Ingest Worker ya estaba iniciado (PID: $(cat $WORKER_INGEST_PID_FILE))."
    else
        echo -n "Iniciando Ingest Worker (vod_ingest)... "
        "$VENV_PYTHON" src/scripts/run_worker.py --queue vod_ingest --name vod-ingest-worker > "$WORKER_INGEST_LOG_FILE" 2>&1 &
        echo $! > "$WORKER_INGEST_PID_FILE"
        worker_ingest_started_this_run=1
        success "Ingest Worker iniciado (PID: $(cat $WORKER_INGEST_PID_FILE))"
    fi

    if check_pid_running "$WORKER_BATCH_PID_FILE" "run_worker"; then
        warning "El Batch Worker ya estaba iniciado (PID: $(cat $WORKER_BATCH_PID_FILE))."
    else
        echo -n "Iniciando Batch Worker (vod_batch)... "
        "$VENV_PYTHON" src/scripts/run_worker.py --queue vod_batch --name vod-batch-worker > "$WORKER_BATCH_LOG_FILE" 2>&1 &
        echo $! > "$WORKER_BATCH_PID_FILE"
        worker_batch_started_this_run=1
        success "Batch Worker iniciado (PID: $(cat $WORKER_BATCH_PID_FILE))"
    fi

    # --- Post-process Workers ---
    if check_pid_running "$WORKER_BACKUP_PID_FILE" "run_worker"; then
        warning "El Backup Worker ya estaba iniciado (PID: $(cat $WORKER_BACKUP_PID_FILE))."
    else
        echo -n "Iniciando Backup Worker (vod_backup)... "
        "$VENV_PYTHON" src/scripts/run_worker.py --queue vod_backup --name vod-backup-worker > "$WORKER_BACKUP_LOG_FILE" 2>&1 &
        echo $! > "$WORKER_BACKUP_PID_FILE"
        worker_backup_started_this_run=1
        success "Backup Worker iniciado (PID: $(cat $WORKER_BACKUP_PID_FILE))"
    fi

    if check_pid_running "$WORKER_SUBTITLES_PID_FILE" "run_worker"; then
        warning "El Subtitles Worker ya estaba iniciado (PID: $(cat $WORKER_SUBTITLES_PID_FILE))."
    else
        echo -n "Iniciando Subtitles Worker (vod_subtitles)... "
        "$VENV_PYTHON" src/scripts/run_worker.py --queue vod_subtitles --name vod-subtitles-worker > "$WORKER_SUBTITLES_LOG_FILE" 2>&1 &
        echo $! > "$WORKER_SUBTITLES_PID_FILE"
        worker_subtitles_started_this_run=1
        success "Subtitles Worker iniciado (PID: $(cat $WORKER_SUBTITLES_PID_FILE))"
    fi

    if check_pid_running "$WORKER_SYNC_PID_FILE" "run_worker"; then
        warning "El Sync Worker ya estaba iniciado (PID: $(cat $WORKER_SYNC_PID_FILE))."
    else
        echo -n "Iniciando Sync Worker (vod_sync)... "
        "$VENV_PYTHON" src/scripts/run_worker.py --queue vod_sync --name vod-sync-worker > "$WORKER_SYNC_LOG_FILE" 2>&1 &
        echo $! > "$WORKER_SYNC_PID_FILE"
        worker_sync_started_this_run=1
        success "Sync Worker iniciado (PID: $(cat $WORKER_SYNC_PID_FILE))"
    fi

    # Check API health
    info "Esperando comprobaciones de salud de la API..."
    local max_retries=${VOD_HEALTH_RETRIES:-15}
    local wait=${VOD_HEALTH_INTERVAL:-1}
    local health_ok=0

    # Python script para leer y parsear json recibido por argumentos
    local health_script=$(cat << 'EOF'
import sys, json
try:
    live = json.loads(sys.argv[1])
    ready = json.loads(sys.argv[2])
    if live.get("status") == "alive" and ready.get("status") == "ready":
        sys.exit(0)
    sys.exit(1)
except Exception:
    sys.exit(1)
EOF
)

    for ((i=1; i<=max_retries; i++)); do
        local live_resp=$(curl -s "http://localhost:${VOD_API_PORT}/health/live" || echo "{}")
        local ready_resp=$(curl -s "http://localhost:${VOD_API_PORT}/health/ready" || echo "{}")
        if "$VENV_PYTHON" -c "$health_script" "$live_resp" "$ready_resp" 2>/dev/null; then
            health_ok=1
            break
        fi
        sleep $wait
    done

    if [ "$health_ok" -eq 0 ]; then
        error "Healthcheck de la API falló. Revisar logs en $API_LOG_FILE"
        
        # Rollback de lo que iniciamos
        info "Iniciando Rollback de procesos..."
        if [ "$api_started_this_run" -eq 1 ]; then
            graceful_kill "$API_PID_FILE" "uvicorn"
        fi
        if [ "$worker_started_this_run" -eq 1 ]; then
            graceful_kill "$WORKER_PID_FILE" "run_worker"
        fi
        if [ "$worker_priority_started_this_run" -eq 1 ]; then
            graceful_kill "$WORKER_PRIORITY_PID_FILE" "run_worker"
        fi
        if [ "$worker_ingest_started_this_run" -eq 1 ]; then
            graceful_kill "$WORKER_INGEST_PID_FILE" "run_worker"
        fi
        if [ "$worker_batch_started_this_run" -eq 1 ]; then
            graceful_kill "$WORKER_BATCH_PID_FILE" "run_worker"
        fi
        if [ "$worker_backup_started_this_run" -eq 1 ]; then
            graceful_kill "$WORKER_BACKUP_PID_FILE" "run_worker"
        fi
        if [ "$worker_subtitles_started_this_run" -eq 1 ]; then
            graceful_kill "$WORKER_SUBTITLES_PID_FILE" "run_worker"
        fi
        if [ "$worker_sync_started_this_run" -eq 1 ]; then
            graceful_kill "$WORKER_SYNC_PID_FILE" "run_worker"
        fi
        exit 1
    fi

    # Check Worker health
    info "Esperando comprobaciones de salud del Worker..."
    local worker_health_script=$(cat << 'EOF'
import sys, time
from redis import Redis
from rq import Worker
from src.core.config import settings

redis_conn = Redis.from_url(settings.REDIS_URL)
queue_name = settings.RQ_QUEUE_NAME

for _ in range(15):
    workers = Worker.all(connection=redis_conn)
    qnames = set()
    for w in workers:
        qnames.update(w.queue_names())
    if queue_name in qnames:
        print(", ".join(sorted(qnames)))
        sys.exit(0)
    time.sleep(1)
sys.exit(1)
EOF
)
    local worker_queue
    if worker_queue=$("$VENV_PYTHON" -c "$worker_health_script" 2>/dev/null); then
        success "Workers escuchando en las colas: $worker_queue"
    else
        error "Healthcheck del Worker falló. No se detectaron workers escuchando en las colas esperadas."
        
        # Rollback de lo que iniciamos
        info "Iniciando Rollback de procesos..."
        if [ "$api_started_this_run" -eq 1 ]; then
            graceful_kill "$API_PID_FILE" "uvicorn"
        fi
        if [ "$worker_started_this_run" -eq 1 ]; then
            graceful_kill "$WORKER_PID_FILE" "run_worker"
        fi
        if [ "$worker_priority_started_this_run" -eq 1 ]; then
            graceful_kill "$WORKER_PRIORITY_PID_FILE" "run_worker"
        fi
        if [ "$worker_ingest_started_this_run" -eq 1 ]; then
            graceful_kill "$WORKER_INGEST_PID_FILE" "run_worker"
        fi
        if [ "$worker_batch_started_this_run" -eq 1 ]; then
            graceful_kill "$WORKER_BATCH_PID_FILE" "run_worker"
        fi
        if [ "$worker_backup_started_this_run" -eq 1 ]; then
            graceful_kill "$WORKER_BACKUP_PID_FILE" "run_worker"
        fi
        if [ "$worker_subtitles_started_this_run" -eq 1 ]; then
            graceful_kill "$WORKER_SUBTITLES_PID_FILE" "run_worker"
        fi
        if [ "$worker_sync_started_this_run" -eq 1 ]; then
            graceful_kill "$WORKER_SYNC_PID_FILE" "run_worker"
        fi
        exit 1
    fi

    echo ""
    success "==========================================================="
    success " Sistema VOD inicializado correctamente."
    success "==========================================================="
    echo -e "API Base:    ${CYAN}http://localhost:${VOD_API_PORT}${NC}"
    echo -e "Swagger UI:  ${CYAN}http://localhost:${VOD_API_PORT}/docs${NC}"
    echo -e "Nginx / HLS: ${CYAN}http://localhost:${VOD_NGINX_PORT}${NC}"
    echo ""
    cmd_status
}

cmd_stop() {
    info "Deteniendo servicios VOD..."
    graceful_kill "$WORKER_SYNC_PID_FILE" "run_worker"
    graceful_kill "$WORKER_SUBTITLES_PID_FILE" "run_worker"
    graceful_kill "$WORKER_BACKUP_PID_FILE" "run_worker"
    graceful_kill "$WORKER_BATCH_PID_FILE" "run_worker"
    graceful_kill "$WORKER_INGEST_PID_FILE" "run_worker"
    graceful_kill "$WORKER_PRIORITY_PID_FILE" "run_worker"
    graceful_kill "$WORKER_PID_FILE" "run_worker"
    graceful_kill "$API_PID_FILE" "uvicorn"
    
    info "Deteniendo contenedores Docker..."
    local dc_cmd=$(get_docker_compose_cmd)
    if [ -n "$dc_cmd" ]; then
        $dc_cmd down || warning "Error al ejecutar docker compose down."
    fi
    success "Todos los servicios han sido detenidos."
}

cmd_status() {
    check_deps
    
    echo -e "\n${CYAN}--- ESTADO DE SERVICIOS ---${NC}"
    local dc_cmd=$(get_docker_compose_cmd)
    if [ "$dc_cmd" = "docker compose" ]; then
        $dc_cmd ps --format "table {{.Service}}\t{{.Status}}\t{{.Ports}}"
    else
        $dc_cmd ps
    fi

    echo -e "\n${CYAN}--- ESTADO DE PROCESOS ---${NC}"
    if check_pid_running "$API_PID_FILE" "uvicorn"; then
        echo -e "API:              ${GREEN}Corriendo${NC} (PID: $(cat $API_PID_FILE))"
    else
        echo -e "API:              ${RED}Detenida${NC}"
    fi

    echo -e "\n${CYAN}TRANSCODE WORKERS:${NC}"
    if check_pid_running "$WORKER_PRIORITY_PID_FILE" "run_worker"; then
        echo -e "Priority Worker:  ${GREEN}Corriendo${NC} (PID: $(cat $WORKER_PRIORITY_PID_FILE), Queue: vod_priority)"
    else
        echo -e "Priority Worker:  ${RED}Detenido${NC}"
    fi

    if check_pid_running "$WORKER_INGEST_PID_FILE" "run_worker"; then
        echo -e "Ingest Worker:    ${GREEN}Corriendo${NC} (PID: $(cat $WORKER_INGEST_PID_FILE), Queue: vod_ingest)"
    else
        echo -e "Ingest Worker:    ${RED}Detenido${NC}"
    fi

    if check_pid_running "$WORKER_BATCH_PID_FILE" "run_worker"; then
        echo -e "Batch Worker:     ${GREEN}Corriendo${NC} (PID: $(cat $WORKER_BATCH_PID_FILE), Queue: vod_batch)"
    else
        echo -e "Batch Worker:     ${RED}Detenido${NC}"
    fi

    if check_pid_running "$WORKER_PID_FILE" "run_worker"; then
        echo -e "Legacy Worker:    ${GREEN}Corriendo${NC} (PID: $(cat $WORKER_PID_FILE), Queue: vod_tasks)"
    else
        echo -e "Legacy Worker:    ${RED}Detenido${NC}"
    fi

    echo -e "\n${CYAN}POST-PROCESS WORKERS:${NC}"
    if check_pid_running "$WORKER_BACKUP_PID_FILE" "run_worker"; then
        echo -e "Backup Worker:    ${GREEN}Corriendo${NC} (PID: $(cat $WORKER_BACKUP_PID_FILE), Queue: vod_backup)"
    else
        echo -e "Backup Worker:    ${RED}Detenido${NC}"
    fi

    if check_pid_running "$WORKER_SUBTITLES_PID_FILE" "run_worker"; then
        echo -e "Subtitles Worker: ${GREEN}Corriendo${NC} (PID: $(cat $WORKER_SUBTITLES_PID_FILE), Queue: vod_subtitles)"
    else
        echo -e "Subtitles Worker: ${RED}Detenido${NC}"
    fi

    if check_pid_running "$WORKER_SYNC_PID_FILE" "run_worker"; then
        echo -e "Sync Worker:      ${GREEN}Corriendo${NC} (PID: $(cat $WORKER_SYNC_PID_FILE), Queue: vod_sync)"
    else
        echo -e "Sync Worker:      ${RED}Detenido${NC}"
    fi

    echo -e "\n${CYAN}--- HEALTHCHECKS ---${NC}"
    local live_http_code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:${VOD_API_PORT}/health/live" || echo "000")
    local ready_http_code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:${VOD_API_PORT}/health/ready" || echo "000")

    if [ "$live_http_code" = "200" ]; then
        echo -e "Live:   ${GREEN}OK${NC}"
    else
        echo -e "Live:   ${RED}FAIL ($live_http_code)${NC}"
    fi

    if [ "$ready_http_code" = "200" ]; then
        echo -e "Ready:  ${GREEN}OK${NC}"
    else
        echo -e "Ready:  ${RED}FAIL ($ready_http_code)${NC}"
    fi

    # Cola de trabajos RQ
    echo -e "\n${CYAN}--- JOBS EN COLA (RQ) ---${NC}"
    local rq_script=$(cat << 'EOF'
import sys
from redis import Redis
from rq import Queue
try:
    from src.core.config import settings
    from src.core.queues import ALL_QUEUES
    redis_conn = Redis.from_url(settings.REDIS_URL)
    for qname in ALL_QUEUES:
        q = Queue(name=qname, connection=redis_conn)
        queued = q.count
        started = q.started_job_registry.count
        failed = q.failed_job_registry.count
        print(f"[{qname}] Queued: {queued} | Started: {started} | Failed: {failed}")
except Exception as e:
    print(f"Error consultando Redis/RQ: {e}")
EOF
)
    "$VENV_PYTHON" -c "$rq_script" 2>/dev/null || echo "No disponible."
    echo ""
}

cmd_logs() {
    local follow=$1
    local files=""
    if [ -f "$API_LOG_FILE" ]; then
        files="$files $API_LOG_FILE"
    fi
    if [ -f "$WORKER_LOG_FILE" ]; then
        files="$files $WORKER_LOG_FILE"
    fi
    if [ -f "$WORKER_PRIORITY_LOG_FILE" ]; then
        files="$files $WORKER_PRIORITY_LOG_FILE"
    fi
    if [ -f "$WORKER_INGEST_LOG_FILE" ]; then
        files="$files $WORKER_INGEST_LOG_FILE"
    fi
    if [ -f "$WORKER_BATCH_LOG_FILE" ]; then
        files="$files $WORKER_BATCH_LOG_FILE"
    fi
    if [ -f "$WORKER_BACKUP_LOG_FILE" ]; then
        files="$files $WORKER_BACKUP_LOG_FILE"
    fi
    if [ -f "$WORKER_SUBTITLES_LOG_FILE" ]; then
        files="$files $WORKER_SUBTITLES_LOG_FILE"
    fi
    if [ -f "$WORKER_SYNC_LOG_FILE" ]; then
        files="$files $WORKER_SYNC_LOG_FILE"
    fi

    if [ -z "$files" ]; then
        error "No existen archivos de log todavía."
        return 0
    fi

    if [ "$follow" = "--follow" ] || [ "$follow" = "-f" ]; then
        tail -f $files
    else
        tail -n 50 $files
    fi
}

validate_extension() {
    local ext=$(echo "$1" | tr '[:upper:]' '[:lower:]')
    for allowed in "${ALLOWED_EXTENSIONS[@]}"; do
        if [ "$ext" = "$allowed" ]; then
            return 0
        fi
    done
    return 1
}

derive_enlace_id() {
    local base_name=$(basename "$1")
    # Extraer la extensión ignorando hidden files
    local ext=".${base_name##*.}"
    local name_no_ext="${base_name%.*}"
    
    if [ "$ext" = ".$base_name" ]; then
        ext=""
        name_no_ext="$base_name"
    fi

    if [ -n "$ext" ]; then
        if validate_extension "$ext"; then
            echo "$name_no_ext"
            return 0
        fi
        return 1
    fi
    
    echo "$base_name"
    return 0
}

process_single_ingest() {
    local source_uri=$1

    if [[ "$source_uri" == /* ]]; then
        error "source_uri no puede ser absoluto: $source_uri"
        return 1
    fi
    if [[ "$source_uri" == ".." ]] || [[ "$source_uri" == "../"* ]] || [[ "$source_uri" == *"/.." ]] || [[ "$source_uri" == *"/../"* ]]; then
        error "source_uri no puede contener componentes '..': $source_uri"
        return 1
    fi

    local physical_file="$INGEST_ROOT/$source_uri"

    # Verificación estricta de Path con Python
    local path_script=$(cat << 'EOF'
import sys, os
from pathlib import Path
try:
    root = Path(sys.argv[1]).resolve()
    # normpath normalises '..' without resolving symlinks
    norm_str = os.path.normpath(os.path.join(os.getcwd(), sys.argv[2]))
    target = Path(norm_str)

    try:
        target.relative_to(root)
    except ValueError:
        print("FUERA_DE_RAIZ")
        sys.exit(1)

    # Check every path component from root down to target for symlinks
    current = target
    while True:
        if current.is_symlink():
            print("SYMLINK")
            sys.exit(1)
        if current == root:
            break
        parent = current.parent
        if parent == current:
            break
        current = parent

    if not target.is_file():
        print("NO_ES_ARCHIVO_REGULAR")
        sys.exit(1)

    print("OK")
    sys.exit(0)
except Exception as e:
    print(e)
    sys.exit(1)
EOF
)
    
    local path_res
    if ! path_res=$("$VENV_PYTHON" -c "$path_script" "$INGEST_ROOT" "$physical_file"); then
        error "Validación de ruta falló ($path_res): $source_uri"
        return 1
    fi

    local derived_id
    if ! derived_id=$(derive_enlace_id "$source_uri"); then
        error "Extensión no permitida para el archivo: $source_uri"
        return 1
    fi

    if [ -z "$derived_id" ]; then
        error "Identificador derivado vacío: $source_uri"
        return 1
    fi

    if ! [[ "$derived_id" =~ ^[A-Za-z0-9_-]{1,128}$ ]]; then
        error "Identificador '$derived_id' inválido (caracteres no permitidos o longitud incorrecta)."
        return 1
    fi

    local live_http_code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:${VOD_API_PORT}/health/live" || echo "000")
    if [ "$live_http_code" != "200" ]; then
        error "La API no está lista. Asegúrese de ejecutar './vod.sh start' primero."
        return 1
    fi

    info "Ingestando: [ID: $derived_id] -> [URI: $source_uri]"

    # Json payload builder con Python (para escapar comillas, espacios, etc.)
    local payload_script=$(cat << 'EOF'
import sys, json
print(json.dumps({"enlace_id": sys.argv[1], "source_uri": sys.argv[2]}))
EOF
)
    local json_payload
    json_payload=$("$VENV_PYTHON" -c "$payload_script" "$derived_id" "$source_uri")

    local response=$(curl -s -w "\n%{http_code}" -X POST "http://localhost:${VOD_API_PORT}/api/v1/assets" \
         -H "Content-Type: application/json" \
         -d "$json_payload")

    local http_code=$(echo "$response" | tail -n 1)
    local body=$(echo "$response" | sed '$d')

    if [ "$http_code" = "201" ]; then
        success "Registrado correctamente. Respuesta:"
        echo "$body"
        return 0
    elif [ "$http_code" = "200" ]; then
        # 200 significa que fue reutilizado en el backend
        warning "Activo reutilizado (ya existía). Respuesta:"
        echo "$body"
        # Para ingest individual, 200 sigue siendo éxito (exit 0 al final).
        return 2
    else
        error "Fallo al registrar (HTTP $http_code). Respuesta:"
        echo "$body"
        return 3
    fi
}

cmd_ingest() {
    check_deps
    
    if [ -z "$1" ]; then
        die "Uso: ./vod.sh ingest <SOURCE_URI>"
    fi
    
    process_single_ingest "$1"
    local ret=$?
    
    # 0 = 201 Created
    # 2 = 200 Reused
    if [ $ret -eq 0 ] || [ $ret -eq 2 ]; then
        exit 0
    else
        exit 1
    fi
}

cmd_ingest_all() {
    check_deps
    
    local live_http_code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:${VOD_API_PORT}/health/live" || echo "000")
    if [ "$live_http_code" != "200" ]; then
        die "La API no está lista. Asegúrese de ejecutar './vod.sh start' primero."
    fi

    if [ ! -d "$INGEST_ROOT" ]; then
        die "El directorio $INGEST_ROOT no existe."
    fi

    info "Escaneando $INGEST_ROOT en busca de archivos (no se siguen symlinks)..."

    local total_files=0
    local registered=0
    local reused=0
    local rejected=0
    local failed=0

    # find files recursively, avoiding symlinks, process substitution
    while IFS= read -r -d '' file; do
        ((total_files++))
        local relative_path="${file#"$INGEST_ROOT/"}"
        
        process_single_ingest "$relative_path"
        local ret=$?
        
        if [ $ret -eq 0 ]; then
            ((registered++))
        elif [ $ret -eq 2 ]; then
            ((reused++))
        elif [ $ret -eq 1 ]; then
            ((rejected++))
        else
            ((failed++))
        fi
        echo "---"
    done < <(find "$INGEST_ROOT" -type f -not -type l -print0)

    echo -e "\n${CYAN}--- RESUMEN DE INGESTIÓN ---${NC}"
    echo "Archivos escaneados: $total_files"
    echo -e "Registrados:       ${GREEN}$registered${NC}"
    echo -e "Reutilizados:      ${YELLOW}$reused${NC}"
    echo -e "Rechazados:        ${RED}$rejected${NC}"
    echo -e "Fallidos:          ${RED}$failed${NC}"
}

cmd_catalog_import() {
    check_deps
    "$VENV_PYTHON" -m src.scripts.catalog_import "$@"
}

cmd_batch_enqueue() {
    check_deps
    "$VENV_PYTHON" -m src.scripts.batch_enqueue "$@"
}

cmd_prewarm_plan() {
    check_deps
    "$VENV_PYTHON" -m src.scripts.prewarm_cli plan "$@"
}

cmd_prewarm_run() {
    check_deps
    "$VENV_PYTHON" -m src.scripts.prewarm_cli run "$@"
}

cmd_new_video_scan() {
    check_deps
    "$VENV_PYTHON" -m src.scripts.new_video_scan "$@"
}

cmd_workflow_retry() {
    check_deps
    "$VENV_PYTHON" -m src.scripts.workflow_retry "$@"
}

cmd_reset() {
    check_deps
    "$VENV_PYTHON" -m src.scripts.reset_asset "$@"
}

cmd_help() {
    echo -e "${CYAN}VOD MVP Management Script${NC}"
    echo "========================="
    echo "Uso: ./vod.sh <comando> [argumentos]"
    echo ""
    echo "Comandos:"
    echo "  start           Inicializa bases de datos, migraciones, API y Workers."
    echo "  stop            Detiene la API, los Workers y la base de datos de manera segura."
    echo "  restart         Ejecuta stop y luego start."
    echo "  status          Muestra el estado de contenedores, procesos, colas y salud."
    echo "  logs            Muestra las últimas 50 líneas de los logs operativos."
    echo "  logs -f         Sigue en tiempo real los logs operativos (--follow)."
    echo "  ingest          Registra un archivo de origen. Uso: ./vod.sh ingest <SOURCE_URI>"
    echo "                  (Ej: ./vod.sh ingest programas/PREDI-MVIDA464.mp4)"
    echo "  ingest-all      Escanea y registra todos los archivos válidos en INGEST_ROOT."
    echo "  reset           Desprocesa/resetea un video para demos o pruebas."
    echo "                  Uso: ./vod.sh reset <ENLACE_ID> [--list] [--move]"
    echo "  catalog-import  Registro masivo del catálogo como assets COLD sin transcodificar."
    echo "                  Uso: ./vod.sh catalog-import [--dry-run] [--limit N] [--root PATH]"
    echo "  batch-enqueue   Encola un asset COLD para procesamiento en segundo plano (vod_batch)."
    echo "                  Uso: ./vod.sh batch-enqueue <VOD_UUID> [--enlace-id <ENLACE_ID>]"
    echo "  prewarm-plan    Calcula el plan de prewarming en modo 100% dry-run."
    echo "                  Uso: ./vod.sh prewarm-plan --ranking-file <FILE> [--top N] [--limit N]"
    echo "  prewarm-run     Ejecuta el prewarming enviando candidatos COLD a vod_batch."
    echo "                  Uso: ./vod.sh prewarm-run --ranking-file <FILE> [--top N] [--limit N] [--dry-run]"
    echo "  new-video-scan  Escanea nuevos videos en VOD_NEW_INGEST_ROOT con observación de estabilidad."
    echo "                  Uso: ./vod.sh new-video-scan [--dry-run] [--root PATH] [--stable-seconds N]"
    echo "  workflow-retry  Reintenta un paso fallido de post-procesamiento (AZURE_BACKUP, SUBTITLES, ENLACE_SYNC)."
    echo "                  Uso: ./vod.sh workflow-retry <ASSET> <STEP_TYPE> [--force]"
    echo ""
    echo "Ejemplo completo:"
    echo "  cp /ruta/del/video/PREDI-MVIDA464.mp4 storage/input/"
    echo "  ./vod.sh start"
    echo "  ./vod.sh ingest PREDI-MVIDA464.mp4"
    echo "  ./vod.sh status"
    echo "  ./vod.sh stop"
}

# ==============================================================================
# Entrypoint
# ==============================================================================
COMMAND=$1
shift

case "$COMMAND" in
    start)
        cmd_start
        ;;
    stop)
        cmd_stop
        ;;
    restart)
        cmd_stop
        sleep 2
        cmd_start
        ;;
    status)
        cmd_status
        ;;
    logs)
        cmd_logs "$@"
        ;;
    ingest)
        cmd_ingest "$@"
        ;;
    ingest-all)
        cmd_ingest_all
        ;;
    catalog-import)
        cmd_catalog_import "$@"
        ;;
    batch-enqueue)
        cmd_batch_enqueue "$@"
        ;;
    prewarm-plan)
        cmd_prewarm_plan "$@"
        ;;
    prewarm-run)
        cmd_prewarm_run "$@"
        ;;
    new-video-scan)
        cmd_new_video_scan "$@"
        ;;
    workflow-retry)
        cmd_workflow_retry "$@"
        ;;
    reset)
        cmd_reset "$@"
        ;;
    help|"")
        cmd_help
        ;;
    *)
        error "Comando desconocido: $COMMAND"
        cmd_help
        exit 1
        ;;
esac
