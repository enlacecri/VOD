"""
Tests para vod.sh.

Estrategia de aislamiento:
- Cada test usa tmp_path como raíz del proyecto.
- Se inyectan binarios mock en bin/ y .venv/bin/.
- curl mock devuelve código HTTP real (curl -w "%{http_code}") cuando se le pide,
  y JSON cuando se le pide cuerpo.
- Los tests de ingest que necesitan API activa escriben un api.pid falso con un
  proceso real (sleep) y mockean curl para retornar 200 en /health/live.
"""
import os
import sys
import stat
import subprocess
import textwrap
import pytest
from pathlib import Path

VOD_SH_ORIG = Path(__file__).parent.parent / "vod.sh"


@pytest.fixture
def env_root(tmp_path):
    """
    Monta un entorno mínimo para ejecutar vod.sh de forma aislada.
    Retorna (run_vod, tmp_path, bin_dir).
    """
    vod_sh_copy = tmp_path / "vod.sh"
    vod_sh_copy.write_text(VOD_SH_ORIG.read_text())
    vod_sh_copy.chmod(0o755)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    venv_bin = tmp_path / ".venv" / "bin"
    venv_bin.mkdir(parents=True)

    # Script bash que hace de proxy al python real, interceptando el worker_health_script
    real_python = sys.executable
    python_mock = f"""#!/bin/bash
if [[ "$1" == "-c" ]] && [[ "$2" == *"worker_health_script"* || "$2" == *"from rq import Worker"* ]]; then
    echo "vod_tasks"
    exit 0
fi
exec "{real_python}" "$@"
"""
    for name in ("python", "python3"):
        p = venv_bin / name
        p.write_text(python_mock)
        p.chmod(0o755)

    def mk(name: str, content: str, dirs=None):
        """Crea un ejecutable mock en los directorios indicados."""
        if dirs is None:
            dirs = [bin_dir, venv_bin]
        for d in dirs:
            f = d / name
            f.write_text(content)
            f.chmod(0o755)

    # ── Mocks básicos (exit 0) ──────────────────────────────────────────────
    for binary in ("docker", "ffmpeg", "ffprobe", "curl", "alembic", "rq"):
        mk(binary, "#!/bin/bash\nexit 0\n")

    # docker compose / docker-compose
    mk("docker-compose", "#!/bin/bash\nexit 0\n")

    # uvicorn: proceso que dura en background y puede recibir señales
    mk("uvicorn", "#!/bin/bash\ntrap 'exit 0' TERM INT\nwhile true; do sleep 0.1; done\n")

    # rq worker real is called via python src/scripts/run_worker.py
    # Creamos un dummy en src/scripts/run_worker.py para que python lo ejecute
    src_scripts = tmp_path / "src" / "scripts"
    src_scripts.mkdir(parents=True, exist_ok=True)
    run_worker_py = src_scripts / "run_worker.py"
    run_worker_py.write_text("import time\ntry:\n  while True: time.sleep(0.1)\nexcept KeyboardInterrupt:\n  pass\n")

    # curl mock base: sólo sale con 0 (se sobrescribe por test cuando hace falta)
    # La clave: cuando vod.sh llama  curl -s -o /dev/null -w "%{http_code}" URL
    # necesita que curl escriba SOLO el código numérico en stdout.
    # El mock base retorna 000 (API no disponible) a menos que se sobrescriba.
    mk("curl", textwrap.dedent("""\
        #!/bin/bash
        # Si se pide sólo http_code (-w "%{http_code}"), imprime 000
        # Detectamos si -w está en los argumentos
        write_format=""
        url=""
        silent=0
        output_dev_null=0
        for arg in "$@"; do
            case "$arg" in
                -s) silent=1 ;;
                /dev/null) output_dev_null=1 ;;
                %{http_code}) write_format="%{http_code}" ;;
            esac
        done
        # Extraer la URL (último argumento que empieza con http)
        for arg in "$@"; do
            if [[ "$arg" == http* ]]; then
                url="$arg"
            fi
        done
        if [[ "$write_format" == "%{http_code}" ]]; then
            echo "000"
        else
            echo "{}"
        fi
        exit 0
    """))

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    # Aseguramos que storage/input existe
    (tmp_path / "storage" / "input").mkdir(parents=True)

    def run_vod(cmd, *args):
        res = subprocess.run(
            [str(vod_sh_copy), cmd, *args],
            cwd=str(tmp_path),
            env=env,
            capture_output=True,
            text=True,
        )
        combined = res.stdout + "\n" + res.stderr
        return res.returncode, combined

    def inject_curl_api_alive(dirs=None):
        """
        Reemplaza el mock de curl para que simule la API activa.
        curl -w "%{http_code}" en /health/* → 200
        curl plano en /health/live → {"status":"alive"}
        curl plano en /health/ready → {"status":"ready"}
        POST /api/v1/assets → body + "\n201"
        """
        mk("curl", textwrap.dedent("""\
            #!/bin/bash
            write_format=""
            url=""
            for arg in "$@"; do
                case "$arg" in
                    %{http_code}) write_format="%{http_code}" ;;
                esac
            done
            for arg in "$@"; do
                if [[ "$arg" == http* ]]; then url="$arg"; fi
            done
            if [[ "$write_format" == "%{http_code}" ]]; then
                # Todas las health urls -> 200 (API activa)
                echo "200"
            elif [[ "$url" == */health/live* ]]; then
                echo '{"status":"alive"}'
            elif [[ "$url" == */health/ready* ]]; then
                echo '{"status":"ready"}'
            elif [[ "$url" == *"/api/v1/assets"* ]]; then
                echo '{"enlace_id":"ok","uuid":"00000000-0000-0000-0000-000000000001"}'
                echo "201"
            else
                echo "{}"
            fi
            exit 0
        """), dirs)

    def start_fake_api(dirs=None):
        """
        Arranca un proceso `sleep` como fake API y escribe su PID en api.pid.
        """
        (tmp_path / "storage" / "run").mkdir(parents=True, exist_ok=True)
        proc = subprocess.Popen(["sleep", "60"])
        (tmp_path / "storage" / "run" / "api.pid").write_text(str(proc.pid))
        return proc

    return run_vod, tmp_path, bin_dir, inject_curl_api_alive, start_fake_api, mk


# ── Tests de start ──────────────────────────────────────────────────────────

def test_start_rollback_on_health_fail(env_root):
    """Si la API no responde (curl devuelve 000), start hace rollback."""
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    # curl por defecto retorna 000 → healthcheck falla → rollback
    code, out = run_vod("start")
    assert code == 1
    assert "Healthcheck de la API falló" in out
    assert "Iniciando Rollback" in out
    # Después del rollback, no deben quedar PIDs
    assert not (tmp / "storage/run/api.pid").exists()


def test_start_writes_pid_files(env_root):
    """start con curl OK escribe api.pid y worker.pid."""
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    # Inyectamos curl que devuelve 200 para health
    inject_live()
    code, out = run_vod("start")
    assert code == 0, f"start falló:\n{out}"
    assert "Sistema VOD inicializado correctamente" in out
    assert (tmp / "storage/run/api.pid").exists()
    assert (tmp / "storage/run/worker.pid").exists()
    # Cleanup: matar procesos creados
    for pid_file in ["api.pid", "worker.pid"]:
        pf = tmp / "storage/run" / pid_file
        if pf.exists():
            try:
                os.kill(int(pf.read_text()), 15)
            except ProcessLookupError:
                pass


# ── Tests de status ─────────────────────────────────────────────────────────

def test_status_removes_obsolete_pid(env_root):
    """status limpia PIDs obsoletos (proceso 999999 no existe)."""
    run_vod, tmp, *_ = env_root
    (tmp / "storage/run").mkdir(parents=True, exist_ok=True)
    pid_file = tmp / "storage/run/api.pid"
    pid_file.write_text("999999")

    code, out = run_vod("status")
    assert code == 0
    assert "Detenida" in out
    assert not pid_file.exists()


# ── Tests de ingest – validación de ruta (sin API) ──────────────────────────

def test_ingest_rejects_missing_file(env_root):
    """ingest falla si el archivo no existe en INGEST_ROOT."""
    run_vod, tmp, *_ = env_root
    code, out = run_vod("ingest", "nofile.mp4")
    assert code == 1
    assert "Validación de ruta falló" in out


def test_ingest_rejects_path_traversal(env_root):
    """ingest rechaza rutas con '..' que escapan de INGEST_ROOT."""
    run_vod, tmp, *_ = env_root
    # Crear archivo fuera de storage/input
    outside = tmp / "storage" / "outside.mp4"
    outside.parent.mkdir(parents=True, exist_ok=True)
    outside.touch()

    code, out = run_vod("ingest", "../outside.mp4")
    assert code == 1
    assert "no puede contener componentes" in out


def test_ingest_rejects_symlink(env_root):
    """ingest rechaza archivos que son symlinks."""
    run_vod, tmp, *_ = env_root
    real = tmp / "storage/input/real.mp4"
    real.touch()
    sym = tmp / "storage/input/link.mp4"
    sym.symlink_to(real)

    code, out = run_vod("ingest", "link.mp4")
    assert code == 1
    assert "SYMLINK" in out


def test_ingest_rejects_invalid_id(env_root):
    """ingest rechaza archivos cuyo ID derivado tiene caracteres no permitidos."""
    run_vod, tmp, *_ = env_root
    (tmp / "storage/input/MY FILE.mp4").touch()

    code, out = run_vod("ingest", "MY FILE.mp4")
    assert code == 1
    assert "inválido" in out


def test_ingest_rejects_unknown_extension(env_root):
    """ingest rechaza extensiones no permitidas."""
    run_vod, tmp, *_ = env_root
    (tmp / "storage/input/video.xyz").touch()

    code, out = run_vod("ingest", "video.xyz")
    assert code == 1
    assert "no permitida" in out


# ── Tests de ingest – con API activa ────────────────────────────────────────

def test_ingest_success_201(env_root):
    """ingest llama a la API y retorna 0 cuando recibe 201."""
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    inject_live()
    (tmp / "storage/input/PREDI-MVIDA464.mp4").touch()

    code, out = run_vod("ingest", "PREDI-MVIDA464.mp4")
    assert code == 0, f"Esperaba éxito:\n{out}"
    assert "Registrado correctamente" in out


def test_ingest_uppercase_extension(env_root):
    """ingest acepta extensiones en mayúsculas (.MP4)."""
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    inject_live()
    (tmp / "storage/input/VIDEO.MP4").touch()

    code, out = run_vod("ingest", "VIDEO.MP4")
    assert code == 0, f"Esperaba éxito:\n{out}"
    assert "ID: VIDEO" in out or "VIDEO" in out


def test_ingest_reused_200(env_root):
    """ingest retorna 0 cuando la API devuelve 200 (activo reutilizado)."""
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root

    # curl devuelve 200 para health Y para POST
    mk("curl", textwrap.dedent("""\
        #!/bin/bash
        write_format=""
        url=""
        for arg in "$@"; do
            case "$arg" in
                %{http_code}) write_format="%{http_code}" ;;
            esac
        done
        for arg in "$@"; do
            if [[ "$arg" == http* ]]; then url="$arg"; fi
        done
        if [[ "$write_format" == "%{http_code}" ]]; then
            echo "200"
        elif [[ "$url" == *"/api/v1/assets"* ]]; then
            echo '{"enlace_id":"ok"}'
            echo "200"
        else
            echo "{}"
        fi
        exit 0
    """))

    (tmp / "storage/input/valid.mp4").touch()
    code, out = run_vod("ingest", "valid.mp4")
    assert code == 0
    assert "Activo reutilizado" in out


# ── Tests de logs ───────────────────────────────────────────────────────────

def test_logs_not_found(env_root):
    """logs informa que no hay archivos si storage/logs/services está vacío."""
    run_vod, *_ = env_root
    code, out = run_vod("logs")
    assert code == 0
    assert "No existen archivos de log todavía" in out


# ── Tests de ingest-all ─────────────────────────────────────────────────────

def test_ingest_all_counts(env_root):
    """ingest-all cuenta correctamente registrados y rechazados."""
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    inject_live()
    (tmp / "storage/input/file1.mp4").touch()
    (tmp / "storage/input/file 2.mp4").touch()   # ID inválido → rechazado

    code, out = run_vod("ingest-all")
    assert code == 0, f"ingest-all falló:\n{out}"
    assert "Archivos escaneados: 2" in out
    assert "Registrados:" in out
    assert "Rechazados:" in out


# ── Novedades / Cobertura adicional ──────────────────────────────────────────

def test_ingest_rejects_absolute_path(env_root):
    run_vod, tmp, *_ = env_root
    code, out = run_vod("ingest", "/tmp/video.mp4")
    assert code == 1
    assert "no puede ser absoluto" in out

def test_ingest_rejects_dotdot_component(env_root):
    run_vod, tmp, *_ = env_root
    code, out = run_vod("ingest", "programas/../PREDI-MVIDA464.mp4")
    assert code == 1
    assert "no puede contener componentes" in out

def test_ingest_rejects_symlink_dir(env_root):
    run_vod, tmp, *_ = env_root
    real_dir = tmp / "storage/input/real_dir"
    real_dir.mkdir(parents=True, exist_ok=True)
    (real_dir / "video.mp4").touch()
    
    sym_dir = tmp / "storage/input/link_dir"
    sym_dir.symlink_to(real_dir)
    
    code, out = run_vod("ingest", "link_dir/video.mp4")
    assert code == 1
    assert "SYMLINK" in out

def test_status_ignores_unrelated_pid(env_root):
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    (tmp / "storage/run").mkdir(parents=True, exist_ok=True)
    # Crear un proceso real que no es uvicorn
    import subprocess
    proc = subprocess.Popen(["sleep", "60"])
    (tmp / "storage/run/api.pid").write_text(str(proc.pid))
    try:
        code, out = run_vod("status")
        assert code == 0
        assert "Detenida" in out # Porque el PID existe pero el comm no es uvicorn
        assert not (tmp / "storage/run/api.pid").exists() # Lo limpia
    finally:
        proc.kill()

def test_start_fails_postgres_down(env_root, monkeypatch):
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    monkeypatch.setenv("VOD_HEALTH_RETRIES", "1")
    monkeypatch.setenv("VOD_HEALTH_INTERVAL", "0")
    mk("docker", "#!/bin/bash\nif [[ \"$*\" == *\"pg_isready\"* ]]; then exit 1; else exit 0; fi\n")
    code, out = run_vod("start")
    assert code == 1
    assert "Tiempo de espera agotado para DB/Redis" in out

def test_start_fails_redis_down(env_root, monkeypatch):
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    monkeypatch.setenv("VOD_HEALTH_RETRIES", "1")
    monkeypatch.setenv("VOD_HEALTH_INTERVAL", "0")
    mk("docker", "#!/bin/bash\nif [[ \"$*\" == *\"redis-cli\"* ]]; then exit 1; else exit 0; fi\n")
    code, out = run_vod("start")
    assert code == 1
    assert "Tiempo de espera agotado para DB/Redis" in out

def test_start_fails_alembic(env_root):
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    mk("alembic", "#!/bin/bash\nexit 1\n")
    code, out = run_vod("start")
    assert code == 1
    assert "Fallaron las migraciones" in out
    assert not (tmp / "storage/run/api.pid").exists()

def test_start_rollback_preserves_existing_api(env_root, monkeypatch):
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    # API preexistente
    import subprocess
    proc = subprocess.Popen(["bash", "-c", "exec -a uvicorn sleep 60"])
    (tmp / "storage/run").mkdir(parents=True, exist_ok=True)
    (tmp / "storage/run/api.pid").write_text(str(proc.pid))
    
    # Hacer que healthcheck falle
    monkeypatch.setenv("VOD_HEALTH_RETRIES", "1")
    monkeypatch.setenv("VOD_HEALTH_INTERVAL", "0")
    
    try:
        code, out = run_vod("start")
        assert code == 1
        assert "Rollback" in out
        # La API debe seguir corriendo
        assert proc.poll() is None
        assert (tmp / "storage/run/api.pid").exists()
    finally:
        proc.kill()

def test_start_rollback_preserves_existing_worker(env_root, monkeypatch):
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    # Worker preexistente
    import subprocess
    proc = subprocess.Popen(["bash", "-c", "exec -a 'run_worker' sleep 60"])
    (tmp / "storage/run").mkdir(parents=True, exist_ok=True)
    (tmp / "storage/run/worker.pid").write_text(str(proc.pid))
    
    # Hacer que healthcheck falle
    monkeypatch.setenv("VOD_HEALTH_RETRIES", "1")
    monkeypatch.setenv("VOD_HEALTH_INTERVAL", "0")
    
    try:
        code, out = run_vod("start")
        assert code == 1
        assert "Rollback" in out
        # Worker sigue corriendo
        assert proc.poll() is None
        assert (tmp / "storage/run/worker.pid").exists()
    finally:
        proc.kill()

def test_check_deps_fallback_venv(env_root):
    run_vod, tmp, *_ = env_root
    import shutil
    shutil.move(tmp / ".venv", tmp / "venv")
    code, out = run_vod("status") # Ejecuta check_deps
    assert code == 0

def test_ingest_quotes_backslashes(env_root):
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    inject_live()
    (tmp / "storage/input/file\"with\\quotes.mp4").touch()
    code, out = run_vod("ingest", r'file"with\quotes.mp4')
    # Debe rechazarlo porque derive_enlace_id falla en valid regex
    assert code == 1
    assert "inválido" in out

def test_ingest_all_newline(env_root):
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    inject_live()
    (tmp / "storage/input/file\nwithnewline.mp4").touch()
    code, out = run_vod("ingest-all")
    import re
    out = re.sub(r"\x1b\[[0-9;]*m", "", out)
    assert code == 0
    assert "Archivos escaneados: 1" in out
    assert "Rechazados:        1" in out # Por id inválido

def test_ingest_post_400(env_root):
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    mk("curl", "#!/bin/bash\nif [[ \"$*\" == *\"/health/live\"* ]]; then echo 200; elif [[ \"$*\" == *\"POST\"* ]]; then echo \"Bad Request\n400\"; fi\nexit 0\n")
    (tmp / "storage/input/video.mp4").touch()
    code, out = run_vod("ingest", "video.mp4")
    assert code == 1
    assert "Fallo al registrar (HTTP 400)" in out

def test_ingest_post_409(env_root):
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    mk("curl", "#!/bin/bash\nif [[ \"$*\" == *\"/health/live\"* ]]; then echo 200; elif [[ \"$*\" == *\"POST\"* ]]; then echo \"Conflict\n409\"; fi\nexit 0\n")
    (tmp / "storage/input/video.mp4").touch()
    code, out = run_vod("ingest", "video.mp4")
    assert code == 1
    assert "Fallo al registrar (HTTP 409)" in out

def test_ingest_post_500(env_root):
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    mk("curl", "#!/bin/bash\nif [[ \"$*\" == *\"/health/live\"* ]]; then echo 200; elif [[ \"$*\" == *\"POST\"* ]]; then echo \"Internal Server Error\n500\"; fi\nexit 0\n")
    (tmp / "storage/input/video.mp4").touch()
    code, out = run_vod("ingest", "video.mp4")
    assert code == 1
    assert "Fallo al registrar (HTTP 500)" in out

def test_ingest_all_exact_counts(env_root):
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    # 201 Created -> Registrados
    # 200 OK -> Reutilizados
    # Error (ej invalid ID, extension) -> Rechazados
    # 500 Error API -> Fallidos
    
    mk("curl", '''#!/bin/bash
write_format=""
url=""
for arg in "$@"; do
    case "$arg" in
        %{http_code}) write_format="%{http_code}" ;;
    esac
done
for arg in "$@"; do
    if [[ "$arg" == http* ]]; then url="$arg"; fi
done
if [[ "$write_format" == "%{http_code}" ]]; then
    echo "200"
elif [[ "$url" == *"/api/v1/assets"* ]]; then
    if [[ "$*" == *"file_created"* ]]; then echo -e "created\n201"
    elif [[ "$*" == *"file_reused"* ]]; then echo -e "reused\n200"
    elif [[ "$*" == *"file_failed"* ]]; then echo -e "error\n500"
    else echo -e "error\n400"
    fi
else
    echo "{}"
fi
exit 0
''')

    (tmp / "storage/input/file_created.mp4").touch()
    (tmp / "storage/input/file_reused.mp4").touch()
    (tmp / "storage/input/file_failed.mp4").touch()
    (tmp / "storage/input/file invalid.mp4").touch() # Rechazado por validación

    code, out = run_vod("ingest-all")
    import re
    out = re.sub(r"\x1b\[[0-9;]*m", "", out)
    assert code == 0
    assert "Archivos escaneados: 4" in out
    assert "Registrados:       1" in out
    assert "Reutilizados:      1" in out
    assert "Rechazados:        1" in out
    assert "Fallidos:          1" in out

def test_stop_preserves_storage(env_root):
    run_vod, tmp, *_ = env_root
    (tmp / "storage" / "db_data").mkdir(parents=True)
    (tmp / "storage" / "db_data" / "data.bin").touch()
    
    code, out = run_vod("stop")
    assert code == 0
    assert (tmp / "storage" / "db_data" / "data.bin").exists()


def test_start_fails_if_worker_listens_on_wrong_queue(env_root, monkeypatch):
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    inject_live()
    
    # Sobrescribimos el mock de python para devolver 'default' en vez de 'vod_tasks'
    venv_bin = tmp / ".venv" / "bin"
    real_python = sys.executable
    python_mock = f"""#!/bin/bash
if [[ "$1" == "-c" ]] && [[ "$2" == *"worker_health_script"* || "$2" == *"from rq import Worker"* ]]; then
    echo "default"
    exit 1
fi
exec "{real_python}" "$@"
"""
    for name in ("python", "python3"):
        p = venv_bin / name
        p.write_text(python_mock)
        p.chmod(0o755)

    code, out = run_vod("start")
    assert code == 1
    assert "Healthcheck del Worker falló" in out

def test_start_worker_arguments(env_root):
    run_vod, tmp, bin_dir, inject_live, start_fake, mk = env_root
    inject_live()
    
    # Interceptamos vod.sh para guardar qué argumentos recibe python cuando inicia run_worker.py
    run_worker_py = tmp / "src" / "scripts" / "run_worker.py"
    run_worker_py.write_text("""import sys
import time
with open('worker_args.txt', 'w') as f:
    f.write(' '.join(sys.argv))
try:
    while True: time.sleep(0.1)
except KeyboardInterrupt:
    pass
""")
    
    code, out = run_vod("start")
    assert code == 0
    
    # Give the background process a moment to write the file
    import time
    for _ in range(10):
        if (tmp / "worker_args.txt").exists():
            break
        time.sleep(0.1)
    
    args = (tmp / "worker_args.txt").read_text()
    assert "run_worker.py" in args
