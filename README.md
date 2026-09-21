# VOD MVP

Servicio de Video on Demand para la transcodificación y publicación HLS de video.

## Requisitos
- Docker y Docker Compose
- Python 3.11+ (o 3.14+ según entorno macOS ARM)
- `ffmpeg` y `ffprobe` instalados nativamente (Para Fase 2)

## Configuración
Copia el archivo `.env.example` a `.env` y ajusta las rutas absolutas según tu máquina:
```bash
cp .env.example .env
```
Asegúrate de que `INGEST_ROOT`, `STAGING_ROOT` y `OUTPUT_ROOT` apunten a directorios reales en tu entorno, y que `STAGING` y `OUTPUT` residan en el **mismo disco o volumen** para garantizar la promoción atómica.

## Inicio de Docker (Base de Datos y Nginx)
El proyecto utiliza PostgreSQL (Puerto 5433), Redis persistente (Puerto 6379) y Nginx (Puerto 8080).
```bash
docker-compose up -d
```

> **Nota de Puertos:** 
> - Nginx: 8080
> - PostgreSQL: 5433
> - Redis: 6379
> - API (FastAPI): 8000

## Instalación Local
Recomendamos instalar las dependencias con `pip` desde el archivo de configuración `pyproject.toml`.
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

## Migraciones (Alembic)
El esquema de base de datos se gestiona con Alembic. Antes de iniciar la API por primera vez, aplica la migración más reciente:
```bash
source .venv/bin/activate
alembic upgrade head
```

## Inicio de API
```bash
source .venv/bin/activate
uvicorn src.main:app --reload --port 8000
```
La documentación interactiva estará en `http://localhost:8000/docs`.

## Ejecución de Pruebas
Para ejecutar las pruebas:
```bash
source .venv/bin/activate
pytest tests/ -v
```

## Estado y operación

La transcodificación HLS, validación, publicación atómica y reconciliación están implementadas. La API expone:

- `GET /health/live`: confirma que el proceso HTTP está vivo.
- `GET /health/ready`: valida PostgreSQL, Redis, almacenamiento compartido, permisos de escritura y espacio libre.

## Gestión con `vod.sh`

El MVP del sistema provee un script centralizado `vod.sh` para gestionar los procesos (PostgreSQL, Redis, Nginx, API, Worker) de manera idempotente. 

Flujo recomendado:

```bash
./vod.sh start
cp "/ruta/del/video/PREDI-MVIDA464.mp4" storage/input/
./vod.sh ingest PREDI-MVIDA464.mp4
./vod.sh status
./vod.sh stop
```

Otras operaciones disponibles:
- `./vod.sh logs` y `./vod.sh logs --follow`
- `./vod.sh ingest-all` (Registra secuencialmente todos los archivos detectados en la carpeta de entrada).

Consulta el procedimiento de despliegue, alertas y recuperación en [docs/RUNBOOK.md](docs/RUNBOOK.md).
