# Runbook operativo — VOD HLS

## Objetivo

Este documento cubre el arranque, verificación, monitorización y recuperación del API, PostgreSQL, Redis, workers RQ y almacenamiento HLS.

## Arranque y verificación

1. Iniciar dependencias con `docker compose up -d`.
2. Aplicar migraciones con `.venv/bin/alembic upgrade head`.
3. Iniciar la API y al menos un worker (`python -m src.scripts.run_worker`).
4. Verificar `GET /health/live` y `GET /health/ready`; ambos deben responder HTTP 200.
5. Ejecutar `.venv/bin/alembic check` y confirmar `No new upgrade operations detected`.

Readiness requiere PostgreSQL y Redis disponibles, `STAGING_ROOT` y `OUTPUT_ROOT` en el mismo filesystem, ambos escribibles y al menos `MIN_FREE_DISK_BYTES` libres.

## Señales a monitorizar

- HTTP 5xx y latencia de la API.
- Fallos o latencia de `/health/ready`.
- Profundidad y antigüedad de la cola `vod_tasks`.
- Jobs `pending` o `processing` sin heartbeat reciente.
- Assets en `failed`, agrupados por `error_code`.
- Espacio e inodos disponibles en staging, output y logs.
- Reinicios de workers y errores de FFmpeg/FFprobe.

El resumen para el dashboard está disponible en `GET /api/v1/admin/dashboard`. Las listas paginadas están en `/api/v1/admin/assets` y `/api/v1/admin/jobs`. Todos los endpoints administrativos y las acciones de retry requieren el encabezado `X-Admin-Key` con el valor configurado en `ADMIN_API_KEY`; si la clave no está configurada, la API administrativa responde 503.

La consola visual está disponible en `/admin`. La clave se mantiene únicamente en el almacenamiento de sesión del navegador y se elimina al cerrar sesión o cerrar la pestaña. El tablero se actualiza automáticamente cada 30 segundos.

Alertas iniciales recomendadas:

- Crítica: readiness falla durante 2 minutos.
- Crítica: espacio libre por debajo del mayor entre 10 GiB y 10 %.
- Alta: job procesando sin heartbeat durante `2 × RQ_JOB_TIMEOUT_SECONDS`.
- Alta: ningún worker activo con cola pendiente durante 5 minutos.
- Media: tasa de fallos superior al 5 % durante 15 minutos.

## Recuperación

### Redis o PostgreSQL no disponible

1. No fuerce retries mientras readiness sea 503.
2. Recupere el servicio afectado y confirme `/health/ready` en 200.
3. Ejecute `python -m src.scripts.reconcile_jobs` en modo conservador.
4. Revise jobs y assets fallidos antes de solicitar retries.

### Worker interrumpido o job estancado

1. Confirme que no exista un worker todavía ejecutando el mismo job.
2. Reinicie el worker.
3. Ejecute el reconciliador; este reencola jobs pendientes perdidos y marca como fallidos los jobs procesando cuyo heartbeat expiró.
4. Use el endpoint de retry correspondiente solo después de que el job anterior esté en estado terminal.

### Publicación presente con estado inconsistente

Ejecute `python -m src.scripts.reconcile_jobs`. El reconciliador revisa publicaciones existentes y corrige el estado persistido. Verifique el manifiesto y una reproducción de muestra antes de cerrar el incidente.

### Limpieza de staging

Primero ejecute `python -m src.scripts.reconcile_jobs` sin opciones; la limpieza será simulada. Revise los logs. Para eliminar únicamente huérfanos que superen `STAGING_ORPHAN_AGE_SECONDS`, ejecute de nuevo con `--clean-staging`.

## Rollback y respaldo

- Antes de desplegar, respalde PostgreSQL y conserve el artefacto de la versión anterior.
- Redis contiene estado de cola persistente, pero PostgreSQL es la fuente de verdad del dominio.
- No elimine manualmente rutas bajo staging/output durante un job activo.
- Si una migración requiere downgrade, pruebe el procedimiento con una copia de la base antes de producción.

## Cierre de incidente

Confirme readiness 200, cola drenándose, workers activos, ausencia de jobs estancados y reproducción HLS exitosa. Documente duración, activos afectados, causa raíz, acciones realizadas y prevención acordada.
