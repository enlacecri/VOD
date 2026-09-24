import sys
import shutil
import argparse
from pathlib import Path

from src.core.config import settings
from src.core.database import SessionLocal
from src.models.asset import Asset
from src.models.ingest_item import IngestItem


def reset_asset(identifier: str, copy_instead_of_move: bool = True):
    # Normalize identifier: strip directory and extension if full filename passed
    name = Path(identifier).stem

    db = SessionLocal()
    try:
        # Search asset by enlace_id or by vod_uuid
        asset = db.query(Asset).filter(
            (Asset.enlace_id == name) | (Asset.enlace_id == identifier)
        ).first()

        if not asset:
            # Try searching by UUID
            try:
                import uuid
                u = uuid.UUID(identifier)
                asset = db.query(Asset).filter(Asset.vod_uuid == u).first()
            except Exception:
                pass

        enlace_id = asset.enlace_id if asset else name
        vod_uuid_str = str(asset.vod_uuid) if asset else None

        print(f"[INFO] Desprocesando activo: {enlace_id} (UUID: {vod_uuid_str or 'No en DB'})")

        # 1. Clean output files (HLS)
        if asset and asset.manifest_path:
            manifest_file = Path(settings.OUTPUT_ROOT) / asset.manifest_path
            # Directory is usually: storage/output/EnlacePlus/_definst_/amlst:<UUID>/<ENLACE_ID>
            # Or the amlst folder: storage/output/EnlacePlus/_definst_/amlst:<UUID>
            asset_output_dir = manifest_file.parent
            if asset_output_dir.exists() and asset_output_dir.is_dir():
                print(f"[CLEANUP] Eliminando directorio de salida: {asset_output_dir}")
                shutil.rmtree(asset_output_dir, ignore_errors=True)
            
            # Check parent amlst directory if empty or specific to this asset
            amlst_dir = asset_output_dir.parent
            if amlst_dir.exists() and "amlst:" in amlst_dir.name and not any(amlst_dir.iterdir()):
                shutil.rmtree(amlst_dir, ignore_errors=True)

        # 2. Clean staging directory if any
        if vod_uuid_str:
            staging_dir = Path(settings.STAGING_ROOT) / vod_uuid_str
            if staging_dir.exists():
                print(f"[CLEANUP] Eliminando directorio staging: {staging_dir}")
                shutil.rmtree(staging_dir, ignore_errors=True)

        # 3. Source file: ensure it is in storage/input
        processed_dir = Path("storage/processed")
        input_dir = Path(settings.INGEST_ROOT)
        input_dir.mkdir(parents=True, exist_ok=True)

        # Check common extensions
        candidates = [
            f"{name}.mp4", f"{name}.mov", f"{name}.mkv", f"{name}.m4v", f"{name}.avi"
        ]
        if asset and asset.source_uri:
            candidates.insert(0, Path(asset.source_uri).name)

        restored_file = None
        for cand in candidates:
            processed_file = processed_dir / cand
            target_input_file = input_dir / cand

            if processed_file.exists():
                if copy_instead_of_move:
                    print(f"[RESTORE] Copiando {processed_file} -> {target_input_file}")
                    shutil.copy2(processed_file, target_input_file)
                else:
                    print(f"[RESTORE] Moviendo {processed_file} -> {target_input_file}")
                    shutil.move(str(processed_file), str(target_input_file))
                restored_file = target_input_file
                break
            elif target_input_file.exists():
                print(f"[OK] El archivo ya está listo en {target_input_file}")
                restored_file = target_input_file
                break

        # 4. Remove IngestItem records for this file if any
        ingest_items = db.query(IngestItem).filter(
            (IngestItem.filename.in_(candidates)) | 
            (IngestItem.filename == name)
        ).all()
        for item in ingest_items:
            print(f"[DB] Eliminando IngestItem {item.filename} (ID: {item.id})")
            db.delete(item)

        # 5. Remove Asset record (cascades to jobs, renditions, workflow_steps, events)
        if asset:
            print(f"[DB] Eliminando Asset {asset.enlace_id} (UUID: {asset.vod_uuid}) y sus tareas asociadas...")
            db.delete(asset)

        db.commit()
        print(f"[SUCCESS] Activo '{enlace_id}' desprocesado completamente.")
        if restored_file:
            print(f"[READY] Archivo disponible para demo en: {restored_file}")
            print(f"        Puedes probar con: ./vod.sh ingest {restored_file.name}")
        else:
            print(f"[NOTICE] No se encontró video fuente en storage/processed ni en storage/input.")
            print(f"         Coloca un video llamado '{name}.mp4' en storage/input/ para el demo.")
        return True

    except Exception as e:
        db.rollback()
        print(f"[ERROR] Error al desprocesar {identifier}: {e}", file=sys.stderr)
        return False
    finally:
        db.close()


def list_processed_assets():
    db = SessionLocal()
    try:
        assets = db.query(Asset).order_by(Asset.created_at.desc()).all()
        if not assets:
            print("No hay activos registrados en la base de datos.")
            return
        print(f"\n{'ENLACE_ID':<25} {'ESTADO':<12} {'UUID':<38} {'ARCHIVO'}")
        print("-" * 90)
        for a in assets:
            src = Path(a.source_uri).name if a.source_uri else "N/A"
            print(f"{a.enlace_id:<25} {a.status.value:<12} {str(a.vod_uuid):<38} {src}")
        print("-" * 90)
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(
        description="Desprocesar (resetear) un activo de video para volver a procesarlo en un demo."
    )
    parser.add_argument(
        "identifier",
        nargs="?",
        help="Enlace ID, nombre de archivo o UUID del activo a desprocesar (ej: PREDI-BAYLE539)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Listar todos los activos registrados actualmente",
    )
    parser.add_argument(
        "--move",
        action="store_true",
        help="Mover en lugar de copiar el archivo desde storage/processed a storage/input",
    )

    args = parser.parse_args()

    if args.list or not args.identifier:
        list_processed_assets()
        if not args.identifier:
            print("\nUso para desprocesar: python -m src.scripts.reset_asset <ENLACE_ID>")
            sys.exit(0)

    success = reset_asset(args.identifier, copy_instead_of_move=not args.move)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
