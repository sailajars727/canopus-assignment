from pathlib import Path
import shutil

SOURCE_DIR = Path("data/source")
RAW_DIR = Path("data/raw")

FOLDERS = ["devices", "site_master", "sensor_events"]


def ingest_file(source_path: Path, target_dir: Path):
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / source_path.name
    shutil.copy2(source_path, target_path)
    print(f"Copied {source_path} -> {target_path}")


def main():
    if not SOURCE_DIR.exists():
        raise FileNotFoundError(f"Source directory not found: {SOURCE_DIR}")

    print("Ingesting source files into raw layer...")
    ingest_file(SOURCE_DIR / "site_master.csv", RAW_DIR / "site_master")
    ingest_file(SOURCE_DIR / "devices.csv", RAW_DIR / "devices")
    ingest_file(SOURCE_DIR / "sensor_events.csv", RAW_DIR / "sensor_events")

    note_path = RAW_DIR / "README_INGEST.md"
    note_path.write_text(
        "This directory contains bronze/raw files copied from data/source. "
        "Run src/ingest.py to refresh the raw ingest layer.\n"
    )
    print(f"Wrote ingestion note to {note_path}")


if __name__ == "__main__":
    main()
