import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src import generate_data, ingest, transform


def test_generate_sites_default_count():
    sites = generate_data.generate_sites()
    assert len(sites) == 14
    assert set(["site_id", "site_name", "city", "region", "site_category"]).issubset(set(sites.columns))


def test_generate_devices_assigns_valid_site_ids():
    sites = pd.DataFrame(
        [
            {"site_id": "S001", "site_name": "Facility 1", "city": "Austin", "region": "North", "site_category": "warehouse"},
            {"site_id": "S002", "site_name": "Facility 2", "city": "Boston", "region": "East", "site_category": "office"},
        ]
    )
    devices = generate_data.generate_devices(sites, n_devices=10)
    assert len(devices) == 10
    assert devices.device_id.nunique() == 10
    assert devices.site_id.isin(sites.site_id).all()


def test_build_event_rows_includes_duplicate_records():
    devices = pd.DataFrame(
        [
            {"device_id": "D0001", "site_id": "S001", "device_type": "camera"},
            {"device_id": "D0002", "site_id": "S002", "device_type": "temperature_sensor"},
        ]
    )
    sites = pd.DataFrame(
        [
            {"site_id": "S001", "site_name": "Facility 1", "city": "Austin", "region": "North", "site_category": "warehouse"},
            {"site_id": "S002", "site_name": "Facility 2", "city": "Boston", "region": "East", "site_category": "office"},
        ]
    )
    events = generate_data.build_event_rows(devices, sites, n_events=100)
    assert len(events) == 101
    assert events.duplicated().sum() >= 1


def test_clean_site_master_lowercases_category():
    raw_sites = pd.DataFrame(
        [{"site_id": "S001", "site_name": "Facility 1", "city": "Austin", "region": "North", "site_category": "WAREHOUSE"}]
    )
    cleaned = transform.clean_site_master(raw_sites)
    assert cleaned.loc[0, "site_category"] == "warehouse"


def test_normalize_alert_type_variations():
    assert transform.normalize_alert_type("Battery Low") == "battery_low"
    assert transform.normalize_alert_type("TEMP_HIGH") == "temp_high"
    assert transform.normalize_alert_type(None) is None
    assert transform.normalize_alert_type("motion-detected") == "motion_detected"


def test_clean_events_invalid_values_and_deduplication():
    events = pd.DataFrame(
        [
            {
                "event_id": "E000001",
                "event_ts": "2026-05-01 12:00:00",
                "device_id": "D0001",
                "site_id": "S001",
                "temperature_c": 25.0,
                "battery_pct": -5,
                "signal_strength": 5,
                "alert_flag": 1,
                "alert_type": None,
            },
            {
                "event_id": "E000001",
                "event_ts": "2026-05-01 12:00:00",
                "device_id": "D0001",
                "site_id": "S001",
                "temperature_c": 25.0,
                "battery_pct": -5,
                "signal_strength": 5,
                "alert_flag": 1,
                "alert_type": None,
            },
        ]
    )
    devices = pd.DataFrame(
        [{"device_id": "D0001", "site_id": "S001", "device_type": "camera", "status": "ACTIVE"}]
    )
    sites = pd.DataFrame(
        [{"site_id": "S001", "site_name": "Facility 1", "city": "Austin", "region": "North", "site_category": "warehouse"}]
    )
    cleaned = transform.clean_events(events, devices, sites)
    assert len(cleaned) == 1
    assert pd.isna(cleaned.loc[0, "battery_pct"])
    assert pd.isna(cleaned.loc[0, "signal_strength"])
    assert cleaned.loc[0, "alert_type"] == "unknown"


def test_full_pipeline_integration(tmp_path, monkeypatch):
    source_dir = tmp_path / "data" / "source"
    raw_dir = tmp_path / "data" / "raw"
    silver_dir = tmp_path / "data" / "silver"
    gold_dir = tmp_path / "data" / "gold"
    source_dir.mkdir(parents=True)
    raw_dir.mkdir(parents=True)
    silver_dir.mkdir(parents=True)
    gold_dir.mkdir(parents=True)

    monkeypatch.setattr(generate_data, "OUTPUT_DIR", source_dir)
    monkeypatch.setattr(ingest, "SOURCE_DIR", source_dir)
    monkeypatch.setattr(ingest, "RAW_DIR", raw_dir)
    monkeypatch.setattr(transform, "RAW_DIR", raw_dir)
    monkeypatch.setattr(transform, "SILVER_DIR", silver_dir)
    monkeypatch.setattr(transform, "GOLD_DIR", gold_dir)

    generate_data.main()
    ingest.main()
    transform.main()

    assert (raw_dir / "site_master" / "site_master.csv").exists()
    assert (silver_dir / "silver_sites.csv").exists()
    assert (gold_dir / "vw_site_hourly_health.csv").exists()
