import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

OUTPUT_DIR = Path("data/source")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RANDOM_SEED = 2026
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

DEVICE_TYPES = ["camera", "temperature_sensor", "motion_sensor", "gateway"]
SITE_CATEGORIES = ["warehouse", "office", "retail"]
REGIONS = ["North", "South", "East", "West"]
CITIES = ["Austin", "Boston", "Chicago", "Denver", "Miami", "Seattle", "Phoenix", "Denver", "Portland"]
FIRMWARE_VERSIONS = ["1.0.0", "1.1.2", "1.2.0", "2.0.0", "2.1.1"]
STATUS_OPTIONS = ["active", "inactive", "maintenance"]
ALERT_TYPES = [None, "battery_low", "temp_high", "offline", "motion_detected"]


def generate_sites(n_sites=14):
    sites = []
    for i in range(1, n_sites + 1):
        site_id = f"S{i:03d}"
        region = random.choice(REGIONS)
        city = random.choice(CITIES)
        sites.append(
            {
                "site_id": site_id,
                "site_name": f"Facility {i}",
                "city": city,
                "region": region,
                "site_category": random.choice(SITE_CATEGORIES),
            }
        )
    return pd.DataFrame(sites)


def generate_devices(sites, n_devices=180):
    devices = []
    for i in range(1, n_devices + 1):
        site_id = sites.sample(1).iloc[0].site_id
        status = random.choices(STATUS_OPTIONS, weights=[0.75, 0.15, 0.1], k=1)[0]
        install_date = datetime.now() - timedelta(days=random.randint(30, 720))
        firmware = random.choice(FIRMWARE_VERSIONS)
        devices.append(
            {
                "device_id": f"D{i:04d}",
                "site_id": site_id,
                "device_type": random.choice(DEVICE_TYPES),
                "install_date": install_date.date().isoformat(),
                "firmware_version": firmware,
                "status": status,
            }
        )
    return pd.DataFrame(devices)


def random_temperature(device_type):
    if device_type == "temperature_sensor":
        return round(random.uniform(15.0, 32.0), 1)
    if device_type == "camera":
        return round(random.uniform(10.0, 30.0), 1)
    return round(random.uniform(12.0, 28.0), 1)


def random_battery():
    if random.random() < 0.05:
        return None
    value = round(random.uniform(0, 100), 1)
    if random.random() < 0.03:
        return random.choice([-15.0, 150.0, 999.0])
    return value


def random_signal():
    if random.random() < 0.04:
        return random.choice([-120, 200, None])
    return random.randint(-95, -35)


def build_event_rows(devices, sites, n_events=15000):
    events = []
    start_ts = datetime.now() - timedelta(days=7)
    device_lookup = devices.set_index("device_id")
    all_device_ids = devices.device_id.tolist()

    for idx in range(1, n_events + 1):
        device_id = random.choice(all_device_ids)
        device = device_lookup.loc[device_id]
        site_id = device.site_id
        ts = start_ts + timedelta(seconds=random.randint(0, 7 * 24 * 3600))
        alert_flag = random.random() < 0.12
        alert_type = random.choice(ALERT_TYPES) if alert_flag else None
        if alert_flag and alert_type is None:
            alert_type = random.choice(["battery_low", "temp_high", "offline", "motion_detected"])

        events.append(
            {
                "event_id": f"E{idx:06d}",
                "event_ts": ts.isoformat(sep=" ", timespec="seconds"),
                "device_id": device_id,
                "site_id": site_id,
                "temperature_c": random_temperature(device.device_type),
                "battery_pct": random_battery(),
                "signal_strength": random_signal(),
                "alert_flag": int(alert_flag),
                "alert_type": alert_type,
            }
        )

    # Add duplicates
    duplicates = random.sample(events, k=int(len(events) * 0.01))
    events.extend(duplicates)
    random.shuffle(events)
    return pd.DataFrame(events)


def write_csv(df, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def main():
    print("Generating site_master...")
    sites = generate_sites()
    write_csv(sites, OUTPUT_DIR / "site_master.csv")

    print("Generating devices...")
    devices = generate_devices(sites)
    write_csv(devices, OUTPUT_DIR / "devices.csv")

    print("Generating sensor_events...")
    events = build_event_rows(devices, sites)
    write_csv(events, OUTPUT_DIR / "sensor_events.csv")

    print(f"Wrote {len(sites)} sites, {len(devices)} devices, {len(events)} events to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
