from pathlib import Path

import pandas as pd

RAW_DIR = Path("data/raw")
SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")

SILVER_DIR.mkdir(parents=True, exist_ok=True)
GOLD_DIR.mkdir(parents=True, exist_ok=True)

ALERT_NORMALIZATION = {
    "battery_low": "battery_low",
    "temp_high": "temp_high",
    "offline": "offline",
    "motion_detected": "motion_detected",
    "motion-detected": "motion_detected",
    "battery low": "battery_low",
    "temp_high": "temp_high",
}


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing raw file: {path}")
    return pd.read_csv(path)


def clean_site_master(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.site_category = df.site_category.str.lower().fillna("unknown")
    return df


def clean_devices(df: pd.DataFrame, sites: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.status = df.status.str.lower().fillna("active")
    df = df.merge(
        sites[["site_id", "site_name", "city", "region"]],
        on="site_id",
        how="left",
    )
    return df


def normalize_alert_type(value):
    if pd.isna(value):
        return None
    value = str(value).strip()
    normalized = ALERT_NORMALIZATION.get(value.lower(), None)
    return normalized


def clean_events(events: pd.DataFrame, devices: pd.DataFrame, sites: pd.DataFrame) -> pd.DataFrame:
    df = events.copy()
    df = df.drop_duplicates()
    df["event_ts"] = pd.to_datetime(df["event_ts"], errors="coerce")
    df = df[df["event_ts"].notna()]
    df["alert_flag"] = df["alert_flag"].fillna(0).astype(int)
    df["alert_type"] = df["alert_type"].apply(normalize_alert_type)
    df.loc[df["alert_flag"] == 0, "alert_type"] = None
    df.loc[df["alert_type"].isna() & (df["alert_flag"] == 1), "alert_type"] = "unknown"

    df["battery_pct"] = pd.to_numeric(df["battery_pct"], errors="coerce")
    df.loc[(df["battery_pct"] < 0) | (df["battery_pct"] > 100), "battery_pct"] = pd.NA

    df["signal_strength"] = pd.to_numeric(df["signal_strength"], errors="coerce")
    df.loc[(df["signal_strength"] < -100) | (df["signal_strength"] > 0), "signal_strength"] = pd.NA

    df = df.merge(
        devices[["device_id", "site_id", "device_type", "status"]],
        on="device_id",
        how="left",
        suffixes=("", "_device"),
    )
    df = df.merge(
        sites[["site_id", "site_name", "city", "region"]],
        on="site_id",
        how="left",
        suffixes=("", "_site"),
    )
    df["event_date"] = df["event_ts"].dt.date
    df["event_hour"] = df["event_ts"].dt.floor("h")
    return df


def build_gold_views(events: pd.DataFrame, sites: pd.DataFrame) -> None:
    events = events.copy()
    events["is_alert"] = events["alert_flag"] == 1

    vw_site_hourly_health = (
        events.groupby(["event_hour", "site_id", "site_name"], dropna=False)
        .agg(
            event_count=("event_id", "count"),
            alert_count=("is_alert", "sum"),
            avg_temperature_c=("temperature_c", "mean"),
            avg_battery_pct=("battery_pct", "mean"),
        )
        .reset_index()
    )
    vw_site_hourly_health.to_csv(GOLD_DIR / "vw_site_hourly_health.csv", index=False)

    vw_device_alert_summary = (
        events.groupby(["device_id", "site_id"], dropna=False)
        .agg(
            total_events=("event_id", "count"),
            total_alerts=("is_alert", "sum"),
            battery_low_alerts=("alert_type", lambda s: (s == "battery_low").sum()),
            temp_high_alerts=("alert_type", lambda s: (s == "temp_high").sum()),
            offline_alerts=("alert_type", lambda s: (s == "offline").sum()),
        )
        .reset_index()
    )
    vw_device_alert_summary.to_csv(GOLD_DIR / "vw_device_alert_summary.csv", index=False)

    vw_site_daily_kpis = (
        events.groupby(["event_date", "site_id"], dropna=False)
        .agg(
            total_events=("event_id", "count"),
            unique_devices_reporting=("device_id", "nunique"),
            critical_alerts=("alert_type", lambda s: s.isin(["battery_low", "temp_high", "offline"]).sum()),
            avg_signal_strength=("signal_strength", "mean"),
        )
        .reset_index()
    )
    vw_site_daily_kpis.to_csv(GOLD_DIR / "vw_site_daily_kpis.csv", index=False)

    print(f"Saved gold views to {GOLD_DIR}")


def main():
    print("Loading raw data...")
    site_master = clean_site_master(load_csv(RAW_DIR / "site_master" / "site_master.csv"))
    devices = clean_devices(load_csv(RAW_DIR / "devices" / "devices.csv"), site_master)
    events = clean_events(load_csv(RAW_DIR / "sensor_events" / "sensor_events.csv"), devices, site_master)

    print("Writing silver outputs...")
    devices.to_csv(SILVER_DIR / "silver_devices.csv", index=False)
    site_master.to_csv(SILVER_DIR / "silver_sites.csv", index=False)
    events.to_csv(SILVER_DIR / "silver_sensor_events.csv", index=False)

    print("Building gold views...")
    build_gold_views(events, site_master)


if __name__ == "__main__":
    main()
