from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

GOLD_DIR = Path("data/gold")
SCREENSHOTS_DIR = Path("screenshots")
SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

sns.set(style="whitegrid")


def load_view(name: str) -> pd.DataFrame:
    path = GOLD_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing gold view: {path}")
    return pd.read_csv(path)


def total_events_and_alerts(events: pd.DataFrame):
    total_events = len(events)
    total_alerts = events[events["alert_flag"] == 1].shape[0]
    return total_events, total_alerts


def plot_top_devices(events: pd.DataFrame):
    summary = (
        events[events["alert_flag"] == 1]
        .groupby("device_id")
        .size()
        .reset_index(name="alert_count")
        .sort_values(by="alert_count", ascending=False)
        .head(5)
    )
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(data=summary, x="alert_count", y="device_id", color="tab:blue", ax=ax)
    ax.set_title("Top 5 Devices by Alert Count")
    ax.set_xlabel("Alert Count")
    ax.set_ylabel("Device ID")
    fig.tight_layout()
    path = SCREENSHOTS_DIR / "top_devices_by_alerts.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def plot_site_daily_trend(daily_kpis: pd.DataFrame):
    daily_kpis["event_date"] = pd.to_datetime(daily_kpis["event_date"])
    fig, ax = plt.subplots(figsize=(10, 5))
    top_sites = daily_kpis.groupby("site_id")["total_events"].sum().nlargest(4).index
    subset = daily_kpis[daily_kpis["site_id"].isin(top_sites)]
    sns.lineplot(data=subset, x="event_date", y="total_events", hue="site_id", marker="o", ax=ax)
    ax.set_title("Site-wise Daily Event Trend")
    ax.set_xlabel("Date")
    ax.set_ylabel("Total Events")
    fig.tight_layout()
    path = SCREENSHOTS_DIR / "site_daily_event_trend.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def plot_avg_battery_by_site(events: pd.DataFrame):
    summary = (
        events.groupby("site_id")["battery_pct"].mean().reset_index().sort_values(by="battery_pct", ascending=False)
    )
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.barplot(data=summary, x="site_id", y="battery_pct", color="tab:green", ax=ax)
    ax.set_title("Average Battery Level by Site")
    ax.set_xlabel("Site ID")
    ax.set_ylabel("Average Battery %")
    fig.tight_layout()
    path = SCREENSHOTS_DIR / "avg_battery_by_site.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def main():
    events = load_view("vw_site_hourly_health")
    device_alerts = load_view("vw_device_alert_summary")
    daily_kpis = load_view("vw_site_daily_kpis")

    # Re-load silver event details for some metrics
    silver_events = pd.read_csv(Path("data/silver/silver_sensor_events.csv"))

    total_events, total_alerts = total_events_and_alerts(silver_events)
    print(f"Total events: {total_events}")
    print(f"Total alerts: {total_alerts}")

    top_devices_path = plot_top_devices(silver_events)
    site_trend_path = plot_site_daily_trend(daily_kpis)
    battery_path = plot_avg_battery_by_site(silver_events)

    report_text = (
        f"Total events: {total_events}\n"
        f"Total alerts: {total_alerts}\n"
        f"Top 5 devices by alert count chart: {top_devices_path}\n"
        f"Site daily trend chart: {site_trend_path}\n"
        f"Average battery by site chart: {battery_path}\n"
    )
    report_path = Path("data/sample_output_files/report_summary.txt")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report_text)
    print(report_text)
    print(f"Saved report summary to {report_path}")


if __name__ == "__main__":
    main()
