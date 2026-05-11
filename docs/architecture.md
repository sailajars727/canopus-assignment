# Architecture Diagram

The pipeline is structured as a simple layered design:

```
[Source Data Generation]
        |
        v
[data/source/*.csv] -> [src/ingest.py] -> [data/raw/{devices,site_master,sensor_events}/]
        |
        v
[src/transform.py]
        |
        v
[data/silver/silver_devices.csv,
 data/silver/silver_sites.csv,
 data/silver/silver_sensor_events.csv]
        |
        v
[gold views]
        |
        v
[data/gold/vw_site_hourly_health.csv,
 data/gold/vw_device_alert_summary.csv,
 data/gold/vw_site_daily_kpis.csv]
        |
        v
[src/report.py] -> [screenshots/*.png, data/sample_output_files/report_summary.txt]
```

This design maps to a Bronze/Silver/Gold layering pattern commonly used in AWS data lakes.
