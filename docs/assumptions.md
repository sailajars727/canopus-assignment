# Assumptions

- The pipeline is implemented locally using Python and structured folder layers to simulate cloud storage.
- A 7-day sensor event period is generated with hourly and random bursts of activity.
- Some imperfect records are intentionally included in source data:
  - missing battery values
  - invalid signal strength values
  - duplicate event rows
  - events from `inactive` devices
- Alerts are only valid when `alert_flag` is true; otherwise `alert_type` is null.
- Invalid battery and signal values are converted to null during cleaning.
- Duplicate `sensor_events` are removed by exact row deduplication.
- `alert_type` values are standardized and invalid values are normalized to null.
- The report uses gold outputs and local plots to demonstrate analytics readiness.
