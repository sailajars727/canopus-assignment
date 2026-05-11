-- Gold view definitions for analytical consumption.

CREATE VIEW vw_site_hourly_health AS
SELECT
  DATE_TRUNC('hour', event_ts) AS hour,
  site_id,
  site_name,
  COUNT(*) AS event_count,
  SUM(CASE WHEN alert_flag = 1 THEN 1 ELSE 0 END) AS alert_count,
  AVG(temperature_c) AS avg_temperature_c,
  AVG(battery_pct) AS avg_battery_pct
FROM silver_sensor_events
GROUP BY 1, 2, 3;

CREATE VIEW vw_device_alert_summary AS
SELECT
  device_id,
  site_id,
  COUNT(*) AS total_events,
  SUM(CASE WHEN alert_flag = 1 THEN 1 ELSE 0 END) AS total_alerts,
  SUM(CASE WHEN alert_type = 'battery_low' THEN 1 ELSE 0 END) AS battery_low_alerts,
  SUM(CASE WHEN alert_type = 'temp_high' THEN 1 ELSE 0 END) AS temp_high_alerts,
  SUM(CASE WHEN alert_type = 'offline' THEN 1 ELSE 0 END) AS offline_alerts
FROM silver_sensor_events
GROUP BY device_id, site_id;

CREATE VIEW vw_site_daily_kpis AS
SELECT
  CAST(event_ts AS DATE) AS date,
  site_id,
  COUNT(*) AS total_events,
  COUNT(DISTINCT device_id) AS unique_devices_reporting,
  SUM(CASE WHEN alert_type IN ('battery_low', 'temp_high', 'offline') THEN 1 ELSE 0 END) AS critical_alerts,
  AVG(signal_strength) AS avg_signal_strength
FROM silver_sensor_events
GROUP BY 1, 2;
