-- Silver layer SQL examples for cleaning raw sensor event data.

-- Clean site master
CREATE TABLE silver_sites AS
SELECT
  site_id,
  site_name,
  city,
  region,
  LOWER(site_category) AS site_category
FROM raw_site_master;

-- Clean devices and enrich with site details
CREATE TABLE silver_devices AS
SELECT
  d.device_id,
  d.site_id,
  d.device_type,
  d.install_date,
  d.firmware_version,
  LOWER(d.status) AS status,
  s.site_name,
  s.city,
  s.region
FROM raw_devices d
LEFT JOIN silver_sites s ON d.site_id = s.site_id;

-- Clean sensor events
CREATE TABLE silver_sensor_events AS
SELECT
  event_id,
  CAST(event_ts AS TIMESTAMP) AS event_ts,
  device_id,
  site_id,
  temperature_c,
  CASE
    WHEN battery_pct < 0 OR battery_pct > 100 THEN NULL
    ELSE battery_pct
  END AS battery_pct,
  CASE
    WHEN signal_strength < -100 OR signal_strength > 0 THEN NULL
    ELSE signal_strength
  END AS signal_strength,
  alert_flag,
  CASE
    WHEN alert_flag = 0 THEN NULL
    WHEN LOWER(alert_type) IN ('battery_low', 'temp_high', 'offline', 'motion_detected') THEN LOWER(alert_type)
    ELSE 'unknown'
  END AS alert_type
FROM (
  SELECT DISTINCT *
  FROM raw_sensor_events
) e;
