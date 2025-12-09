---------------------------------------------------------
-- 1. Store RAW EVENTS into Blob Storage
---------------------------------------------------------
SELECT
    IoTHub.ConnectionDeviceId AS deviceId,
    timestamp,
    location,
    ice_thickness,
    surface_temperature,
    snow_accumulation,
    external_temperature
INTO
    [historical-data]
FROM
    [rideau-iothub];

---------------------------------------------------------
-- 2. AGGREGATED OUTPUT into Cosmos DB
--    5-minute tumbling window
---------------------------------------------------------
SELECT
    IoTHub.ConnectionDeviceId AS deviceId,
    location,                             -- REQUIRED for partition key /location

    AVG(ice_thickness) AS avg_ice_thickness,
    MIN(ice_thickness) AS min_ice_thickness,
    MAX(ice_thickness) AS max_ice_thickness,

    AVG(surface_temperature) AS avg_surface_temperature,
    MIN(surface_temperature) AS min_surface_temperature,
    MAX(surface_temperature) AS max_surface_temperature,

    MAX(snow_accumulation) AS max_snow_accumulation,
    AVG(external_temperature) AS avg_external_temperature,

    COUNT(*) AS reading_count,
    System.Timestamp AS event_time,

    CASE
        WHEN AVG(ice_thickness) >= 30 AND AVG(surface_temperature) <= -2 THEN 'Safe'
        WHEN AVG(ice_thickness) >= 25 AND AVG(surface_temperature) <= 0 THEN 'Caution'
        ELSE 'Unsafe'
    END AS safety_status

INTO
    [SensorAggregations]
FROM
    [rideau-iothub]
GROUP BY
    IoTHub.ConnectionDeviceId,
    location,
    TumblingWindow(second, 100);
