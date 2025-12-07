/* stream-analytics/query.sql
   Input: IoTHubMessages (messages from IoT Hub)
   Outputs: CosmosDBOutput (alias to Cosmos DB), BlobOutput (alias to Blob Storage)
*/

WITH Parsed AS
(
    SELECT
        System.Timestamp() AS eventProcessedTime,
        CAST(GetArrayElement([body], 0) AS bigint) /* fallback if messages are arrays */ as _unused,
        -- assume incoming body is JSON, use telemetry fields directly
        IoTHub.MessageEnqueuedTime AS enqueuedTime,
        CAST(JSON_VALUE(body, '$.timestamp') AS datetime) AS readingTime,
        JSON_VALUE(body, '$.location') AS location,
        CAST(JSON_VALUE(body, '$.ice_thickness') AS float) AS ice_thickness,
        CAST(JSON_VALUE(body, '$.surface_temperature') AS float) AS surface_temperature,
        CAST(JSON_VALUE(body, '$.snow_accumulation') AS float) AS snow_accumulation,
        CAST(JSON_VALUE(body, '$.external_temperature') AS float) AS external_temperature
    FROM
        IoTHubMessages TIMESTAMP BY EnqueuedTime
)
-- aggregate every 5 minutes per location (tumbling window)
, Aggregated AS
(
    SELECT
        location,
        System.Timestamp() AS aggregationTime,
        DATEPART(year, System.Timestamp()) AS year,
        FORMATDATETIME(System.Timestamp(), 'yyyy-MM-dd') AS aggDate,
        FORMATDATETIME(System.Timestamp(), 'HH-mm') AS aggTime,
        COUNT(*) AS reading_count,
        AVG(ice_thickness) AS avg_ice_thickness,
        MIN(ice_thickness) AS min_ice_thickness,
        MAX(ice_thickness) AS max_ice_thickness,
        AVG(surface_temperature) AS avg_surface_temperature,
        MIN(surface_temperature) AS min_surface_temperature,
        MAX(surface_temperature) AS max_surface_temperature,
        MAX(snow_accumulation) AS max_snow_accumulation,
        AVG(external_temperature) AS avg_external_temperature
    FROM Parsed
    GROUP BY
        location,
        TumblingWindow(minute, 5)
)

/* Safety status logic: Safe: Ice >= 30 & SurfaceTemp <= -2
   Caution: Ice >=25 & SurfaceTemp <=0
   Else: Unsafe
*/
SELECT
    location,
    aggregationTime,
    reading_count,
    avg_ice_thickness,
    min_ice_thickness,
    max_ice_thickness,
    avg_surface_temperature,
    min_surface_temperature,
    max_surface_temperature,
    max_snow_accumulation,
    avg_external_temperature,
    -- Compute safetyStatus
    CASE
        WHEN avg_ice_thickness >= 30 AND avg_surface_temperature <= -2 THEN 'Safe'
        WHEN avg_ice_thickness >= 25 AND avg_surface_temperature <= 0 THEN 'Caution'
        ELSE 'Unsafe'
    END AS safetyStatus,
    -- Document id for Cosmos DB: location-<ISO timestamp>
    CONCAT(location, '-', FORMATDATETIME(aggregationTime, 'yyyy-MM-ddTHH:mm:ssZ')) AS id,
    -- Path for blob output (use in blob output configuration if required)
    CONCAT('aggregations/', FORMATDATETIME(aggregationTime, 'yyyy-MM-dd'), '/', FORMATDATETIME(aggregationTime, 'HH-mm'), '.json') AS blobPath
INTO
    CosmosDBOutput, BlobOutput
FROM
    Aggregated
