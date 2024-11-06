CREATE OR REPLACE TABLE project.analytics.seoul_metro_congestion AS (
    SELECT
        day_type,
        line,
        station_number,
        departure_station,
        direction,
        TO_TIMESTAMP(CONCAT('2024-09-30 ', SUBSTR(REPLACE(time, '_', ':'), 2, 5))) AS time,
        congestion
    FROM project.raw_data.seoul_metro_congestion
    UNPIVOT(
        congestion FOR time IN (
            `10_00`, `10_30`, `11_00`, `11_30`, `12_00`, `12_30`, `13_00`, `13_30`,
            `14_00`, `14_30`, `15_00`, `15_30`, `16_00`, `16_30`, `17_00`, `17_30`,
            `18_00`, `18_30`, `19_00`, `19_30`, `20_00`, `20_30`, `21_00`, `21_30`,
            `22_00`, `22_30`, `23_00`, `23_30`, `00_00`, `00_30`, `05_30`, `06_00`,
            `06_30`, `07_00`, `07_30`, `08_00`, `08_30`, `09_00`, `09_30`
        )
    )
    WHERE date = '20240930'
)
