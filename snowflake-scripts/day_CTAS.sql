SELECT *
FROM project.raw_data.seoul_metro_day
LIMIT 50;

CREATE OR REPLACE TABLE project.analytics.seoul_metro_day AS (
    SELECT
        CONCAT(TRANSPORT_DATE, '-', REPLACE(time, '_', '-')) AS time,
        CASE time
            WHEN 'BEFERE_06' THEN 1
            WHEN 'TIME_06_07' THEN 2
            WHEN 'TIME_07_08' THEN 3
            WHEN 'TIME_08_09' THEN 4
            WHEN 'TIME_09_10' THEN 5
            WHEN 'TIME_10_11' THEN 6
            WHEN 'TIME_11_12' THEN 7
            WHEN 'TIME_12_13' THEN 8
            WHEN 'TIME_13_14' THEN 9
            WHEN 'TIME_14_15' THEN 10
            WHEN 'TIME_15_16' THEN 11
            WHEN 'TIME_16_17' THEN 12
            WHEN 'TIME_17_18' THEN 13
            WHEN 'TIME_18_19' THEN 14
            WHEN 'TIME_19_20' THEN 15
            WHEN 'TIME_20_21' THEN 16
            WHEN 'TIME_21_22' THEN 17
            WHEN 'TIME_22_23' THEN 18
            WHEN 'TIME_23_24' THEN 19
            WHEN 'AFTER_24' THEN 20
        END AS time_index,
        STATION_NAME,
        STATION_ID,
        LINE,
        WEEKDAY,
        BOARDING_TYPE,
        num
    FROM project.raw_data.seoul_metro_day
    UNPIVOT(
        num FOR time IN (
            BEFERE_06, TIME_06_07, TIME_07_08, TIME_08_09, TIME_09_10,
            TIME_10_11, TIME_11_12, TIME_12_13, TIME_13_14, TIME_14_15,
            TIME_15_16, TIME_16_17, TIME_17_18, TIME_18_19, TIME_19_20,
            TIME_20_21, TIME_21_22, TIME_22_23, TIME_23_24, AFTER_24
        )
    )
);

SELECT *
FROM project.analytics.seoul_metro_day
LIMIT 50;

-- 수정필요
CREATE OR REPLACE TABLE project.analytics.seoul_metro_day AS (
    SELECT
        transport_date,
        weekday,
        boarding_type ,
        station_name,
        station_id ,
        line,
        TO_TIMESTAMP(CONCAT('2024-09-30 ', SUBSTR(REPLACE(time, '_', ':'), 2, 5))) AS time,
        num
    FROM project.raw_data.seoul_metro_day
    UNPIVOT(
        num FOR time IN (
            Befere_06, time_06_07, time_07_08, time_08_09, time_09_10,
            time_10_11, time_11_12, time_12_13, time_13_14, time_14_15,
            time_15_16, time_16_17, time_17_18, time_18_19, time_19_20,
            time_20_21, time_21_22, time_22_23, time_23_24, after_24
        
    )
)


SELECT *
FROM project.analytics.seoul_metro_congestion
WHERE departure_station = '청량리'
LIMIT 50;
)
