-- adhoc 스키마로 작업 테이블 끌어오기
CREATE OR REPLACE TABLE project.adhoc.seoul_metro_congestion AS (
    SELECT *
    FROM project.analytics.seoul_metro_congestion
)

CREATE OR REPLACE TABLE project.adhoc.seoul_metro_congestion AS (
    SELECT
        DAY_TYPE,
        LINE,
        STATION_NUMBER,
        DEPARTURE_STATION,
        DIRECTION,
        TIME,
        CASE 
            WHEN TO_CHAR(TIME, 'HH24:MI') = '05:30' THEN 1
            WHEN TO_CHAR(TIME, 'HH24:MI') = '06:00' THEN 2
            WHEN TO_CHAR(TIME, 'HH24:MI') = '06:30' THEN 3
            WHEN TO_CHAR(TIME, 'HH24:MI') = '07:00' THEN 4
            WHEN TO_CHAR(TIME, 'HH24:MI') = '07:30' THEN 5
            WHEN TO_CHAR(TIME, 'HH24:MI') = '08:00' THEN 6
            WHEN TO_CHAR(TIME, 'HH24:MI') = '08:30' THEN 7
            WHEN TO_CHAR(TIME, 'HH24:MI') = '09:00' THEN 8
            WHEN TO_CHAR(TIME, 'HH24:MI') = '09:30' THEN 9
            WHEN TO_CHAR(TIME, 'HH24:MI') = '10:00' THEN 10
            WHEN TO_CHAR(TIME, 'HH24:MI') = '10:30' THEN 11
            WHEN TO_CHAR(TIME, 'HH24:MI') = '11:00' THEN 12
            WHEN TO_CHAR(TIME, 'HH24:MI') = '11:30' THEN 13
            WHEN TO_CHAR(TIME, 'HH24:MI') = '12:00' THEN 14
            WHEN TO_CHAR(TIME, 'HH24:MI') = '12:30' THEN 15
            WHEN TO_CHAR(TIME, 'HH24:MI') = '13:00' THEN 16
            WHEN TO_CHAR(TIME, 'HH24:MI') = '13:30' THEN 17
            WHEN TO_CHAR(TIME, 'HH24:MI') = '14:00' THEN 18
            WHEN TO_CHAR(TIME, 'HH24:MI') = '14:30' THEN 19
            WHEN TO_CHAR(TIME, 'HH24:MI') = '15:00' THEN 20
            WHEN TO_CHAR(TIME, 'HH24:MI') = '15:30' THEN 21
            WHEN TO_CHAR(TIME, 'HH24:MI') = '16:00' THEN 22
            WHEN TO_CHAR(TIME, 'HH24:MI') = '16:30' THEN 23
            WHEN TO_CHAR(TIME, 'HH24:MI') = '17:00' THEN 24
            WHEN TO_CHAR(TIME, 'HH24:MI') = '17:30' THEN 25
            WHEN TO_CHAR(TIME, 'HH24:MI') = '18:00' THEN 26
            WHEN TO_CHAR(TIME, 'HH24:MI') = '18:30' THEN 27
            WHEN TO_CHAR(TIME, 'HH24:MI') = '19:00' THEN 28
            WHEN TO_CHAR(TIME, 'HH24:MI') = '19:30' THEN 29
            WHEN TO_CHAR(TIME, 'HH24:MI') = '20:00' THEN 30
            WHEN TO_CHAR(TIME, 'HH24:MI') = '20:30' THEN 31
            WHEN TO_CHAR(TIME, 'HH24:MI') = '21:00' THEN 32
            WHEN TO_CHAR(TIME, 'HH24:MI') = '21:30' THEN 33
            WHEN TO_CHAR(TIME, 'HH24:MI') = '22:00' THEN 34
            WHEN TO_CHAR(TIME, 'HH24:MI') = '22:30' THEN 35
            WHEN TO_CHAR(TIME, 'HH24:MI') = '23:00' THEN 36
            WHEN TO_CHAR(TIME, 'HH24:MI') = '23:30' THEN 37
            WHEN TO_CHAR(TIME, 'HH24:MI') = '00:00' THEN 38
            WHEN TO_CHAR(TIME, 'HH24:MI') = '00:30' THEN 39
            ELSE NULL
        END AS INDEX,
        CONGESTION
    FROM project.adhoc.seoul_metro_congestion
)


CREATE OR REPLACE TABLE project.analytics.seoul_metro_congestion AS (
    SELECT *
    FROM project.adhoc.seoul_metro_congestion
)


-- test code
SELECT *
FROM project.analytics.seoul_metro_congestion
LIMIT 50;
