CREATE OR REPLACE DATABASE PROJECT;

-- 먼저 3개의 스키마를 생성한다
CREATE OR REPLACE SCHEMA project.raw_data;
CREATE OR REPLACE SCHEMA project.analytics;
CREATE OR REPLACE SCHEMA project.adhoc;


-- 일단 1개의 테이블 raw_data 밑에 생성(수정필요)
CREATE TABLE project.raw_data.seoul_metro_congestion (
    `10_00` FLOAT,
    `10_30` FLOAT,
    `11_00` FLOAT,
    `11_30` FLOAT,
    `12_00` FLOAT,
    `12_30` FLOAT,
    `13_00` FLOAT,
    `13_30` FLOAT,
    `14_00` FLOAT,
    `14_30` FLOAT,
    `15_00` FLOAT,
    `15_30` FLOAT,
    `16_00` FLOAT,
    `16_30` FLOAT,
    `17_00` FLOAT,
    `17_30` FLOAT,
    `18_00` FLOAT,
    `18_30` FLOAT,
    `19_00` FLOAT,
    `19_30` FLOAT,
    `20_00` FLOAT,
    `20_30` FLOAT,
    `21_00` FLOAT,
    `21_30` FLOAT,
    `22_00` FLOAT,
    `22_30` FLOAT,
    `23_00` FLOAT,
    `23_30` FLOAT,
    `00_00` FLOAT,
    `00_30` FLOAT,
    `05_30` FLOAT,
    `06_00` FLOAT,
    `06_30` FLOAT,
    `07_00` FLOAT,
    `07_30` FLOAT,
    `08_00` FLOAT,
    `08_30` FLOAT,
    `09_00` FLOAT,
    `09_30` FLOAT,
    direction VARCHAR(10),
    departure_station VARCHAR(50),
    station_number INT,
    day_type VARCHAR(10),
    line VARCHAR(10),
    date INT
);

-- 일단 하나만 진행해보기 
COPY INTO project.raw_data.seoul_metro_congestion
from 's3://seoul-metro-s3-bucket/prod_data/서울교통공사_지하철혼잡도_전체데이터.csv'
credentials=(AWS_KEY_ID=(your_aws_key_id) AWS_SECRET_KEY=(your_aws_secret_key)
FILE_FORMAT = (type = 'CSV' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');


-- test(raw_data에서 SELECT문)
SELECT *
FROM project.raw_data.seoul_metro_congestion
LIMIT 10;

-- CTAS
CREATE TABLE project.adhoc.seoul_metro_congestion AS
SELECT *
FROM raw_data.seoul-metro-congestion

SELECT * FROM project.adhoc.seoul_metro_congestion LIMIT 10;



-- 월별 승하차 인원 테이블 뼈대 생성
CREATE OR REPLACE TABLE project.raw_data.seoul_metro_monthly (
    station_number INT,
    transportation_date VARCHAR(10),
    passenger_count INT,
    departure_station VARCHAR(20),
    line INT,
    date VARCHAR(10)
);

-- 월별 승하차 인원 테이블 COPY
COPY INTO project.raw_data.seoul_metro_monthly
from 's3://seoul-metro-s3-bucket/prod_data/서울교통공사_월별_승하차인원_전체데이터.csv'
credentials=(AWS_KEY_ID=(your_aws_key_id) AWS_SECRET_KEY=(your_aws_secret_key)
FILE_FORMAT = (type = 'CSV' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- test(raw_data에서 SELECT문)
SELECT *
FROM project.raw_data.seoul_metro_monthly
LIMIT 10;

-- 일별 승하차 인원 테이블 뼈대 생성
CREATE OR REPLACE TABLE project.raw_data.seoul_metro_day (
    Befere_06 FLOAT,
    time_06_07 FLOAT,
    time_07_08 FLOAT,
    time_08_09 FLOAT,
    time_09_10 FLOAT,
    time_10_11 FLOAT,
    time_11_12 FLOAT,
    time_12_13 FLOAT,
    time_13_14 FLOAT,
    time_14_15 FLOAT,
    time_15_16 FLOAT,
    time_16_17 FLOAT,
    time_17_18 FLOAT,
    time_18_19 FLOAT,
    time_19_20 FLOAT,
    time_20_21 FLOAT,
    time_21_22 FLOAT,
    time_22_23 FLOAT,
    time_23_24 FLOAT,
    after_24 FLOAT,
    transport_date DATE,
    weekday INT,
    boarding_type VARCHAR(10),
    station_name VARCHAR(50),
    station_id INT,
    line VARCHAR(10)
);

-- 일별 승하차 인원 테이블 COPY
COPY INTO project.raw_data.seoul_metro_day
from 's3://seoul-metro-s3-bucket/prod_data/서울교통공사_역별_일별_시간대별_승하차인원정보_전체데이터.csv'
credentials=(AWS_KEY_ID=(your_aws_key_id) AWS_SECRET_KEY=(your_aws_secret_key)
FILE_FORMAT = (type = 'CSV' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- test(raw_data에서 SELECT문)
SELECT *
FROM project.raw_data.seoul_metro_day
LIMIT 10;

-- CTAS
CREATE TABLE project.analytics.seoul_metro_day AS
SELECT *
FROM raw_data.seoul_metro_day

/*
-- CTAS
CREATE TABLE dev.analytics.mau_summary AS
SELECT
    TO_CHAR(A.ts, 'YYYY-MM') AS month,
    COUNT(DISTINCT B.userid) AS mau,
FROM raw_data.session_timestamp A
JOIN raw_data.user_session_channel B ON A.sessionid = B.sessionid
GROUP BY 1
ORDER BY 1 DESC;

SELECT * FROM dev.analytics.mau_summary;

-- 3개의 ROLE을 생성한다
CREATE ROLE analytics_users;
CREATE ROLE analytics_authors;
CREATE ROLE pii_users


*/





