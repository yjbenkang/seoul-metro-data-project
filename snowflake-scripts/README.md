# snowflake-scripts

- setup-project : '서울교통공사_지하철혼잡도_전체데이터.csv', '서울교통공사_월별_승하차인원_전체데이터.csv', '서울교통공사_역별_일별_시간대별_승하차인원정보_전체데이터.csv' 테이블 s3에서 끌어오는 쿼리
- congestion-pivot : 혼잡도 데이터 피버팅한 seoul_metro_congestion 테이블 analytics에 생성하는 쿼리
- day-CTAS : seoul_metro_day 테이블 analytics에 생성하는 쿼리
