import requests
import pandas as pd
from io import BytesIO
import os
from dotenv import load_dotenv
from utils import save_to_s3

load_dotenv()

api_key = os.getenv("API_KEY")
base_url = os.getenv("BASE_URL")
bucket_name = os.getenv("S3_BUCKET_NAME")

column_mapping = {
    '06시이전': 'before_06',
    '06-07시간대': 'time_06_07',
    '07-08시간대': 'time_07_08',
    '08-09시간대': 'time_08_09',
    '09-10시간대': 'time_09_10',
    '10-11시간대': 'time_10_11',
    '11-12시간대': 'time_11_12',
    '12-13시간대': 'time_12_13',
    '13-14시간대': 'time_13_14',
    '14-15시간대': 'time_14_15',
    '15-16시간대': 'time_15_16',
    '16-17시간대': 'time_16_17',
    '17-18시간대': 'time_17_18',
    '18-19시간대': 'time_18_19',
    '19-20시간대': 'time_19_20',
    '20-21시간대': 'time_20_21',
    '21-22시간대': 'time_21_22',
    '22-23시간대': 'time_22_23',
    '23-24시간대': 'time_23_24',
    '24시이후': 'after_24',
    '수송일자': 'transport_date',
    '승하차구분': 'boarding_type',
    '역명': 'station_name',
    '역번호': 'station_id',
    '호선': 'line',
}
def fetch_hourly_passenger_data(endpoint: str):
    """
    역별 일별 시간대별 승하차 인원의 여러 API 엔드포인트에서 데이터를 가져옵니다.
    :param endpoint (str): 데이터를 가져올 API 엔드포인트.
    """
    all_data = pd.DataFrame()
    page = 1
    per_page = 1000
    while True:
        url = f"{base_url}{endpoint}?page={page}&perPage={per_page}&serviceKey={api_key}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch data from {endpoint} on page {page}: {response.status_code}")
            break
        data = response.json()
        if 'data' not in data or not data['data']:
            print(f"No more data to fetch for {endpoint}.")
            break
        df = pd.DataFrame(data['data'])
        df.rename(columns=column_mapping, inplace=True)
        if 'transport_date' in df.columns:
            df['transport_date'] = pd.to_datetime(df['transport_date'])
            df['weekday'] = df['transport_date'].dt.dayofweek
        if '연번' in df.columns:
            df.drop(columns=['연번'], inplace=True)
        column_order = [
            'before_06', 'time_06_07', 'time_07_08', 'time_08_09', 'time_09_10',
            'time_10_11', 'time_11_12', 'time_12_13', 'time_13_14', 'time_14_15',
            'time_15_16', 'time_16_17', 'time_17_18', 'time_18_19', 'time_19_20',
            'time_20_21', 'time_21_22', 'time_22_23', 'time_23_24', 'after_24',
            'transport_date', 'weekday', 'boarding_type', 'station_name',
            'station_id', 'line'
        ]
        df = df.reindex(columns=column_order)
        all_data = pd.concat([all_data, df], ignore_index=True)
        print(f"{endpoint} - Page {page} data fetched and added.")
        page += 1

    # 최종 데이터를 CSV로 변환하고 S3에 업로드
    csv_buffer = BytesIO()
    all_data.to_csv(csv_buffer, index=False, encoding='utf-8')
    csv_buffer.seek(0)
    return csv_buffer

api_endpoint = "/15048032/v1/uddi:bff5665b-6c0b-4d2f-80ff-9af01761c355"
output_file = "prod_data/서울교통공사_역별_일별_시간대별_승하차인원정보_전체데이터.csv"
csv_buffer = fetch_hourly_passenger_data(api_endpoint)
save_to_s3(csv_buffer, bucket_name, output_file)
