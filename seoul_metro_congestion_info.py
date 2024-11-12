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

def fetch_subway_congestion_data(api_endpoints: dict):
    """
    지하철혼잡도정보의 여러 API 엔드포인트에서 데이터를 가져옵니다.
    :param api_endpoints (dict): 데이터를 가져올 API 엔드포인트 목록.
    """
    all_data = pd.DataFrame()

    for endpoint, date in api_endpoints.items():
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
                print(f"No data found for {endpoint}.")
                break
            df = pd.DataFrame(data['data'])
            df['date'] = date
            df.rename(columns={
                '5시30분': '05_30',
                '6시00분': '06_00',
                '6시30분': '06_30',
                '7시00분': '07_00',
                '7시30분': '07_30',
                '8시00분': '08_00',
                '8시30분': '08_30',
                '9시00분': '09_00',
                '9시30분': '09_30',
                '10시00분': '10_00',
                '10시30분': '10_30',
                '11시00분': '11_00',
                '11시30분': '11_30',
                '12시00분': '12_00',
                '12시30분': '12_30',
                '13시00분': '13_00',
                '13시30분': '13_30',
                '14시00분': '14_00',
                '14시30분': '14_30',
                '15시00분': '15_00',
                '15시30분': '15_30',
                '16시00분': '16_00',
                '16시30분': '16_30',
                '17시00분': '17_00',
                '17시30분': '17_30',
                '18시00분': '18_00',
                '18시30분': '18_30',
                '19시00분': '19_00',
                '19시30분': '19_30',
                '20시00분': '20_00',
                '20시30분': '20_30',
                '21시00분': '21_00',
                '21시30분': '21_30',
                '22시00분': '22_00',
                '22시30분': '22_30',
                '23시00분': '23_00',
                '23시30분': '23_30',
                '24시00분': '00_00',
                '24시30분': '00_30',
                '00시00분': '00_00',
                '00시30분': '00_30',
                '상하구분': 'direction',
                '구분': 'direction',
                '출발역': 'departure_station',
                '역명': 'departure_station',
                '요일구분': 'day_type',
                '조사일자': 'day_type',
                '역번호': 'station_number',
                '호선': 'line'
            }, inplace=True)

            if '연번' in df.columns:
                df.drop(columns=['연번'], inplace=True)
            all_data = pd.concat([all_data, df], ignore_index=True)
            print(f"{endpoint} - Page {page} data fetched and added.")
            page += 1

    csv_buffer = BytesIO()
    all_data.to_csv(csv_buffer, index=False, encoding='utf-8')
    csv_buffer.seek(0)
    return csv_buffer

api_endpoints = {
    "/15071311/v1/uddi:70e3a3d3-0872-4828-8234-f0bca459b44f": "20191231",
    "/15071311/v1/uddi:b3803d43-ffe3-4d17-9024-fd6cfa37c284": "20211231",
    "/15071311/v1/uddi:75461a18-17a3-42fe-9322-a51148003b69": "20221231",
    "/15071311/v1/uddi:99771417-a036-46f1-8ad5-8edf4591c2ee": "20201231",
    "/15071311/v1/uddi:e477f1d9-2c3a-4dc8-b147-a55584583fa2": "20231231",
    "/15071311/v1/uddi:c87b6af0-0ef7-4182-b172-fd2680a79d6f": "20240331",
    "/15071311/v1/uddi:9aff0ee6-26e7-42c4-af0c-84bf31680ca9": "20240630",
    "/15071311/v1/uddi:da7cd08f-94f0-4dba-b33d-d02dcb35b57b": "20240930"
}

output_file = "prod_data/서울교통공사_지하철혼잡도_전체데이터.csv"
csv_buffer = fetch_subway_congestion_data(api_endpoints)
save_to_s3(csv_buffer, bucket_name, output_file)
