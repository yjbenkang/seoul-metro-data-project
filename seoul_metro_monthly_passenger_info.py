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

def fetch_monthly_passenger_data(api_endpoints: dict):
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
                **{f'2022-{month}': f'{month:02}' for month in range(1, 13)},
                **{f'2023-{month}': f'{month:02}' for month in range(1, 13)},
                '상하구분': 'direction',
                '구분': 'direction',
                '출발역': 'departure_station',
                '역명': 'departure_station',
                '요일구분': 'day_type',
                '조사일자': 'day_type',
                '역번호': 'station_number',
                '고유역번호(외부역코드)': 'station_number',
                '호선': 'line',
                '수송연월': 'transportation_date',
                '승하차인원수': 'passenger_count'
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
    "/15044249/v1/uddi:c2b72a04-63da-4d1c-8613-dfbd799b1a95": "20221231",
    "/15044249/v1/uddi:796901f7-428f-4a5e-b8cd-b0a05603e3ce": "20231231"
}

output_file = "prod_data/서울교통공사_월별_승하차인원_전체데이터.csv"
csv_buffer = fetch_monthly_passenger_data(api_endpoints)
save_to_s3(csv_buffer, bucket_name, output_file)
