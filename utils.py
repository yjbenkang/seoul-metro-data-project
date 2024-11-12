import boto3
import os
from dotenv import load_dotenv
from io import BytesIO

load_dotenv()

aws_access_key_id = os.getenv("S3_AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("S3_AWS_SECRET_ACCESS_KEY")
region_name = os.getenv("S3_REGION")
bucket_name = os.getenv("S3_BUCKET_NAME")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

def save_to_s3(csv_buffer: BytesIO, bucket_name: str, output_file: str):
    """
    S3에 CSV 파일을 저장합니다.
    :param csv_buffer: CSV 파일 데이터가 저장된 BytesIO 객체
    :param bucket_name: S3 버킷 이름
    :param output_file: 저장할 파일 이름
    """
    try:
        s3_client.upload_fileobj(csv_buffer, bucket_name, output_file)
        print(f"모든 데이터가 {output_file} 파일로 S3에 성공적으로 업로드되었습니다.")
    except Exception as e:
        print(f"Failed to upload combined data to S3: {e}")
