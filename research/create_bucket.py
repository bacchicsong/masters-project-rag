import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(s3={"addressing_style": "path"}),
)

# Создаём bucket для артефактов MLflow
bucket_name = "mlflow-artifacts"
try:
    s3.create_bucket(Bucket=bucket_name)
    print(f"✅ Bucket '{bucket_name}' created successfully")
except Exception as e:
    if "BucketAlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
        print(f"ℹ️  Bucket '{bucket_name}' already exists")
    else:
        print(f"❌ Error creating bucket: {e}")

# Вывод списка существующих bucket'ов
response = s3.list_buckets()
print("\n📦 Buckets:")
for bucket in response["Buckets"]:
    print(f"   • {bucket['Name']}")