from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import LOCALSTACK_ENDPOINT, S3_BUCKET

app = FastAPI()


@app.on_event("startup")
def create_s3_bucket():
    import boto3
    s3 = boto3.client(
        's3',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        endpoint_url=LOCALSTACK_ENDPOINT
    )
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except s3.exceptions.ClientError:
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"Bucket '{S3_BUCKET}' created!")


# 𝗠𝗨𝗦𝗧 𝗯𝗲 𝗯𝗲𝗳𝗼𝗿𝗲 𝗿𝗼𝘂𝘁𝗲𝘀
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3004"],  # Exact frontend origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Initialize test bucket

# Include routers AFTER middleware
from view.profiling_view import router as profiling_router

app.include_router(profiling_router, prefix="/api")
