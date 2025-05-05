import json
import os
import uuid
import boto3
from botocore import UNSIGNED

from fastapi import APIRouter, UploadFile, File, HTTPException
from pandas.api.types import is_numeric_dtype, is_datetime64_any_dtype
from pydantic import BaseModel
from starlette.responses import JSONResponse

import pandas as pd
from io import StringIO


from uvicorn import Config

from main import LOCALSTACK_ENDPOINT, S3_BUCKET
from config import settings
from view.data_quality import DataQualityChecker


router = APIRouter()


class MetadataRequest(BaseModel):
    file_id: str


class FixDataRequest(BaseModel):
    file_id:str


@router.get("/health/")
async def health_check():
    return {"status": "ok"}


def infer_column_type(series: pd.Series) -> str:
    """Determine column type (numeric, datetime, categorical, or text)."""
    if is_numeric_dtype(series):
        return "numeric"
    elif is_datetime64_any_dtype(series):
        return "datetime"
    elif series.nunique() < 20:  # Treat as categorical if unique values < 20
        return "categorical"
    else:
        return "text"


@router.post("/profile/")
async def profile_csv(request: FixDataRequest):
    file_id = request.file_id
    try:
        # Initialize S3 client
        s3 = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY,
            aws_secret_access_key=settings.AWS_SECRET_KEY,
            region_name=settings.AWS_REGION
        )

        # Fetch cleaned CSV from S3
        try:
            response = s3.get_object(
                Bucket=settings.S3_BUCKET_NAME,
                Key=f"cleaned/{file_id}.csv"
            )
        except s3.exceptions.NoSuchKey:
            raise HTTPException(
                status_code=404,
                detail="File not found",
                headers={"Access-Control-Allow-Origin": "*"}
            )

        # Read CSV into DataFrame
        df = pd.read_csv(response['Body'])

        # Initialize profile
        profile = {
            "overview": {
                "num_rows": df.shape[0],
                "num_columns": df.shape[1],
                "columns": list(df.columns),
                "missing_values": df.isnull().sum().to_dict(),
                "data_types": df.dtypes.astype(str).to_dict(),
            },
            "column_analysis": {}
        }

        # Analyze each column
        for col in df.columns:
            col_series = df[col]
            col_type = infer_column_type(col_series)
            stats = {}

            if col_type == "numeric":
                stats = {
                    "min": float(col_series.min()),
                    "max": float(col_series.max()),
                    "mean": float(col_series.mean()),
                    "median": float(col_series.median()),
                    "std": float(col_series.std()),
                    "percentiles": {
                        "25%": float(col_series.quantile(0.25)),
                        "50%": float(col_series.quantile(0.5)),
                        "75%": float(col_series.quantile(0.75)),
                    },
                }
            elif col_type == "datetime":
                stats = {
                    "min": col_series.min().strftime("%Y-%m-%d") if not col_series.empty else None,
                    "max": col_series.max().strftime("%Y-%m-%d") if not col_series.empty else None,
                    "range_days": (col_series.max() - col_series.min()).days if not col_series.empty else None,
                }
            else:  # categorical or text
                value_counts = col_series.value_counts().head(10).to_dict()
                stats = {
                    "unique_count": col_series.nunique(),
                    "top_values": {str(k): v for k, v in value_counts.items()},
                    "sample_values": list(col_series.dropna().astype(str).unique()[:5]),
                }

            stats["missing"] = int(col_series.isnull().sum())
            profile["column_analysis"][col] = {
                "type": col_type,
                "stats": stats
            }
            # Save to S3
            profile_key = f"profiled/{file_id}/profile.json"
            csv_key = f"profiled/{file_id}/data.csv"

            # Save profile as JSON
            s3.put_object(
                Bucket=settings.S3_BUCKET_NAME,
                Key=profile_key,
                Body=json.dumps(profile, default=str)
            )

            # Save CSV copy (optional)
            csv_data = df.to_csv(index=False)
            s3.put_object(
                Bucket=settings.S3_BUCKET_NAME,
                Key=csv_key,
                Body=csv_data
            )

            return JSONResponse(
                content={
                    "status": "success",
                    "profile_location": profile_key,
                    "data_location": csv_key,
                    "profile": profile  # Optional: include profile in response
                },
                headers={"Access-Control-Allow-Origin": "*"}
            )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": f"Unexpected error: {str(e)}"},
            headers={"Access-Control-Allow-Origin": "*"}
        )


@router.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    try:
        file_id = str(uuid.uuid4())
        contents = await file.read()

        s3 = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY,
            aws_secret_access_key=settings.AWS_SECRET_KEY,
            region_name=settings.AWS_REGION
        )

        # Upload to S3
        s3.put_object(
            Bucket=settings.S3_BUCKET_NAME,
            Key=f"original/{file_id}.csv",
            Body=contents
        )

        # Process data quality report
        df = pd.read_csv(StringIO(contents.decode('utf-8')))
        report = DataQualityChecker(df).generate_report()

        return {"file_id": file_id, "report": report}

    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/fix-data/")
async def fix_data(request: FixDataRequest):
    file_id = request.file_id
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY,
            aws_secret_access_key=settings.AWS_SECRET_KEY,
            region_name=settings.AWS_REGION
        )

        # Fetch original file from S3
        try:
            response = s3.get_object(
                Bucket=settings.S3_BUCKET_NAME,
                Key=f"original/{file_id}.csv"
            )
        except s3.exceptions.NoSuchKey:
            raise HTTPException(404, detail="File not found")

        # Process data
        df = pd.read_csv(response['Body'])
        fixed_df = DataQualityChecker(df).fix_data()

        # Convert cleaned data to JSON or CSV for the response
        cleaned_data = fixed_df.to_dict(orient="records")  # Return as JSON
        # cleaned_csv = fixed_df.to_csv(index=False)      # Return as CSV

        # Save to S3 (optional)
        cleaned_csv = fixed_df.to_csv(index=False)
        s3.put_object(
            Bucket=settings.S3_BUCKET_NAME,
            Key=f"cleaned/{file_id}.csv",
            Body=cleaned_csv
        )

        return {
            "status": "success",
            "cleaned_key": f"cleaned/{file_id}.csv",
            "cleaned_data": cleaned_data  # Add cleaned data to the response
        }

    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/generate-metadata/")
async def generate_metadata(request: MetadataRequest):
    try:
        # Initialize Bedrock client
        bedrock = boto3.client(
            'bedrock-runtime',
            region_name='us-west-2',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY')
        )

        # Get cleaned data from S3
        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        response = s3.get_object(
            Bucket=settings.S3_BUCKET_NAME,
            Key=f"cleaned/{request.file_id}.csv"
        )
        csv_content = response['Body'].read().decode('utf-8')

        # Create AI prompt
        prompt = f"""
        Analyze this dataset and generate comprehensive metadata:
        {csv_content[:2000]}  # Send first 2000 chars to stay within token limits

        Include in JSON format:
        - dataset_description
        - data_quality_assessment
        - suggested_analytics
        - potential_insights
        - data_cleaning_suggestions
        """

        # Invoke Bedrock model
        response = bedrock.invoke_model(
            body=json.dumps({
                "prompt": prompt,
                "max_tokens_to_sample": 1000,
                "temperature": 0.5
            }),
            modelId="anthropic.claude-v2",
            contentType="application/json",
            accept="application/json"
        )

        # Parse response
        result = json.loads(response['body'].read())
        return {"metadata": json.loads(result['completion'])}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
