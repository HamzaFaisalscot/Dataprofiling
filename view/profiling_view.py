from fastapi import APIRouter, UploadFile, File, HTTPException
import pandas as pd
import io
from pandas.api.types import is_numeric_dtype, is_datetime64_any_dtype

router = APIRouter()

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
async def profile_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    # Read CSV into DataFrame
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode("utf-8")))

    # Attempt to parse datetime columns
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass

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

    return profile