"""
ML step 3 -- score providers with the trained model, LOCALLY.

Downloads the trained model artifact (model.tar.gz) from S3, loads it on
your machine, scores every provider, and writes results to
dashboard/exports/ for the Streamlit app. Scoring locally avoids paying for
a hosted SageMaker inference endpoint.

Run:  python -m ml.score
Optionally point at a specific artifact:  MODEL_DATA=s3://.../model.tar.gz

Outputs:
  dashboard/exports/predictions.csv        (provider_npi, actual, risk_score)
  dashboard/exports/feature_importance.csv (copied from the model artifact)
"""

import os
import tarfile
from pathlib import Path

import joblib
import pandas as pd

from config import settings
from config.aws_session import get_client

ARTIFACTS = Path(__file__).parent / "artifacts"
EXPORTS = Path(__file__).parents[1] / "dashboard" / "exports"

TARGET = "is_high_cost_outlier"
ID_COL = "provider_npi"


# ============================================================
# STEP 1 -- find the model artifact in S3 (latest if not specified)
# ============================================================
def _resolve_model_key() -> str:
    explicit = os.environ.get("MODEL_DATA")
    if explicit:
        return explicit.split(f"{settings.S3_BUCKET}/", 1)[-1]

    s3 = get_client("s3")
    objs = s3.list_objects_v2(
        Bucket=settings.S3_BUCKET, Prefix=settings.ML_MODEL_PREFIX
    ).get("Contents", [])
    models = [o for o in objs if o["Key"].endswith("model.tar.gz")]
    if not models:
        raise SystemExit(
            "No model.tar.gz found under "
            f"s3://{settings.S3_BUCKET}/{settings.ML_MODEL_PREFIX} -- "
            "run ml.train_xgboost first."
        )
    return max(models, key=lambda o: o["LastModified"])["Key"]


# ============================================================
# STEP 2 -- download + unpack the artifact
# ============================================================
def _load_model():
    key = _resolve_model_key()
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    local_tar = ARTIFACTS / "model.tar.gz"

    print(f"Downloading s3://{settings.S3_BUCKET}/{key}")
    get_client("s3").download_file(settings.S3_BUCKET, key, str(local_tar))
    with tarfile.open(local_tar) as tar:
        tar.extractall(ARTIFACTS)

    return joblib.load(ARTIFACTS / "model.joblib")


# ============================================================
# STEP 3 -- score the full dataset and export
# ============================================================
def main():
    model = _load_model()

    full = pd.read_csv(ARTIFACTS / "full.csv")
    ids = full[ID_COL]
    actual = full[TARGET]
    X = full.drop(columns=[ID_COL, TARGET])

    risk = model.predict_proba(X)[:, 1]

    EXPORTS.mkdir(parents=True, exist_ok=True)
    predictions = pd.DataFrame({
        "provider_npi": ids,
        "actual_high_cost": actual,
        "risk_score": risk.round(4),
    }).sort_values("risk_score", ascending=False)
    predictions.to_csv(EXPORTS / "predictions.csv", index=False)

    # copy the feature-importance ranking next to the predictions
    fi_src = ARTIFACTS / "feature_importance.csv"
    if fi_src.exists():
        (EXPORTS / "feature_importance.csv").write_text(
            fi_src.read_text(encoding="utf-8"), encoding="utf-8"
        )

    print(f"Scored {len(predictions):,} providers -> {EXPORTS}")
    print(predictions.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
