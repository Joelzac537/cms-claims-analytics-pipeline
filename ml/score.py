"""
ML step 3 -- score providers with the trained model, LOCALLY.

Downloads the built-in-XGBoost model artifact (model.tar.gz -> contains a
pickled Booster named 'xgboost-model'), loads it on your machine, scores
every provider, and writes results to dashboard/exports/ for Streamlit.
Scoring locally avoids paying for a hosted SageMaker endpoint.

Run:  python -m ml.score
Optionally point at a specific artifact:  MODEL_DATA=s3://.../model.tar.gz

Outputs:
  dashboard/exports/predictions.csv        (provider_npi, actual, risk_score)
  dashboard/exports/feature_importance.csv (gain-ranked features)
"""

import json
import os
import pickle
import tarfile
from pathlib import Path

import pandas as pd
import xgboost as xgb

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

    objs = get_client("s3").list_objects_v2(
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
# STEP 2 -- download + unpack, load the Booster
# ============================================================
def _load_booster() -> xgb.Booster:
    key = _resolve_model_key()
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    local_tar = ARTIFACTS / "model.tar.gz"

    print(f"Downloading s3://{settings.S3_BUCKET}/{key}")
    get_client("s3").download_file(settings.S3_BUCKET, key, str(local_tar))
    with tarfile.open(local_tar) as tar:
        tar.extractall(ARTIFACTS)

    # built-in XGBoost saves a pickled Booster named 'xgboost-model'
    with open(ARTIFACTS / "xgboost-model", "rb") as fh:
        return pickle.load(fh)


# ============================================================
# STEP 3 -- score the full dataset and export
# ============================================================
def main():
    booster = _load_booster()

    full = pd.read_csv(ARTIFACTS / "full.csv")
    ids = full[ID_COL]
    actual = full[TARGET]
    X = full.drop(columns=[ID_COL, TARGET])

    risk = booster.predict(xgb.DMatrix(X.values))

    EXPORTS.mkdir(parents=True, exist_ok=True)
    predictions = pd.DataFrame({
        "provider_npi": ids,
        "actual_high_cost": actual,
        "risk_score": risk.round(4),
    }).sort_values("risk_score", ascending=False)
    predictions.to_csv(EXPORTS / "predictions.csv", index=False)

    # feature importance -- map XGBoost's f0/f1/... back to real names
    feature_cols = json.loads((ARTIFACTS / "feature_columns.json").read_text())
    gain = booster.get_score(importance_type="gain")  # {'f12': 3.4, ...}
    importance = (
        pd.DataFrame(
            [(feature_cols[int(k[1:])], v) for k, v in gain.items()],
            columns=["feature", "importance"],
        )
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )
    importance.to_csv(EXPORTS / "feature_importance.csv", index=False)

    print(f"Scored {len(predictions):,} providers -> {EXPORTS}")
    print("\nTop 15 features driving high-cost risk:")
    print(importance.head(15).to_string(index=False))


if __name__ == "__main__":
    main()
