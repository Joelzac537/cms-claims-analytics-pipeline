"""
ML step 1 -- build the model-ready feature set from the curated data.

Pulls the fact view from Athena, engineers features, and writes
train / validation / full CSVs locally (ml/artifacts/) and uploads the
train + validation splits to S3 for SageMaker.

IMPORTANT -- target leakage
---------------------------
The label `is_high_cost_outlier` was created in the ETL job as
`medicare_payment > 1,033,737`. So the payment/charge/allowed columns
essentially CONTAIN the answer. We DROP them from the features on purpose,
so the model has to learn high-cost risk from independent signals
(service volumes, chronic-condition mix, specialty, state).

Run:  python -m ml.prepare_features
"""

import json
from pathlib import Path

import awswrangler as wr
import pandas as pd
from sklearn.model_selection import train_test_split

from config import settings
from config.aws_session import get_session, get_client

ARTIFACTS = Path(__file__).parent / "artifacts"

TARGET = "is_high_cost_outlier"
ID_COL = "provider_npi"
LEAKAGE_COLS = ["medicare_payment", "submitted_charge", "medicare_allowed"]
CATEGORICAL = ["provider_type", "state"]


# ============================================================
# STEP 1 -- read the curated fact view from Athena
# ============================================================
def load_fact() -> pd.DataFrame:
    print("Querying fact_provider_metrics from Athena ...")
    df = wr.athena.read_sql_query(
        "SELECT * FROM fact_provider_metrics",
        database=settings.GLUE_DATABASE,
        workgroup=settings.ATHENA_WORKGROUP,
        s3_output=settings.ATHENA_OUTPUT_LOCATION,
        boto3_session=get_session(),
    )
    print(f"  loaded {len(df):,} rows")
    return df


# ============================================================
# STEP 2 -- engineer features (drop leakage, one-hot encode)
# ============================================================
def build_features(df: pd.DataFrame):
    y = df[TARGET].astype(int)

    drop_cols = [c for c in (LEAKAGE_COLS + [TARGET, ID_COL]) if c in df.columns]
    X = df.drop(columns=drop_cols)

    # one-hot encode the categorical keys so XGBoost can use them
    X = pd.get_dummies(
        X,
        columns=[c for c in CATEGORICAL if c in X.columns],
        dummy_na=False,
    )
    X = X.fillna(0)

    print(f"  {X.shape[1]} features, positive rate {y.mean():.3%}")
    return X, y


# ============================================================
# STEP 3 -- split and persist (local + S3)
# ============================================================
def save_and_upload(X: pd.DataFrame, y: pd.Series):
    ARTIFACTS.mkdir(parents=True, exist_ok=True)

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )

    # target in the first column (a common training-CSV convention)
    train = pd.concat([y_train.rename(TARGET), X_train], axis=1)
    val = pd.concat([y_val.rename(TARGET), X_val], axis=1)
    # full set keeps provider_npi (as an identifier, NOT a feature) so the
    # scoring step can attach predictions back to specific providers.
    full = pd.concat([df[ID_COL], y.rename(TARGET), X], axis=1)

    train.to_csv(ARTIFACTS / "train.csv", index=False)
    val.to_csv(ARTIFACTS / "validation.csv", index=False)
    full.to_csv(ARTIFACTS / "full.csv", index=False)

    # record feature order so scoring reproduces the exact same columns
    (ARTIFACTS / "feature_columns.json").write_text(
        json.dumps(list(X.columns), indent=2), encoding="utf-8"
    )

    # upload the two training splits to S3 for SageMaker channels
    s3 = get_client("s3")
    for name in ("train.csv", "validation.csv"):
        key = f"{settings.ML_TRAIN_PREFIX}{name}"
        s3.upload_file(str(ARTIFACTS / name), settings.S3_BUCKET, key)
        print(f"  uploaded s3://{settings.S3_BUCKET}/{key}")


# ============================================================
# MAIN
# ============================================================
def main():
    df = load_fact()
    X, y = build_features(df)
    save_and_upload(X, y)
    print("\nFeature prep complete -> ml/artifacts/ and s3://.../ml/input/")


if __name__ == "__main__":
    main()
