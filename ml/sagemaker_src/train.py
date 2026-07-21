"""
SageMaker training entry point (runs INSIDE the training container).

This is NOT run on your laptop -- SageMaker copies this file into the
container, installs requirements.txt, and executes it with the data
channels mounted as local directories. It trains an XGBoost classifier to
flag high-cost providers and writes the model + feature importances to the
model directory, which SageMaker packages into model.tar.gz on S3.

SageMaker provides these via environment variables:
    SM_CHANNEL_TRAIN       -> dir containing train.csv
    SM_CHANNEL_VALIDATION  -> dir containing validation.csv
    SM_MODEL_DIR           -> dir to save the model into
"""

import argparse
import os
from pathlib import Path

import joblib
import pandas as pd
from sklearn.metrics import roc_auc_score, average_precision_score
from xgboost import XGBClassifier

TARGET = "is_high_cost_outlier"


# ============================================================
# Load a channel's single CSV into X, y
# ============================================================
def _load(channel_dir: str):
    csv = next(Path(channel_dir).glob("*.csv"))
    df = pd.read_csv(csv)
    y = df[TARGET]
    X = df.drop(columns=[TARGET])
    return X, y


# ============================================================
# MAIN -- train, evaluate, persist
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--train", default=os.environ.get("SM_CHANNEL_TRAIN"))
    parser.add_argument("--validation", default=os.environ.get("SM_CHANNEL_VALIDATION"))
    parser.add_argument("--model-dir", default=os.environ.get("SM_MODEL_DIR"))
    parser.add_argument("--n-estimators", type=int, default=300)
    parser.add_argument("--max-depth", type=int, default=6)
    parser.add_argument("--learning-rate", type=float, default=0.1)
    args = parser.parse_args()

    X_train, y_train = _load(args.train)
    X_val, y_val = _load(args.validation)

    # the outlier class is rare -> weight it so the model doesn't ignore it
    pos = max(int(y_train.sum()), 1)
    neg = len(y_train) - pos
    scale_pos_weight = neg / pos

    model = XGBClassifier(
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        learning_rate=args.learning_rate,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,
        eval_metric="aucpr",
        n_jobs=-1,
        random_state=42,
    )
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)

    # evaluate
    proba = model.predict_proba(X_val)[:, 1]
    print(f"Validation ROC-AUC : {roc_auc_score(y_val, proba):.4f}")
    print(f"Validation PR-AUC  : {average_precision_score(y_val, proba):.4f}")

    # persist model
    model_dir = Path(args.model_dir)
    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_dir / "model.joblib")

    # persist feature importances (the "what drives risk" ranking)
    importance = (
        pd.DataFrame({
            "feature": X_train.columns,
            "importance": model.feature_importances_,
        })
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )
    importance.to_csv(model_dir / "feature_importance.csv", index=False)
    print("\nTop 15 features driving high-cost risk:")
    print(importance.head(15).to_string(index=False))


if __name__ == "__main__":
    main()
