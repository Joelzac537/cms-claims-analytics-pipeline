# CMS Medicare Provider Analytics Pipeline

An end-to-end AWS data pipeline over **real-world CMS Medicare data**
(~1.3M **provider-level** records from the "Medicare Physician & Other
Practitioners – by Provider" dataset), plus an XGBoost model that flags
high-cost providers and ranks what drives the risk.

> Note on scope: this dataset is **provider-level summaries**, not
> individual claims. Figures and framing throughout reflect that.

## Architecture

```
                    ┌─────────────── EventBridge ──────────────┐
                    │        (new file in raw/ triggers)        │
                    ▼                                           │
  raw CSV ─► S3 raw/ ─► Step Functions ─► Glue / PySpark ETL ─► S3 processed/ (Parquet)
                                              │  + Glue Data Quality (warn-only)
                                              ▼
                                         SNS alerts
                                              │
                          Glue Catalog (cms_medical_db)
                                              │
                                   Athena gold views (star schema)
                                        │              │
                            Streamlit dashboard   SageMaker XGBoost
                                                  (high-cost risk model)
```

- **No Redshift** — the gold layer is Athena/Spectrum over the Parquet
  (cost decision; the dataset is small).
- **No Airflow / dbt** — orchestration is Step Functions + EventBridge.

## Tech stack
AWS S3 · Glue (PySpark) · Glue Data Quality · Athena · Lambda ·
Step Functions · EventBridge · SNS · SageMaker · Python · Streamlit

## Repository layout

| Path | What it does |
|------|--------------|
| `config/` | `settings.py` (all resource names) + `aws_session.py` (central boto3 access) |
| `glue_jobs/cms_transform.py` | PySpark ETL: select/clean columns, flag outliers, **Glue Data Quality**, write partitioned Parquet |
| `lambda_functions/` | S3-upload file validator |
| `step_functions/` | State machine definition (Glue → quality check → SNS) |
| `orchestration/eventbridge_rule.py` | Wires S3 uploads → EventBridge → Step Functions |
| `athena/gold/*.sql` | Star-schema views + gold marts |
| `athena/deploy_views.py` | Deploys the gold views |
| `ml/` | `prepare_features.py` → `train_xgboost.py` (SageMaker) → `score.py` (local) |
| `dashboard/` | `export_gold_results.py` + Streamlit `app.py` |
| `notebooks/` | Exploratory EDA / cleaning notebooks |

## Setup

```bash
python -m venv .venv && . .venv/Scripts/activate   # Windows
pip install -r requirements.txt

# verify AWS access (uses your local 'joel-admin' profile)
python -m config.aws_session
```

## Run order

```bash
# 1. ETL + data quality — deploy glue_jobs/cms_transform.py to the Glue job, run it
# 2. Orchestration (creates AWS resources)
python -m orchestration.eventbridge_rule
# 3. Gold layer (free — views only)
python -m athena.deploy_views
# 4. ML  (train_xgboost is BILLABLE — SageMaker)
python -m ml.prepare_features
python -m ml.train_xgboost
python -m ml.score
# 5. Dashboard
python -m dashboard.export_gold_results
streamlit run dashboard/app.py
```

## Cost notes
- Athena is pay-per-scan; on this dataset queries cost fractions of a cent.
- `ml/train_xgboost.py` starts a **billable** SageMaker job (small instance,
  managed spot, 30-min cap). Scoring runs locally — **no** hosted endpoint.
- Everything else (S3, Glue jobs, Lambda, Step Functions, EventBridge, SNS)
  is pay-per-use with negligible cost at this scale.

## Status — AWS Certified Data Engineer (DEA-C01) portfolio project
Milestones: ✅ ETL · ✅ Glue Data Quality · ✅ Athena gold layer ·
✅ EventBridge orchestration · ✅ XGBoost model · ✅ Streamlit dashboard
