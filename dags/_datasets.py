"""Airflow Datasets для event-driven цепочки Understat → stage → RV.

ingest_understat_daily  outlets=[ds_understat_raw]
stage_load_understat    schedule=[ds_understat_raw],   outlets=[ds_understat_stage]
dbt_raw_vault           schedule=[ds_understat_stage], outlets=[ds_raw_vault]

build_marts остаётся manual: требует docker-exec spark-submit с хоста
(см. scripts/run_spark_marts.sh), вызвать из airflow-контейнера нельзя.
"""
from __future__ import annotations

from airflow.datasets import Dataset

ds_understat_raw = Dataset("s3://raw-understat/")
ds_understat_stage = Dataset("postgres://stage.understat")
ds_raw_vault = Dataset("postgres://public_raw_vault")
