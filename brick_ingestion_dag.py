from __future__ import annotations

"""
Airflow DAG: brick_ingestion_dag
--------------------------------
Ingests local Brick dataset files for buildings A, B, C.
 * Scans a repository directory for *.csv, *.ttl, *.zip
 * Extracts the building code from filename (regex _[ABC]_)
 * Processes each file type:
     - CSV  : uploaded as‑is to S3 and (optionally) published to Kafka
     - TTL  : converted to JSONL with rdflib, then uploaded & published
     - ZIP  : unpacked, Pickle files archived to S3 (conversion optional)
 * Demonstrates dynamic task mapping (Airflow ≥2.5) and TaskFlow API.

Edit the PARAMS dict or override via Airflow Variables/conn.
"""

import json
import os
import re
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.kafka.operators.produce import ProduceToKafkaOperator
from airflow.utils.task_group import TaskGroup

try:
    import rdflib  # TTL parsing
except ImportError:
    rdflib = None  # will raise inside task if TTL encountered

# ---------------------------------------------------------------------------
# 💡  Configuration – adjust as needed or move to Airflow Variables/Connections
# ---------------------------------------------------------------------------
PARAMS = {
    "local_repo_dir": "/data/brick_repository",  # 📂 root path with the files
    "s3_bucket": "lakehouse",
    "aws_conn_id": "aws_default",  # Airflow connection for S3 access
    "kafka_conn_id": "kafka_default",  # Airflow connection ID for Kafka
    "kafka_topic_tpl": "brick.{building}.raw.{ext}",
    "max_files_per_run": 1000,  # safeguard
}

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
}

with DAG(
    dag_id="brick_ingestion_dag",
    start_date=datetime(2025, 7, 21),
    schedule_interval="@hourly",  # ⏰ adjust: e.g. "0 2 * * *" for daily
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["brick", "ingestion", "lakehouse"],
) as dag:

    # ---------------------------------------------------------------------
    # 1️⃣  List relevant files in the repository (dynamic task mapping source)
    # ---------------------------------------------------------------------

    @task()
    def list_files(repo_dir: str, limit: int) -> List[str]:
        """Return absolute paths of candidate files limited to *limit* items."""
        patterns = ("*.csv", "*.ttl", "*.zip")
        found = []
        for pattern in patterns:
            found.extend(Path(repo_dir).rglob(pattern))
        # Sort by mtime for deterministic ordering
        found = sorted(found, key=lambda p: p.stat().st_mtime, reverse=True)
        return [str(p) for p in found[:limit]]

    file_list = list_files(PARAMS["local_repo_dir"], PARAMS["max_files_per_run"])

    # ---------------------------------------------------------------------
    # 2️⃣  Process each file (dynamic mapping) → uploads + optional Kafka
    # ---------------------------------------------------------------------

    @task()
    def process_file(file_path: str, s3_bucket: str, aws_conn_id: str) -> str:
        """Handle a single file; returns the S3 key uploaded."""
        path = Path(file_path)
        match = re.search(r"_([ABC])_", path.name)
        building = match.group(1) if match else "X"
        ext = path.suffix.lower().lstrip(".")  # csv / ttl / zip

        hook = S3Hook(aws_conn_id=aws_conn_id)
        s3_prefix = f"raw/building={building}/{ext}/"
        s3_key = f"{s3_prefix}{path.name}"

        if ext == "csv":
            hook.load_file(filename=str(path), bucket_name=s3_bucket, key=s3_key, replace=True)

        elif ext == "ttl":
            if rdflib is None:
                raise ModuleNotFoundError("rdflib not installed in Airflow environment")
            tmp_dir = tempfile.mkdtemp()
            jsonl_path = Path(tmp_dir) / f"{path.stem}.jsonl"
            _ttl_to_jsonl(source=str(path), target=str(jsonl_path))
            hook.load_file(filename=str(jsonl_path), bucket_name=s3_bucket, key=s3_key.replace("ttl", "jsonl"), replace=True)
            shutil.rmtree(tmp_dir)

        elif ext == "zip":
            # archive as‑is
            hook.load_file(filename=str(path), bucket_name=s3_bucket, key=s3_key, replace=True)
        else:
            raise ValueError(f"Unsupported extension: {ext}")

        return s3_key

    def _ttl_to_jsonl(source: str, target: str) -> None:
        """Convert TTL Brick graph to newline‑delimited JSON records (simple example)."""
        import datetime as dt
        g = rdflib.Graph()
        g.parse(source, format="ttl")
        records = []
        for s, p, o in g.triples((None, rdflib.namespace.RDF.type, None)):
            if "Sensor" in o.split("#")[-1]:
                sensor_id = s.split("/")[-1]
                for _, _, val in g.triples((s, rdflib.URIRef("https://brickschema.org/schema/Brick#hasValue"), None)):
                    records.append({
                        "timestamp": int(dt.datetime.utcnow().timestamp()*1000),
                        "sensor_id": sensor_id,
                        "reading": float(val.toPython()),
                    })
        with open(target, "w", encoding="utf-8") as fp:
            for rec in records:
                fp.write(json.dumps(rec) + "\n")

    uploaded_keys = process_file.expand(  # noqa: F841 – captured as XCom
        file_path=file_list,
        s3_bucket=PARAMS["s3_bucket"],
        aws_conn_id=PARAMS["aws_conn_id"],
    )

    # ---------------------------------------------------------------------
    # 3️⃣  Optionally publish messages to Kafka after upload – grouped task
    # ---------------------------------------------------------------------

    with TaskGroup("kafka_publish") as kafka_group:
        def _topic_from_key(s3_key: str) -> str:
            m = re.search(r"building=([ABC])/([a-z]+)/", s3_key)
            if not m:
                return "brick.unknown.raw"
            building, ext = m.groups()
            return PARAMS["kafka_topic_tpl"].format(building=building, ext=ext)

        @task()
        def prepare_kafka_messages(keys: List[str]) -> List[dict]:
            return [
                {
                    "topic": _topic_from_key(k),
                    "key": k.encode(),
                    "value": json.dumps({"s3_key": k}).encode(),
                }
                for k in keys
            ]

        messages = prepare_kafka_messages(uploaded_keys)

        ProduceToKafkaOperator(
            task_id="produce_uploaded_keys",
            messages=messages,
            kafka_config_id=PARAMS["kafka_conn_id"],
        )

    uploaded_keys >> kafka_group
