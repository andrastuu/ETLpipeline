import sys
import os
import pandas as pd
import logging
import yaml
import json
from typing import Tuple, Dict, Any
from pathlib import Path
from datetime import timedelta

from etl.extract import extract

# ----------------------
# Configure Logging
# ----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ----------------------
# Constants
# ----------------------
START_DATE_RAW = os.getenv("START_DATE", "2024-04-21")
try:
    START_DATE = pd.to_datetime(START_DATE_RAW)
    logging.info(f"📅 Using START_DATE: {START_DATE}")
except Exception as e:
    raise ValueError(f"Invalid START_DATE format '{START_DATE_RAW}': {e}")

TIME_TOLERANCE_MINUTES = int(os.getenv("TIME_TOLERANCE_MINUTES", "20"))
DEFAULT_REGION_PATH = Path(__file__).parent / "regions.yml"

# ----------------------
# Data Cleaning Utilities
# ----------------------
def filter_conversions(conversions: pd.DataFrame, start_date: str) -> pd.DataFrame:
    conversions = conversions.copy()
    conversions["created_at"] = pd.to_datetime(conversions["created_at"], errors="coerce")
    return conversions[conversions["created_at"] >= pd.to_datetime(start_date)]

def load_country_mapping(filepath: Path = DEFAULT_REGION_PATH) -> Dict[str, str]:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            regions = yaml.safe_load(f)

        mapping = {}
        for region in regions:
            name = region.get("name", "")
            short_name = region.get("short_name", name)
            aliases = region.get("aliases", [])
            short_clean = short_name.strip().lower()

            for key in [name, short_name] + aliases:
                if key:
                    mapping[key.strip().lower()] = short_clean

        return mapping
    except Exception as e:
        logging.warning(f"Could not load country mapping: {e}")
        return {}

def apply_country_mapping(series: pd.Series, mapping: Dict[str, str]) -> pd.Series:
    cleaned = series.astype(str).str.strip().str.lower()
    mapped = cleaned.map(mapping)
    missing = cleaned[~cleaned.isin(mapping.keys())].unique()
    if len(missing) > 0:
        logging.warning(f"Unmatched country names: {missing[:10]}{'...' if len(missing) > 10 else ''}")
    return mapped.fillna(series)

# ----------------------
# Matching Logic
# ----------------------
def normalize_data(conversions: pd.DataFrame, broker_data: pd.DataFrame, mapping: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    conversions = conversions.copy()
    broker_data = broker_data.copy()

    conversions["country_name"] = apply_country_mapping(conversions["country_name"], mapping)
    broker_data["ip_country"] = apply_country_mapping(broker_data["ip_country"], mapping)
    broker_data["country_name"] = broker_data["ip_country"]
    broker_data["broker_timestamp"] = pd.to_datetime(broker_data["timestamp"], unit="s", errors="coerce")

    return conversions, broker_data

def perform_matching(conversions: pd.DataFrame, broker_data: pd.DataFrame) -> pd.DataFrame:
    conversions_sorted = conversions.sort_values("created_at")
    broker_sorted = broker_data.sort_values("broker_timestamp")

    return pd.merge_asof(
        conversions_sorted,
        broker_sorted,
        left_on="created_at",
        right_on="broker_timestamp",
        by="country_name",
        direction="nearest",
        tolerance=timedelta(minutes=TIME_TOLERANCE_MINUTES)
    )

def export_unmatched_diagnostics(matched: pd.DataFrame, broker_data: pd.DataFrame) -> Dict[str, int]:
    unmatched = matched[matched["broker_timestamp"].isna()].copy()
    matched_count = len(matched) - len(unmatched)
    total = len(matched)

    if total > 0:
        logging.info(f"✅ Matched {matched_count}/{total} conversions ({matched_count / total:.1%})")
    else:
        logging.warning("⚠️ No conversions to match. Skipping ratio calculation.")

    known_countries = set(broker_data["country_name"])
    unmatched["country_match"] = unmatched["country_name"].isin(known_countries)
    unmatched["timestamp_within_tolerance"] = unmatched["country_match"] & unmatched["created_at"].between(
        broker_data["broker_timestamp"].min(), broker_data["broker_timestamp"].max()
    )

    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    country_mismatch = unmatched[~unmatched["country_match"]].copy()
    country_mismatch.to_csv(output_dir / "unmatched_country_mismatch.csv", index=False)

    time_mismatch = unmatched[unmatched["country_match"] & ~unmatched["timestamp_within_tolerance"]].copy()
    time_mismatch.to_csv(output_dir / "unmatched_time_mismatch.csv", index=False)

    unmatched.to_csv(output_dir / "unmatched_conversions.csv", index=False)

    return {
        "total": total,
        "matched": matched_count,
        "unmatched": len(unmatched),
        "country_mismatch": len(country_mismatch),
        "time_mismatch": len(time_mismatch)
    }


# ----------------------
# ✅ Reusable Transform Function for Tests & DAG
# ----------------------
def transform(conversions: pd.DataFrame, broker_data: pd.DataFrame) -> pd.DataFrame:
    region_mapping = load_country_mapping()
    conversions_filtered = filter_conversions(conversions, START_DATE)
    conversions_normalized, broker_normalized = normalize_data(conversions_filtered, broker_data, region_mapping)
    matched = perform_matching(conversions_normalized, broker_normalized)
    export_unmatched_diagnostics(matched, broker_normalized)
    return matched

# ----------------------
# CLI Entry Point
# ----------------------
def main():
    try:
        datasets = extract()
        matched = transform(datasets["conversions"], datasets["broker_data"])

        summary = {
            "total": len(matched),
            "matched": len(matched.dropna(subset=["broker_timestamp"])),
            "unmatched": len(matched[matched["broker_timestamp"].isna()])
        }

        with open(Path("output") / "summary.json", "w") as f:
            json.dump(summary, f, indent=2)

        matched.to_csv(Path("output") / "matched_data.csv", index=False)
        logging.info("📦 Saved matched data to output/matched_data.csv")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)

if __name__ == "__main__":
    main()
