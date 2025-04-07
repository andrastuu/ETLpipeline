import pandas as pd
import logging
from pathlib import Path
from typing import Optional, List, Dict

# ----------------------
# Configure Logging
# ----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------------
# Helpers
# ----------------------
def count_lines(file_path: Path) -> int:
    try:
        with file_path.open('r', encoding='utf-8', errors='ignore') as f:
            return sum(1 for _ in f) - 1  # exclude header
    except Exception as e:
        logging.error(f"❌ Failed to count lines in {file_path.name}: {e}")
        return 0

def validate_columns(df: pd.DataFrame, required_cols: List[str], file_name: str):
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"❌ Missing required columns {missing} in {file_name}")

def load_csv(
    file_path: Path,
    delimiter: str,
    parse_dates: Optional[List[str]] = None,
    name: str = "file"
) -> pd.DataFrame:
    total = count_lines(file_path)
    df = pd.read_csv(
        file_path,
        delimiter=delimiter,
        on_bad_lines="skip",
        engine="python",
        parse_dates=parse_dates
    )
    logging.info(f"✅ Loaded {len(df)} rows from {file_path.name}")
    if total:
        skipped = total - len(df)
        if skipped > 0:
            logging.warning(f"⚠️ Skipped {skipped} malformed rows in {file_path.name}")
    return df

# ----------------------
# Main Extract Function
# ----------------------
def extract(
    data_dir: str = "data",
    conversion_pattern: str = "brokerchooser_conversions*.csv",
    broker_file: str = "broker_data.csv",
    category_file: str = "page_category_mapping.csv"
) -> Dict[str, pd.DataFrame]:
    try:
        data_path = Path(data_dir)

        # Load all conversions files (semicolon-separated expected)
        conversions_files = list(data_path.glob(conversion_pattern))
        if not conversions_files:
            raise FileNotFoundError(f"No conversions files matching pattern '{conversion_pattern}' found in {data_path}")

        conversions = pd.concat([
            load_csv(f, ";", parse_dates=["created_at"], name="Conversions")
            for f in conversions_files
        ], ignore_index=True)
        validate_columns(conversions, ["created_at", "country_name", "measurement_category"], "Conversions")

        # Load broker data (assumed comma-separated)
        broker_data_path = data_path / broker_file
        broker_data = load_csv(broker_data_path, ",", name="Broker Data")
        validate_columns(broker_data, ["timestamp", "ip_country"], "Broker Data")

        # Load category mapping (semicolon-delimited), then save as comma-delimited
        category_mapping_path = data_path / category_file
        category_mapping = load_csv(category_mapping_path, ";", name="Category Mapping")
        validate_columns(category_mapping, ["measurement_category", "page_category"], "Category Mapping")

        # Save as comma-delimited for future use
        comma_mapping_path = data_path / "page_category_mapping_comma.csv"
        category_mapping.to_csv(comma_mapping_path, index=False)
        logging.info(f"✅ Converted and saved category mapping to comma-delimited: {comma_mapping_path.name}")

        logging.info("📦 Data successfully extracted.")
        return {
            "conversions": conversions,
            "broker_data": broker_data,
            "category_mapping": category_mapping
        }

    except Exception as e:
        logging.error(f"❌ Failed to extract data: {e}")
        raise

# ----------------------
# Optional CLI entry point
# ----------------------
def main():
    try:
        extract()
    except Exception as e:
        logging.error(f"Extraction failed in __main__: {e}", exc_info=True)

if __name__ == "__main__":
    main()
