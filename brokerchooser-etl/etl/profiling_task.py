import logging
from pathlib import Path
from ydata_profiling import ProfileReport
import pandas as pd

def profile_transformed_data(input_path: Path = Path("output/matched_data.csv"),
                              output_report: Path = Path("output/profiling_report.html")):
    try:
        df = pd.read_csv(input_path)
        profile = ProfileReport(df, title="BrokerChooser ETL Profile", explorative=True)
        profile.to_file(output_report)
        logging.info(f"📊 Profiling report saved to {output_report}")
    except Exception as e:
        logging.error(f"❌ Failed to generate profiling report: {e}")
        raise
