import os
import datetime
from dateutil.rrule import rrule, DAILY




PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")


os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)



OI_ENDPOINT = "https://object.openintel.nl"

OI_BUCKET = "openintel-public"

FDNS_BASE = "fdns"

ZONE_BASED_PATH = f"{FDNS_BASE}/basis=zonefile"



ENRICHED_OUTPUT = os.path.join(
    OUTPUT_DIR,
    "enriched_csv"
)

SOA_SUMMARY_OUTPUT = os.path.join(
    OUTPUT_DIR,
    "soa_summary_csv"
)



TLDS = [
    "li",
    "se",
    "nu"
]


START_DATE = datetime.datetime(2026, 7, 14)

END_DATE = datetime.datetime(2026, 7, 15)

PROCESS_DATES = list(
    rrule(
        DAILY,
        dtstart=START_DATE,
        until=END_DATE
    )
)

# =========================================================
# SPARK CONFIGURATION
# =========================================================

SPARK_MASTER = "local[*]"

APP_NAME = "OpenIntel ETL"

DRIVER_MEMORY = "8g"

EXECUTOR_MEMORY = "8g"

SHUFFLE_PARTITIONS = "8"

# Disable vectorized reader to reduce memory pressure
ENABLE_VECTORIZED_PARQUET = False

# Disable S3 vectored read
ENABLE_S3_VECTORED_READ = False



LOG_LEVEL = "INFO"