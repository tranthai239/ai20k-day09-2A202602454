"""Settings, all from environment variables so the same code runs in Docker and on a laptop."""
import os
from pathlib import Path

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET = os.getenv("S3_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("LAKE_BUCKET", "lake")

DATA_DIR = Path(os.getenv("DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
WORK_DIR = Path(os.getenv("WORK_DIR", Path(__file__).resolve().parent.parent / "work"))
DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(WORK_DIR / "lakehouse.duckdb"))

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "lab")
PG_PASSWORD = os.getenv("PG_PASSWORD", "lab")
PG_WAREHOUSE_DB = os.getenv("PG_WAREHOUSE_DB", "warehouse")

SAMPLE_FPS = 2            # frames sampled per second of video
DARK_LUMA = 12.0          # mean luma below this = black frame
BLUR_VAR = 40.0           # Laplacian variance below this = blurry frame
DUP_HAMMING = 2           # dHash bits that may differ before two frames count as near-duplicates
SYNC_TOL_MS = 150         # max |frame time - telemetry time| for a valid join
SPLIT = (70, 15, 15)      # train / val / test percent, assigned per trip


def lake(*parts: str) -> str:
    """s3:// URI inside the lake bucket."""
    return "s3://" + "/".join([BUCKET, *[p.strip("/") for p in parts]])
