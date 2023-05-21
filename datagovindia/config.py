import os
from pathlib import Path

DATAGOVINDIA_API_KEY = os.environ.get("DATAGOVINDIA_API_KEY", None)

DATAGOVINDIA_SAMPLE_API_KEY = os.environ.get(
    "DATAGOVINDIA_SAMPLE_API_KEY",
    "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b",
)

DATAGOVINDIA_CACHE_DIR = Path(
    os.environ.get("DATAGOVINDIA_CACHE_DIR", Path.home() / ".cache" / "datagovindia")
)

LOG_LEVEL = os.environ.get("LOGLEVEL", "WARNING").upper()
