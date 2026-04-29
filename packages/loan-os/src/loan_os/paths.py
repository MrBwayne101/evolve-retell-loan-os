from __future__ import annotations

from pathlib import Path


MODULE_PATH = Path(__file__).resolve()
PACKAGE_ROOT = MODULE_PATH.parents[2]
REPO_ROOT = MODULE_PATH.parents[4]
CONTRACTS_DIR = PACKAGE_ROOT / "contracts"
DATA_DIR = REPO_ROOT / "data" / "voice-agent"
CARDS_PATH = DATA_DIR / "cards" / "cards.yaml"
BAKEOFF_OUTPUT_PATH = DATA_DIR / "bakeoff" / "track-0-fake-run.json"
