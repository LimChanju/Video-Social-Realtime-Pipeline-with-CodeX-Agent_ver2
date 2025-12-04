"""
Configuration helpers for logging and app settings.
"""
import logging.config
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


def load_logging_config(path: str = "conf/logging.yaml") -> None:
    """Load logging configuration if the file exists."""
    cfg_path = Path(path)
    if cfg_path.exists():
        with cfg_path.open() as f:
            logging.config.dictConfig(yaml.safe_load(f))


def load_app_config(path: str = "conf/app.yaml", fallback: str = "conf/app.yaml.example") -> Dict[str, Any]:
    """Load app config YAML; fall back to example if main config is missing."""
    cfg_path = Path(path)
    if not cfg_path.exists():
        cfg_path = Path(fallback)
    if cfg_path.exists():
        with cfg_path.open() as f:
            return yaml.safe_load(f) or {}
    return {}
