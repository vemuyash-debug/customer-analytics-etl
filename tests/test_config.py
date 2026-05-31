"""Tests for configuration and data quality logic."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def config_path() -> Path:
    return PROJECT_ROOT / "config" / "config.yaml"


def test_config_loads_with_defaults(config_path: Path) -> None:
    os.environ.setdefault("STORAGE_ACCOUNT_NAME", "testaccount")
    from src.config.settings import PipelineConfig

    config = PipelineConfig(config_path)
    assert config.catalog == "customer_analytics"
    assert "abfss://" in config.get("storage", "base_path")
    assert config.table("bronze", "customers") == "customer_analytics.bronze.customers"


def test_config_path_resolution(config_path: Path) -> None:
    os.environ["STORAGE_ACCOUNT_NAME"] = "proddatalake"
    from src.config.settings import PipelineConfig

    config = PipelineConfig(config_path)
    assert "proddatalake" in config.get("storage", "base_path")


def test_quality_check_result_enum() -> None:
    from src.data_quality.validators import CheckStatus

    assert CheckStatus.PASSED.value == "PASSED"
    assert CheckStatus.FAILED.value == "FAILED"
