"""Configuration loader with environment variable substitution."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml


_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _substitute_env(value: str) -> str:
    def replacer(match: re.Match[str]) -> str:
        key = match.group(1)
        return os.environ.get(key, match.group(0))

    return _ENV_PATTERN.sub(replacer, value)


def _resolve_templates(obj: Any, context: dict[str, str]) -> Any:
    if isinstance(obj, dict):
        return {k: _resolve_templates(v, context) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_templates(item, context) for item in obj]
    if isinstance(obj, str):
        result = _substitute_env(obj)
        for _ in range(3):
            updated = result.format(**context)
            if updated == result:
                break
            result = updated
        return result
    return obj


class PipelineConfig:
    """Loads and resolves pipeline configuration from YAML."""

    def __init__(self, config_path: str | Path | None = None) -> None:
        if config_path is None:
            config_path = Path(__file__).resolve().parents[2] / "config" / "config.yaml"
        self._config_path = Path(config_path)
        with self._config_path.open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle)

        storage_account = os.environ.get("STORAGE_ACCOUNT_NAME", "devdatalake")
        base_path = raw["storage"]["base_path"].replace("${STORAGE_ACCOUNT_NAME}", storage_account)

        context = {
            "base_path": base_path,
            "catalog": raw["catalog"]["name"],
            "bronze_schema": raw["catalog"]["bronze_schema"],
            "silver_schema": raw["catalog"]["silver_schema"],
            "gold_schema": raw["catalog"]["gold_schema"],
            "audit_schema": raw["catalog"]["audit_schema"],
            "STORAGE_ACCOUNT_NAME": storage_account,
        }
        self._config = _resolve_templates(raw, context)
        self._config["storage"]["base_path"] = base_path

    @property
    def raw(self) -> dict[str, Any]:
        return self._config

    def get(self, *keys: str, default: Any = None) -> Any:
        node: Any = self._config
        for key in keys:
            if not isinstance(node, dict) or key not in node:
                return default
            node = node[key]
        return node

    def table(self, layer: str, name: str) -> str:
        return self.get("tables", layer, name)

    def path(self, layer: str, name: str) -> str:
        return self.get("paths", layer, name)

    @property
    def catalog(self) -> str:
        return self._config["catalog"]["name"]
