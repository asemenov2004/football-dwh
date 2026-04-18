"""Обёртка над statsbombpy для Open Data.

statsbombpy по умолчанию возвращает pandas.DataFrame. Для raw lake
нужен сериализуемый JSON — берём DataFrame и кастуем в list[dict]
через to_dict(orient='records'). fmt='dict' не подходит: там ключи-кортежи,
которые json.dumps не умеет сериализовать.
"""
from __future__ import annotations

import logging
from typing import Any

from statsbombpy import sb

log = logging.getLogger(__name__)


def get_competitions() -> list[dict[str, Any]]:
    return sb.competitions().to_dict(orient="records")


def get_matches(competition_id: int, season_id: int) -> list[dict[str, Any]]:
    return sb.matches(
        competition_id=competition_id, season_id=season_id
    ).to_dict(orient="records")
