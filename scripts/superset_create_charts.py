"""Создаёт 6 чартов в Superset и привязывает к дашборду 'Football DWH'.
Идемпотентность: если чарт с таким slice_name уже есть — обновляет params.
Запуск: python scripts/superset_create_charts.py
"""
from __future__ import annotations

import json
import os
import sys
import uuid
from typing import Any

import requests

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
PASS = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")
DASHBOARD_TITLE = "Football DWH"

DATASETS = {
    "mart_league_table": None,
    "mart_top_scorers": None,
    "mart_match_facts": None,
    "mart_player_overperformers": None,
    "mart_team_xg_trend": None,
    "mart_team_elo_current": None,
    "mart_team_elo_history": None,
}

LEAGUE = "epl"
SEASON = 2025


def login() -> tuple[requests.Session, dict[str, str]]:
    s = requests.Session()
    r = s.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": USER, "password": PASS, "provider": "db", "refresh": True},
        timeout=10,
    )
    r.raise_for_status()
    token = r.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    csrf = s.get(
        f"{SUPERSET_URL}/api/v1/security/csrf_token/", headers=headers, timeout=10
    ).json()["result"]
    headers["X-CSRFToken"] = csrf
    headers["Referer"] = SUPERSET_URL + "/"
    headers["Content-Type"] = "application/json"
    return s, headers


def resolve_datasets(s: requests.Session, h: dict[str, str]) -> None:
    """Резолвит существующие + создаёт отсутствующие в CH-датасорсе (db id=1)."""
    r = s.get(f"{SUPERSET_URL}/api/v1/dataset/?q=(page_size:100)", headers=h)
    r.raise_for_status()
    for ds in r.json()["result"]:
        if ds["table_name"] in DATASETS:
            DATASETS[ds["table_name"]] = ds["id"]
    for tbl, ds_id in list(DATASETS.items()):
        if ds_id is not None:
            continue
        r = s.post(
            f"{SUPERSET_URL}/api/v1/dataset/", headers=h,
            json={"database": 1, "schema": "marts", "table_name": tbl},
        )
        r.raise_for_status()
        DATASETS[tbl] = r.json()["id"]
        print(f"  registered dataset {tbl} (id={DATASETS[tbl]})")
    print(f"datasets: {DATASETS}")


def get_dashboard_id(s: requests.Session, h: dict[str, str]) -> int:
    r = s.get(
        f"{SUPERSET_URL}/api/v1/dashboard/?q=(filters:!((col:dashboard_title,opr:eq,value:'{DASHBOARD_TITLE}')))",
        headers=h,
    )
    r.raise_for_status()
    res = r.json()["result"]
    if not res:
        raise RuntimeError(f"dashboard '{DASHBOARD_TITLE}' not found")
    return res[0]["id"]


def get_chart_by_name(s: requests.Session, h: dict[str, str], name: str) -> int | None:
    r = s.get(
        f"{SUPERSET_URL}/api/v1/chart/?q=(filters:!((col:slice_name,opr:eq,value:'{name}')))",
        headers=h,
    )
    r.raise_for_status()
    res = r.json()["result"]
    return res[0]["id"] if res else None


def upsert_chart(
    s: requests.Session,
    h: dict[str, str],
    name: str,
    viz_type: str,
    dataset_name: str,
    params: dict[str, Any],
    dashboard_id: int,
) -> int:
    ds_id = DATASETS[dataset_name]
    params.setdefault("datasource", f"{ds_id}__table")
    params.setdefault("viz_type", viz_type)
    body = {
        "slice_name": name,
        "viz_type": viz_type,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(params),
        "dashboards": [dashboard_id],
    }
    existing = get_chart_by_name(s, h, name)
    if existing:
        body.pop("datasource_id", None)
        body.pop("datasource_type", None)
        r = s.put(f"{SUPERSET_URL}/api/v1/chart/{existing}", headers=h, json=body)
        r.raise_for_status()
        print(f"  updated {name} (id={existing})")
        return existing
    r = s.post(f"{SUPERSET_URL}/api/v1/chart/", headers=h, json=body)
    r.raise_for_status()
    cid = r.json()["id"]
    print(f"  created {name} (id={cid})")
    return cid


def filter_league_season() -> list[dict[str, Any]]:
    return [
        {
            "expressionType": "SIMPLE",
            "subject": "league_id",
            "operator": "==",
            "comparator": LEAGUE,
            "clause": "WHERE",
        },
        {
            "expressionType": "SIMPLE",
            "subject": "season_year",
            "operator": "==",
            "comparator": str(SEASON),
            "clause": "WHERE",
        },
    ]


def chart_params() -> dict[str, dict[str, Any]]:
    """6 чартов: имя -> (viz_type, dataset, params)."""
    return {
        "Турнирная таблица": {
            "viz_type": "table",
            "dataset": "mart_league_table",
            "params": {
                "query_mode": "raw",
                "all_columns": ["position", "team_title", "pts", "xpts", "gf", "ga", "gd", "ppda"],
                "order_by_cols": [json.dumps(["position", True])],
                "row_limit": 25,
                "adhoc_filters": [],
                "table_timestamp_format": "smart_date",
            },
        },
        "Топ-15 бомбардиров: goals vs xG": {
            "viz_type": "echarts_timeseries_bar",
            "dataset": "mart_top_scorers",
            "params": {
                "x_axis": "player_name",
                "metrics": [
                    {
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "goals"},
                        "aggregate": "MAX",
                        "label": "Goals",
                    },
                    {
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "xg"},
                        "aggregate": "MAX",
                        "label": "xG",
                    },
                ],
                "groupby": [],
                "row_limit": 15,
                "orientation": "vertical",
                "color_scheme": "supersetColors",
                "show_legend": True,
                "legendType": "scroll",
                "legendOrientation": "top",
                "show_value": True,
                "stack": False,
                "x_axis_sort": "Goals",
                "x_axis_sort_asc": False,
                "x_axis_label": "",
                "y_axis_label": "",
                "y_axis_format": ".1f",
                "xAxisLabelRotation": 45,
                "rich_tooltip": True,
                "adhoc_filters": [
                    {
                        "expressionType": "SIMPLE",
                        "subject": "minutes",
                        "operator": ">=",
                        "comparator": "450",
                        "clause": "WHERE",
                    }
                ],
            },
        },
        "Avg total_xG по неделям": {
            "viz_type": "echarts_area",
            "dataset": "mart_match_facts",
            "params": {
                "x_axis": "match_date",
                "time_grain_sqla": "P1W",
                "metrics": [
                    {
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "total_xg"},
                        "aggregate": "AVG",
                        "label": "Avg total xG",
                    }
                ],
                "groupby": [],
                "row_limit": 1000,
                "adhoc_filters": [],
                "show_legend": False,
                "color_scheme": "supersetColors",
                "opacity": 0.4,
                "markerEnabled": True,
                "x_axis_title": "Неделя",
                "y_axis_title": "Avg total xG",
            },
        },
        "Топ-5 overperformers": {
            "viz_type": "table",
            "dataset": "mart_player_overperformers",
            "params": {
                "query_mode": "raw",
                "all_columns": [
                    "player_name", "team_title", "goals", "xg", "goals_minus_xg",
                ],
                "order_by_cols": [json.dumps(["goals_minus_xg", False])],
                "row_limit": 5,
                "adhoc_filters": [],
                "table_timestamp_format": "smart_date",
                "color_pn": True,
                "conditional_formatting": [
                    {
                        "column": "goals_minus_xg",
                        "operator": ">",
                        "targetValue": 0,
                        "colorScheme": "#5AC189",
                    },
                ],
            },
        },
        "Top-10 команд по avg xG": {
            "viz_type": "dist_bar",
            "dataset": "mart_team_xg_trend",
            "params": {
                "groupby": ["team_title"],
                "metrics": [
                    {
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "avg_xg_for"},
                        "aggregate": "MAX",
                        "label": "avg xG for",
                    }
                ],
                "row_limit": 10,
                "order_desc": True,
                "adhoc_filters": [],
                "show_legend": False,
                "color_scheme": "supersetColors",
                "show_bar_value": True,
                "bar_stacked": False,
                "y_axis_format": ".2f",
                "x_axis_label": "Команда",
                "y_axis_label": "Avg xG for",
            },
        },
        "Топ-10 матчей по total_xG": {
            "viz_type": "table",
            "dataset": "mart_match_facts",
            "params": {
                "query_mode": "raw",
                "all_columns": [
                    "match_date", "home_team_title", "home_goals",
                    "away_goals", "away_team_title", "total_xg",
                ],
                "order_by_cols": [json.dumps(["total_xg", False])],
                "row_limit": 10,
                "adhoc_filters": [],
                "table_timestamp_format": "smart_date",
                "conditional_formatting": [
                    {
                        "column": "total_xg",
                        "operator": ">",
                        "targetValue": 4.5,
                        "colorScheme": "#FFB020",
                    },
                ],
            },
        },
        "Топ-10 команд по Elo (текущий)": {
            "viz_type": "dist_bar",
            "dataset": "mart_team_elo_current",
            "params": {
                "groupby": ["team_title"],
                "metrics": [
                    {
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "current_rating"},
                        "aggregate": "MAX",
                        "label": "Elo",
                    }
                ],
                "row_limit": 10,
                "order_desc": True,
                "adhoc_filters": [],
                "show_legend": False,
                "color_scheme": "supersetColors",
                "show_bar_value": True,
                "y_axis_format": ".0f",
                "x_axis_label": "Команда",
                "y_axis_label": "Elo",
            },
        },
        "Эволюция Elo: топ-3 команды лиги": {
            "viz_type": "echarts_timeseries_line",
            "dataset": "mart_team_elo_history",
            "params": {
                "x_axis": "match_date",
                "time_grain_sqla": "P1M",
                "metrics": [
                    {
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "rating_after"},
                        "aggregate": "AVG",
                        "label": "Elo",
                    }
                ],
                "groupby": ["team_title"],
                "row_limit": 5000,
                "adhoc_filters": [
                    {
                        "expressionType": "SQL",
                        "sqlExpression": "is_top3_in_league = 1",
                        "clause": "WHERE",
                    }
                ],
                "time_range": "No filter",
                "show_legend": True,
                "legendOrientation": "top",
                "color_scheme": "supersetColors",
                "x_axis_title": "Месяц",
                "y_axis_title": "Elo",
                "rich_tooltip": True,
                "truncateYAxis": True,
                "logAxis": False,
                "markerEnabled": True,
            },
        },
    }


DASHBOARD_HEADER_MD = """### Football DWH — курсовая 2026
Источники: **Understat** (xG/xA/PPDA, топ-5 лиг, сезоны 2022–2025) + StatsBomb (исторические).
Стек: Airflow → MinIO → Postgres (DV2.0 + dbt + datavault4dbt) → ClickHouse → Superset.

Фильтры справа переключают **лигу** и **сезон** для всех графиков сразу.
**Коды лиг:** `epl` = Premier League · `la_liga` = La Liga · `bundesliga` = Bundesliga · `serie_a` = Serie A · `ligue_1` = Ligue 1.
"""


def build_dashboard_layout(chart_ids: dict[str, int]) -> dict[str, Any]:
    """Layout: header (markdown) + 3 ряда по 2 чарта.
    Структура Superset position_json: dict с GRID_ID, ROW-uuid, CHART-uuid.
    """
    def cid(name: str) -> int: return chart_ids[name]

    names = list(chart_ids.keys())
    rows = [
        (names[0], names[3]),  # таблица + top overperformers
        (names[1], names[4]),  # bar bombardirov + bar teams xG
        (names[2], names[5]),  # area xG + матчи
        (names[6], names[7]),  # Elo bar + Elo line
    ]

    layout: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [],
            "parents": ["ROOT_ID"],
        },
    }

    # Markdown header строка
    header_row_id = "ROW-" + uuid.uuid4().hex[:10]
    header_md_id = "MARKDOWN-" + uuid.uuid4().hex[:10]
    layout["GRID_ID"]["children"].append(header_row_id)
    layout[header_row_id] = {
        "type": "ROW",
        "id": header_row_id,
        "children": [header_md_id],
        "parents": ["ROOT_ID", "GRID_ID"],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }
    layout[header_md_id] = {
        "type": "MARKDOWN",
        "id": header_md_id,
        "children": [],
        "parents": ["ROOT_ID", "GRID_ID", header_row_id],
        "meta": {"width": 12, "height": 22, "code": DASHBOARD_HEADER_MD},
    }

    for left, right in rows:
        row_id = "ROW-" + uuid.uuid4().hex[:10]
        chart_left = "CHART-" + uuid.uuid4().hex[:10]
        chart_right = "CHART-" + uuid.uuid4().hex[:10]
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [chart_left, chart_right],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        layout[chart_left] = {
            "type": "CHART",
            "id": chart_left,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {"width": 6, "height": 50, "chartId": cid(left), "uuid": str(uuid.uuid4())},
        }
        layout[chart_right] = {
            "type": "CHART",
            "id": chart_right,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {"width": 6, "height": 50, "chartId": cid(right), "uuid": str(uuid.uuid4())},
        }
    return layout


def build_native_filters() -> list[dict[str, Any]]:
    """Native filters в дашборде: League (default epl) + Season (default 2025).
    Применяются ко всем чартам, у которых в датасете есть колонка league_id/season_year.
    """
    targets_league = [
        {"datasetId": ds_id, "column": {"name": "league_id"}}
        for ds_id in DATASETS.values()
    ]
    # Elo-витрины кросс-сезонные (рейтинг накапливается через все сезоны),
    # season filter к ним не применяем — иначе history-график будет урезан.
    targets_season = [
        {"datasetId": ds_id, "column": {"name": "season_year"}}
        for name, ds_id in DATASETS.items()
        if name not in ("mart_team_elo_current", "mart_team_elo_history")
    ]
    return [
        {
            "id": "NATIVE_FILTER-league",
            "name": "Лига",
            "filterType": "filter_select",
            "type": "NATIVE_FILTER",
            "targets": targets_league,
            "defaultDataMask": {
                "filterState": {"value": [LEAGUE]},
                "extraFormData": {"filters": [{"col": "league_id", "op": "IN", "val": [LEAGUE]}]},
            },
            "controlValues": {
                "multiSelect": False,
                "enableEmptyFilter": True,
                "defaultToFirstItem": False,
                "inverseSelection": False,
                "searchAllOptions": False,
            },
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "cascadeParentIds": [],
        },
        {
            "id": "NATIVE_FILTER-season",
            "name": "Сезон",
            "filterType": "filter_select",
            "type": "NATIVE_FILTER",
            "targets": targets_season,
            "defaultDataMask": {
                "filterState": {"value": [SEASON]},
                "extraFormData": {"filters": [{"col": "season_year", "op": "IN", "val": [SEASON]}]},
            },
            "controlValues": {
                "multiSelect": False,
                "enableEmptyFilter": True,
                "defaultToFirstItem": False,
                "inverseSelection": False,
                "searchAllOptions": False,
            },
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "cascadeParentIds": [],
        },
    ]


def update_dashboard_layout(
    s: requests.Session, h: dict[str, str], dashboard_id: int, layout: dict[str, Any]
) -> None:
    json_metadata = {
        "native_filter_configuration": build_native_filters(),
        "color_scheme": "supersetColors",
        "refresh_frequency": 0,
        "shared_label_colors": {},
    }
    r = s.put(
        f"{SUPERSET_URL}/api/v1/dashboard/{dashboard_id}",
        headers=h,
        json={
            "position_json": json.dumps(layout),
            "json_metadata": json.dumps(json_metadata),
            "css": "",
            "certification_details": "Football DWH курсовая 2026 — Understat + StatsBomb → Postgres DV2.0 → ClickHouse",
        },
    )
    r.raise_for_status()
    print(f"dashboard {dashboard_id} layout + native filters updated ({len(layout) - 2} blocks)")


def main() -> int:
    s, h = login()
    resolve_datasets(s, h)
    dash_id = get_dashboard_id(s, h)
    print(f"dashboard id: {dash_id}")

    chart_ids: dict[str, int] = {}
    print("creating charts...")
    for name, spec in chart_params().items():
        cid = upsert_chart(
            s, h, name, spec["viz_type"], spec["dataset"], spec["params"], dash_id
        )
        chart_ids[name] = cid

    layout = build_dashboard_layout(chart_ids)
    update_dashboard_layout(s, h, dash_id, layout)
    print(f"\nDONE. Открой http://localhost:8088/superset/dashboard/{dash_id}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
