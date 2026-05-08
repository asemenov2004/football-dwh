"""Создаёт чарты в Superset и привязывает к двум дашбордам:
  - Football DWH (основной, 9 чартов: 8 + SB la_liga history)
  - Football DWH — European Teams (кросс-лиговый, 4 чарта)

Идемпотентность: чарт находится по slice_name → обновляется (PUT).
Дашборд European Teams создаётся, если его нет.

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

DASHBOARD_MAIN = "Football DWH"
DASHBOARD_EURO = "Football DWH — European Teams"

DATASETS = {
    "mart_league_table": None,
    "mart_top_scorers": None,
    "mart_match_facts": None,
    "mart_player_overperformers": None,
    "mart_team_xg_trend": None,
    "mart_team_elo_current": None,
    "mart_team_elo_history": None,
    "mart_sb_la_liga_history": None,
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


def get_dashboard_id(s: requests.Session, h: dict[str, str], title: str) -> int | None:
    r = s.get(
        f"{SUPERSET_URL}/api/v1/dashboard/?q=(filters:!((col:dashboard_title,opr:eq,value:'{title}')))",
        headers=h,
    )
    r.raise_for_status()
    res = r.json()["result"]
    return res[0]["id"] if res else None


def create_dashboard(s: requests.Session, h: dict[str, str], title: str) -> int:
    r = s.post(
        f"{SUPERSET_URL}/api/v1/dashboard/", headers=h,
        json={"dashboard_title": title, "published": True},
    )
    r.raise_for_status()
    cid = r.json()["id"]
    print(f"  created dashboard '{title}' (id={cid})")
    return cid


def get_chart_by_name(s: requests.Session, h: dict[str, str], name: str) -> int | None:
    r = s.get(
        f"{SUPERSET_URL}/api/v1/chart/?q=(filters:!((col:slice_name,opr:eq,value:'{name}')))",
        headers=h,
    )
    r.raise_for_status()
    res = r.json()["result"]
    return res[0]["id"] if res else None


def build_query_context(viz_type: str, ds_id: int, params: dict[str, Any]) -> dict[str, Any]:
    """Минимальный query_context для дашборд-рендера.
    Без него Superset 4.x падает с 'Chart has no query context saved'.
    """
    qc_query: dict[str, Any] = {
        "filters": [],
        "extras": {"where": "", "having": ""},
        "applied_time_extras": {},
        "row_limit": params.get("row_limit", 1000),
        "annotation_layers": [],
        "url_params": {},
        "custom_params": {},
        "custom_form_data": {},
    }
    # adhoc filters → SQL clauses
    for f in params.get("adhoc_filters", []):
        if f.get("clause") != "WHERE":
            continue
        if f.get("expressionType") == "SQL":
            qc_query["extras"]["where"] = (
                qc_query["extras"]["where"] + " AND " + f["sqlExpression"]
                if qc_query["extras"]["where"] else f["sqlExpression"]
            )
        else:
            qc_query["filters"].append({
                "col": f["subject"],
                "op": f["operator"],
                "val": f["comparator"],
            })

    if params.get("query_mode") == "raw" or params.get("all_columns"):
        qc_query["columns"] = params.get("all_columns", [])
        qc_query["orderby"] = [json.loads(o) for o in params.get("order_by_cols", [])]
    else:
        # aggregate mode
        groupby = params.get("groupby") or []
        if params.get("x_axis"):
            groupby = list(set([params["x_axis"]] + list(groupby)))
        if params.get("all_columns_x"):
            groupby = [params["all_columns_x"], params["all_columns_y"]]
        qc_query["columns"] = groupby
        qc_query["metrics"] = params.get("metrics") or (
            [params["metric"]] if params.get("metric") else []
        )
        qc_query["orderby"] = [
            (m, not params.get("order_desc", True)) for m in qc_query["metrics"][:1]
        ] if qc_query["metrics"] else []

    return {
        "datasource": {"id": ds_id, "type": "table"},
        "force": False,
        "queries": [qc_query],
        "form_data": {**params, "datasource": f"{ds_id}__table", "viz_type": viz_type},
        "result_format": "json",
        "result_type": "full",
    }


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
    qc = build_query_context(viz_type, ds_id, params)
    body = {
        "slice_name": name,
        "viz_type": viz_type,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(params),
        "query_context": json.dumps(qc),
        "dashboards": [dashboard_id],
    }
    existing = get_chart_by_name(s, h, name)
    if existing:
        r = s.put(f"{SUPERSET_URL}/api/v1/chart/{existing}", headers=h, json=body)
        r.raise_for_status()
        print(f"  updated {name} (id={existing})")
        return existing
    r = s.post(f"{SUPERSET_URL}/api/v1/chart/", headers=h, json=body)
    r.raise_for_status()
    cid = r.json()["id"]
    print(f"  created {name} (id={cid})")
    return cid


# ------------- ЧАРТЫ ОСНОВНОГО ДАШБОРДА -------------
# Правило форматирования: xG/PPDA/xPTS/avg/total → .2f, Elo → .0f, goals/pts → целые.

def main_charts() -> dict[str, dict[str, Any]]:
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
                "column_config": {
                    "xpts": {"d3NumberFormat": ".2f"},
                    "ppda": {"d3NumberFormat": ".2f"},
                },
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
                "y_axis_format": ".2f",
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
                "y_axis_format": ".2f",
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
                "column_config": {
                    "xg": {"d3NumberFormat": ".2f"},
                    "goals_minus_xg": {"d3NumberFormat": ".2f"},
                },
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
                "column_config": {
                    "total_xg": {"d3NumberFormat": ".2f"},
                },
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
                "y_axis_format": ".0f",
                "rich_tooltip": True,
                "truncateYAxis": True,
                "logAxis": False,
                "markerEnabled": True,
            },
        },
        "StatsBomb: Barcelona по сезонам La Liga": {
            "viz_type": "table",
            "dataset": "mart_sb_la_liga_history",
            "params": {
                "query_mode": "raw",
                "all_columns": [
                    "season_year", "matches_played",
                    "wins", "draws", "losses",
                    "goals_scored", "goals_conceded",
                    "avg_goals_scored", "avg_goals_conceded",
                ],
                "order_by_cols": [json.dumps(["season_year", False])],
                "row_limit": 25,
                "adhoc_filters": [],
                "table_timestamp_format": "smart_date",
                "column_config": {
                    "avg_goals_scored":   {"d3NumberFormat": ".2f"},
                    "avg_goals_conceded": {"d3NumberFormat": ".2f"},
                },
                "conditional_formatting": [
                    {
                        "column": "wins",
                        "operator": ">",
                        "targetValue": 25,
                        "colorScheme": "#1FA8C9",
                    },
                ],
            },
        },
    }


# ------------- ЧАРТЫ ВТОРОГО ДАШБОРДА (European Teams) -------------

def euro_charts() -> dict[str, dict[str, Any]]:
    return {
        "Топ-20 клубов Европы по Elo": {
            "viz_type": "dist_bar",
            "dataset": "mart_team_elo_current",
            "params": {
                "groupby": ["team_title", "league_id"],
                "metrics": [
                    {
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "current_rating"},
                        "aggregate": "MAX",
                        "label": "Elo",
                    }
                ],
                "row_limit": 20,
                "order_desc": True,
                "adhoc_filters": [],
                "show_legend": True,
                "legendOrientation": "top",
                "color_scheme": "supersetColors",
                "show_bar_value": True,
                "bar_stacked": False,
                "y_axis_format": ".0f",
                "x_axis_label": "Команда",
                "y_axis_label": "Elo",
                "xAxisLabelRotation": 45,
            },
        },
        "Топ-15 over/underperformers: PTS - xPTS": {
            "viz_type": "table",
            "dataset": "mart_league_table",
            "params": {
                "query_mode": "raw",
                "all_columns": [
                    "team_title", "league_id", "season_year",
                    "pts", "xpts", "position",
                ],
                "order_by_cols": [json.dumps(["pts", False])],
                "row_limit": 30,
                "adhoc_filters": [
                    {
                        "expressionType": "SQL",
                        "sqlExpression": "ABS(pts - xpts) >= 8",
                        "clause": "WHERE",
                    }
                ],
                "table_timestamp_format": "smart_date",
                "column_config": {
                    "xpts": {"d3NumberFormat": ".2f"},
                },
                "conditional_formatting": [
                    {
                        "column": "pts",
                        "operator": ">",
                        "targetValue": 70,
                        "colorScheme": "#1FA8C9",
                    },
                ],
            },
        },
        "Топ-15 голеадоров Европы (все лиги)": {
            "viz_type": "table",
            "dataset": "mart_top_scorers",
            "params": {
                "query_mode": "raw",
                "all_columns": [
                    "player_name", "teams_concat", "league_id",
                    "season_year", "goals", "xg", "assists",
                ],
                "order_by_cols": [json.dumps(["goals", False])],
                "row_limit": 15,
                "adhoc_filters": [
                    {
                        "expressionType": "SIMPLE",
                        "subject": "minutes",
                        "operator": ">=",
                        "comparator": "900",
                        "clause": "WHERE",
                    }
                ],
                "table_timestamp_format": "smart_date",
                "column_config": {
                    "xg": {"d3NumberFormat": ".2f"},
                },
                "conditional_formatting": [
                    {
                        "column": "goals",
                        "operator": ">",
                        "targetValue": 25,
                        "colorScheme": "#1FA8C9",
                    },
                ],
            },
        },
        "Топ-апсеты: xG предсказал не того победителя": {
            "viz_type": "table",
            "dataset": "mart_match_facts",
            "params": {
                "query_mode": "raw",
                "all_columns": [
                    "match_date", "league_id",
                    "home_team_title", "home_goals", "home_xg",
                    "away_team_title", "away_goals", "away_xg",
                    "total_xg",
                ],
                # сортируем по total_xg DESC — матчи с большим объёмом игры, исход которых
                # противоречил xG-прогнозу, наиболее показательны
                "order_by_cols": [json.dumps(["total_xg", False])],
                "row_limit": 25,
                "adhoc_filters": [
                    {
                        "expressionType": "SQL",
                        "sqlExpression": (
                            "SIGN(home_xg - away_xg) <> SIGN(home_goals - away_goals) "
                            "AND home_goals != away_goals "
                            "AND total_xg > 2.5"
                        ),
                        "clause": "WHERE",
                    }
                ],
                "table_timestamp_format": "smart_date",
                "column_config": {
                    "home_xg": {"d3NumberFormat": ".2f"},
                    "away_xg": {"d3NumberFormat": ".2f"},
                    "total_xg": {"d3NumberFormat": ".2f"},
                },
                "conditional_formatting": [
                    {
                        "column": "total_xg",
                        "operator": ">",
                        "targetValue": 4.0,
                        "colorScheme": "#FFB020",
                    },
                ],
            },
        },
    }


# ------------- LAYOUT ОСНОВНОГО ДАШБОРДА -------------

DASHBOARD_HEADER_MD = """### Football DWH — курсовая 2026
Источники: **Understat** (xG/xA/PPDA, топ-5 лиг, сезоны 2022–2025) + **StatsBomb** (исторические).
Стек: Airflow → MinIO → Postgres (DV2.0 + dbt + datavault4dbt) → ClickHouse → Superset.

Фильтры справа переключают **лигу** и **сезон** для всех графиков сразу.
**Коды лиг:** `epl` = Premier League · `la_liga` = La Liga · `bundesliga` = Bundesliga · `serie_a` = Serie A · `ligue_1` = Ligue 1.
"""

EURO_HEADER_MD = """### Football DWH — Команды Европы
Кросс-лиговые витрины: фильтр по лигам **не применяется**, чтобы видеть всю топ-5 + UCL разом.
Витрины-источники: `mart_team_elo_current`, `mart_league_table`, `mart_match_facts`.
"""


def build_main_layout(chart_ids: dict[str, int]) -> dict[str, Any]:
    """9 чартов: header + 4 ряда по 2 чарта + 1 ряд один чарт (SB)."""
    names = list(chart_ids.keys())
    rows = [
        (names[0], names[3]),  # таблица + overperformers
        (names[1], names[4]),  # bar bombardirov + bar teams xG
        (names[2], names[5]),  # area xG + матчи
        (names[6], names[7]),  # Elo bar + Elo line
        (names[8], None),      # SB la_liga history (полный ряд)
    ]
    return _layout_with_rows(rows, DASHBOARD_HEADER_MD, chart_ids)


def build_euro_layout(chart_ids: dict[str, int]) -> dict[str, Any]:
    names = list(chart_ids.keys())
    rows = [
        (names[0], names[1]),  # Elo bar + scatter PTS/xPTS
        (names[2], names[3]),  # heatmap + upsets
    ]
    return _layout_with_rows(rows, EURO_HEADER_MD, chart_ids)


def _layout_with_rows(
    rows: list[tuple[str, str | None]],
    header_md: str,
    chart_ids: dict[str, int],
) -> dict[str, Any]:
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
        "meta": {"width": 12, "height": 22, "code": header_md},
    }

    for left, right in rows:
        row_id = "ROW-" + uuid.uuid4().hex[:10]
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        if right is None:
            chart_full = "CHART-" + uuid.uuid4().hex[:10]
            layout[row_id]["children"] = [chart_full]
            layout[chart_full] = {
                "type": "CHART",
                "id": chart_full,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {"width": 12, "height": 50, "chartId": chart_ids[left], "uuid": str(uuid.uuid4())},
            }
            continue

        chart_left = "CHART-" + uuid.uuid4().hex[:10]
        chart_right = "CHART-" + uuid.uuid4().hex[:10]
        layout[row_id]["children"] = [chart_left, chart_right]
        layout[chart_left] = {
            "type": "CHART",
            "id": chart_left,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {"width": 6, "height": 50, "chartId": chart_ids[left], "uuid": str(uuid.uuid4())},
        }
        layout[chart_right] = {
            "type": "CHART",
            "id": chart_right,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {"width": 6, "height": 50, "chartId": chart_ids[right], "uuid": str(uuid.uuid4())},
        }
    return layout


def build_main_native_filters(chart_ids: dict[str, int]) -> list[dict[str, Any]]:
    """League + Season для основного дашборда. SB-чарт и Elo-чарты исключаем
    из scope сезонного фильтра — у них свои сезонные диапазоны (SB: 1973-2020,
    Elo: кросс-сезонный)."""
    targets_league = [
        {"datasetId": ds_id, "column": {"name": "league_id"}}
        for name, ds_id in DATASETS.items()
        if name != "mart_sb_la_liga_history"
    ]
    targets_season = [
        {"datasetId": ds_id, "column": {"name": "season_year"}}
        for name, ds_id in DATASETS.items()
        if name not in ("mart_team_elo_current", "mart_team_elo_history", "mart_sb_la_liga_history")
    ]
    # Исключаем чарты из scope сезонного фильтра по id
    sb_chart = chart_ids.get("StatsBomb: Barcelona по сезонам La Liga")
    elo_bar = chart_ids.get("Топ-10 команд по Elo (текущий)")
    elo_line = chart_ids.get("Эволюция Elo: топ-3 команды лиги")
    excluded_season = [c for c in [sb_chart, elo_bar, elo_line] if c is not None]
    excluded_league = [c for c in [sb_chart] if c is not None]
    return [
        _native_filter("league", "Лига", "league_id", targets_league, [LEAGUE], excluded_league),
        _native_filter("season", "Сезон", "season_year", targets_season, [SEASON], excluded_season),
    ]


def _native_filter(
    fid: str,
    name: str,
    col: str,
    targets: list[dict[str, Any]],
    default: list[Any],
    excluded_chart_ids: list[int] | None = None,
) -> dict[str, Any]:
    return {
        "id": f"NATIVE_FILTER-{fid}",
        "name": name,
        "filterType": "filter_select",
        "type": "NATIVE_FILTER",
        "targets": targets,
        "defaultDataMask": {
            "filterState": {"value": default},
            "extraFormData": {"filters": [{"col": col, "op": "IN", "val": default}]},
        },
        "controlValues": {
            "multiSelect": False,
            "enableEmptyFilter": True,
            "defaultToFirstItem": False,
            "inverseSelection": False,
            "searchAllOptions": False,
        },
        "scope": {"rootPath": ["ROOT_ID"], "excluded": excluded_chart_ids or []},
        "cascadeParentIds": [],
    }


def update_dashboard(
    s: requests.Session,
    h: dict[str, str],
    dashboard_id: int,
    layout: dict[str, Any],
    native_filters: list[dict[str, Any]],
    cert: str,
) -> None:
    json_metadata = {
        "native_filter_configuration": native_filters,
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
            "certification_details": cert,
        },
    )
    r.raise_for_status()
    print(f"  dashboard {dashboard_id} layout + filters updated ({len(layout) - 2} blocks)")


def build_charts_for_dashboard(
    s: requests.Session,
    h: dict[str, str],
    specs: dict[str, dict[str, Any]],
    dashboard_id: int,
) -> dict[str, int]:
    chart_ids: dict[str, int] = {}
    for name, spec in specs.items():
        cid = upsert_chart(
            s, h, name, spec["viz_type"], spec["dataset"], spec["params"], dashboard_id
        )
        chart_ids[name] = cid
    return chart_ids


def main() -> int:
    s, h = login()
    resolve_datasets(s, h)

    main_id = get_dashboard_id(s, h, DASHBOARD_MAIN)
    if main_id is None:
        main_id = create_dashboard(s, h, DASHBOARD_MAIN)
    print(f"main dashboard id: {main_id}")

    euro_id = get_dashboard_id(s, h, DASHBOARD_EURO)
    if euro_id is None:
        euro_id = create_dashboard(s, h, DASHBOARD_EURO)
    print(f"euro dashboard id: {euro_id}")

    print("creating main charts...")
    main_ids = build_charts_for_dashboard(s, h, main_charts(), main_id)
    update_dashboard(
        s, h, main_id,
        build_main_layout(main_ids),
        build_main_native_filters(main_ids),
        "Football DWH курсовая 2026 — Understat + StatsBomb → Postgres DV2.0 → ClickHouse",
    )

    print("creating euro charts...")
    euro_ids = build_charts_for_dashboard(s, h, euro_charts(), euro_id)
    update_dashboard(
        s, h, euro_id,
        build_euro_layout(euro_ids),
        [],  # без native-фильтров — кросс-лиговые
        "Football DWH — кросс-лиговые витрины Европы",
    )

    print(f"\nDONE. Main: http://localhost:8088/superset/dashboard/{main_id}/")
    print(f"      Euro: http://localhost:8088/superset/dashboard/{euro_id}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
