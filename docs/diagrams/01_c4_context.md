# C4: Context-уровень

Внешние границы Football DWH: один пользователь и два источника данных.
Внутреннее устройство — на уровне Containers (`02_c4_containers.md`).

```mermaid
flowchart LR
    classDef person fill:#08427B,stroke:#052E56,color:#fff
    classDef system fill:#1168BD,stroke:#0B4884,color:#fff
    classDef external fill:#999999,stroke:#6B6B6B,color:#fff

    user(("Студент-аналитик<br/><i>[Person]</i><br/>смотрит футбольную<br/>статистику в дашбордах"))

    dwh["<b>Football DWH</b><br/><i>[Software System]</i><br/>Хранилище футбольной статистики:<br/>ingestion + DV2.0 + витрины + BI"]

    understat["Understat.com<br/><i>[External System]</i><br/>HTML-страницы<br/>с xG/xA/PPDA<br/>(топ-5 лиг, 2014+)"]

    statsbomb["StatsBomb Open Data<br/><i>[External System]</i><br/>GitHub-репозиторий<br/>с JSON-разметкой матчей<br/>(La Liga focus)"]

    user -->|"смотрит дашборды<br/>HTTPS"| dwh
    understat -->|"парсинг HTML<br/>HTTPS"| dwh
    statsbomb -->|"raw JSON<br/>HTTPS"| dwh

    class user person
    class dwh system
    class understat,statsbomb external
```

## Описание

| Элемент | Тип | Роль |
|---|---|---|
| **Студент-аналитик** | Person | Открывает дашборды Superset, фильтрует по лиге/сезону |
| **Football DWH** | System | Целевая система — данные, расчёты, BI |
| **Understat** | External | Основной источник: xG, xA, PPDA, расширенная разметка матчей |
| **StatsBomb Open Data** | External | Дополнительный источник: исторические данные La Liga (Barcelona-фокус) |

Архитектура внутри `Football DWH` показана на следующей диаграмме —
[02_c4_containers.md](./02_c4_containers.md).
