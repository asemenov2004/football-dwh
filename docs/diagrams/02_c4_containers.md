# C4: Containers-уровень

Внутреннее устройство Football DWH — 6 контейнеров и протоколы между ними.
Все контейнеры запускаются через `docker-compose` (см. `docker-compose.yml`).

```mermaid
flowchart TB
    classDef person fill:#08427B,stroke:#052E56,color:#fff
    classDef container fill:#1168BD,stroke:#0B4884,color:#fff
    classDef external fill:#999999,stroke:#6B6B6B,color:#fff
    classDef storage fill:#1168BD,stroke:#0B4884,color:#fff,stroke-dasharray:5

    user(("Студент-аналитик<br/><i>[Person]</i>"))

    understat["Understat.com<br/><i>[External]</i>"]
    statsbomb["StatsBomb<br/>Open Data<br/><i>[External]</i>"]

    subgraph DWH["Football DWH"]
        airflow["<b>Airflow 2.9</b><br/><i>[Orchestrator]</i><br/>5 DAGs:<br/>ingest_understat_daily<br/>ingest_statsbomb<br/>stage_load_*<br/>dbt_raw_vault<br/>build_marts"]

        minio[("<b>MinIO</b><br/><i>[Object Store, S3]</i><br/>Buckets:<br/>raw-understat/<br/>raw-statsbomb/<br/>marts/")]

        postgres[("<b>Postgres 16</b><br/><i>[OLTP]</i><br/>Schemas:<br/>stage.*<br/>public_raw_vault.*<br/>public_business_vault.*<br/>public_marts.*")]

        spark["<b>Spark 3.5</b><br/><i>[Compute]</i><br/>Jobs:<br/>calculate_elo<br/>marts_pg_to_minio"]

        clickhouse[("<b>ClickHouse 24.8</b><br/><i>[OLAP]</i><br/>marts.*<br/>(BI-витрины)")]

        superset["<b>Superset 4.x</b><br/><i>[BI]</i><br/>2 дашборда,<br/>13 чартов"]
    end

    understat -->|"HTML/JSON<br/>HTTPS"| airflow
    statsbomb -->|"JSON<br/>HTTPS"| airflow

    airflow -->|"raw JSON<br/>S3 API"| minio
    airflow -->|"stage.* INSERT<br/>JDBC"| postgres
    airflow -->|"dbt run/test<br/>subprocess"| postgres
    airflow -->|"spark-submit<br/>docker exec"| spark

    spark -->|"JDBC read mart_match_facts<br/>JDBC write mart_team_elo_*"| postgres
    spark -->|"JDBC read marts<br/>write Parquet"| postgres
    spark -->|"Parquet upload<br/>S3 API"| minio

    minio -->|"INSERT FROM s3()<br/>HTTP"| clickhouse

    clickhouse -->|"SELECT<br/>HTTP"| superset

    user -->|"HTTPS"| superset

    class user person
    class understat,statsbomb external
    class airflow,spark,superset container
    class minio,postgres,clickhouse storage
```

## Контейнеры

| Контейнер | Технология | Роль |
|---|---|---|
| **Airflow** | Apache Airflow 2.9, Python 3.11 | Оркестрация всех DAG-ов; Datasets-цепочка для Understat |
| **MinIO** | MinIO (S3-совместимый) | Raw lake (партиции `dt=YYYY-MM-DD/source/endpoint/`) + хранилище Parquet-витрин |
| **Postgres** | PostgreSQL 16 | DV2.0 (raw_vault + business_vault) + промежуточные marts перед переливом в CH |
| **Spark** | Apache Spark 3.5 (master+1 worker) | Расчёт Elo (per-league ClubElo) + перелив `mart_*` в Parquet |
| **ClickHouse** | ClickHouse 24.8 | OLAP-слой для BI; финальное место витрин |
| **Superset** | Apache Superset 4.x | Дашборды и чарты поверх ClickHouse |

## Ключевые протоколы и интеграции

- **Airflow → Spark**: запуск через `docker exec spark-master spark-submit` (внутри `build_marts` DAG-а). Альтернатива через `SparkSubmitOperator` отвергнута — нужны JAR-зависимости через Maven, которые из РФ нестабильно отдаются.
- **PG → ClickHouse**: переливаем не напрямую (нет CH JDBC connector в Spark образе), а через Parquet в MinIO + `ClickHouse INSERT FROM s3()`. Дешевле и кэшируется.
- **Datasets** (Airflow 2.4+): `ingest_understat_daily → stage_load_understat → dbt_raw_vault` срабатывают по событиям, а не по cron.
