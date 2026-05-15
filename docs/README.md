# docs/

Документация и материалы курсовой работы.

## Диаграммы

Все диаграммы в Mermaid, рендерятся как в Markdown, так и в PNG.

| Файл | Тип | Что показывает |
|---|---|---|
| [diagrams/01_c4_context.md](diagrams/01_c4_context.md) | C4 Context | Внешние границы DWH: пользователь и два источника данных |
| [diagrams/02_c4_containers.md](diagrams/02_c4_containers.md) | C4 Containers | Внутренние контейнеры DWH (Airflow / MinIO / PG / Spark / CH / Superset) и протоколы между ними |
| [diagrams/03_dfd.md](diagrams/03_dfd.md) | DFD | Поток данных по шагам конвейера: ingestion → raw → stage → DV → marts → CH → BI |
| [diagrams/04_er_data_vault.md](diagrams/04_er_data_vault.md) | ER | Hubs/Links/Satellites Raw Vault + PIT/Bridge Business Vault |
| [diagrams/05_er_marts.md](diagrams/05_er_marts.md) | ER + lineage | Финальные витрины и их источники в DV |

PNG-версии диаграмм лежат в [diagrams/png/](diagrams/png/), генерируются скриптом `scripts/render_mermaid.py`.

## Текстовые материалы

| Файл | Содержимое |
|---|---|
| [отчёт.md](отчёт.md) | Курсовой отчёт (исходник для .docx) |
| [презентация.md](презентация.md) | План слайдов для защиты |

## Скриншоты

Скриншоты дашбордов Superset — в [screenshots/](screenshots/).

## Как открывать

- На GitHub все `.md` файлы рендерятся в браузере, включая Mermaid.
- В VS Code — нужно расширение **Markdown Preview Mermaid Support**.
