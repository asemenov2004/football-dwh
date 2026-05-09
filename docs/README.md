# docs/ — документация для защиты

## Диаграммы

Все диаграммы написаны в **Mermaid** и рендерятся прямо на GitHub.

| Файл | Тип | Что показывает |
|---|---|---|
| [diagrams/01_c4_context.md](diagrams/01_c4_context.md) | C4 Context | Внешние границы DWH: пользователь и два источника данных |
| [diagrams/02_c4_containers.md](diagrams/02_c4_containers.md) | C4 Containers | Внутренние контейнеры DWH (Airflow / MinIO / PG / Spark / CH / Superset) и протоколы |
| [diagrams/03_dfd.md](diagrams/03_dfd.md) | DFD | Поток данных по 8 шагам конвейера: ingestion → raw → stage → DV → marts → CH → BI |
| [diagrams/04_er_data_vault.md](diagrams/04_er_data_vault.md) | ER | Hubs/Links/Satellites Data Vault 2.0 + PIT/Bridge Business Vault |
| [diagrams/05_er_marts.md](diagrams/05_er_marts.md) | ER + lineage | 8 финальных витрин и их источники в DV |

## Текстовая документация

| Файл | Содержимое |
|---|---|
| [отчёт.md](отчёт.md) | Курсовой отчёт (~15-20 стр после конвертации в .docx) |
| [презентация.md](презентация.md) | План 12-15 слайдов для защиты |

## Скриншоты

Скриншоты дашбордов лежат в [screenshots/](screenshots/) — собраны автоматически
скриптом `scripts/capture_dashboards.py` (использует Selenium headless Chrome).

## Конвертация в .docx

Скрипт `scripts/build_report_docx.sh` собирает `отчёт.md` в `.docx` через `pandoc`,
с автоматическим рендером Mermaid-диаграмм в PNG (через `mermaid-cli`).

```bash
bash scripts/build_report_docx.sh
# → docs/отчёт.docx
```

## Как открывать

- На GitHub — все `.md` файлы рендерятся в браузере (включая Mermaid)
- В VS Code — нужно расширение **Markdown Preview Mermaid Support**
- Локально в Word — запустить `scripts/build_report_docx.sh`
