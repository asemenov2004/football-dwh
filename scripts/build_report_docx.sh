#!/usr/bin/env bash
# Конвертация docs/отчёт.md → docs/отчёт.docx через pandoc.
# Картинки диаграмм должны быть предварительно отрендерены:
#   python scripts/render_mermaid.py
#
# Зависимости: pandoc (winget install JohnMacFarlane.Pandoc)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DOCS="$ROOT/docs"
SRC="$DOCS/отчёт.md"
OUT="$DOCS/отчёт.docx"

if ! command -v pandoc >/dev/null 2>&1; then
    PANDOC_PATH="/c/Users/Пользователь/AppData/Local/Pandoc/pandoc.exe"
    if [ -x "$PANDOC_PATH" ]; then
        export PATH="$PATH:/c/Users/Пользователь/AppData/Local/Pandoc"
    else
        echo "ERROR: pandoc не найден"
        echo "Установи: winget install JohnMacFarlane.Pandoc"
        exit 1
    fi
fi

if [ ! -f "$DOCS/diagrams/png/01_c4_context.png" ]; then
    echo "WARN: PNG-диаграммы не найдены, перерендерю"
    python "$ROOT/scripts/render_mermaid.py"
fi

echo "==> pandoc → $OUT"
pandoc "$SRC" \
    --from=gfm+tex_math_dollars \
    --to=docx \
    --toc \
    --toc-depth=2 \
    --resource-path="$DOCS:$ROOT" \
    -o "$OUT"

echo "==> готово: $OUT"
ls -lh "$OUT"
