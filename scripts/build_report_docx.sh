#!/usr/bin/env bash
# Конвертация docs/отчёт.md → docs/отчёт.docx через pandoc.
# Mermaid-блоки автоматически рендерятся в PNG через mermaid-cli (mmdc),
# если он установлен. Без mmdc — pandoc оставит код-блок как текст.
#
# Зависимости (Windows):
#   winget install JohnMacFarlane.Pandoc
#   npm install -g @mermaid-js/mermaid-cli
#
# Запуск: bash scripts/build_report_docx.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DOCS="$ROOT/docs"
SRC="$DOCS/отчёт.md"
OUT="$DOCS/отчёт.docx"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

if ! command -v pandoc >/dev/null 2>&1; then
    echo "ERROR: pandoc не найден в PATH"
    echo "Установи: winget install JohnMacFarlane.Pandoc"
    exit 1
fi

# Mermaid → PNG (опционально). Без mmdc Mermaid останется как код-блок.
HAVE_MMDC=0
if command -v mmdc >/dev/null 2>&1; then
    HAVE_MMDC=1
    echo "==> mermaid-cli найден, рендерю Mermaid-блоки в PNG"
fi

cp "$SRC" "$TMPDIR/отчёт.md"

if [ "$HAVE_MMDC" -eq 1 ]; then
    # Извлечь mermaid-блоки и заменить на ![](path/to/png)
    python3 - <<'PYEOF'
import re
import subprocess
from pathlib import Path
import os

tmpdir = Path(os.environ.get("TMPDIR", "/tmp"))
src = tmpdir / "отчёт.md"
text = src.read_text(encoding="utf-8")
img_dir = tmpdir / "mermaid_png"
img_dir.mkdir(exist_ok=True)

def replace(match: re.Match) -> str:
    idx = replace.idx
    replace.idx += 1
    block = match.group(1)
    mmd = img_dir / f"diagram_{idx:02d}.mmd"
    png = img_dir / f"diagram_{idx:02d}.png"
    mmd.write_text(block, encoding="utf-8")
    subprocess.run(["mmdc", "-i", str(mmd), "-o", str(png), "-w", "1600"], check=True)
    return f"![Диаграмма {idx}]({png})"

replace.idx = 1
new = re.sub(r"```mermaid\n(.*?)\n```", replace, text, flags=re.DOTALL)
src.write_text(new, encoding="utf-8")
print(f"  отрендерено {replace.idx - 1} mermaid-блоков")
PYEOF
fi

echo "==> pandoc → $OUT"
cd "$DOCS"
pandoc "$TMPDIR/отчёт.md" \
    --from=gfm+tex_math_dollars \
    --to=docx \
    --toc \
    --toc-depth=2 \
    --resource-path="$DOCS:$ROOT" \
    -o "$OUT"

echo "==> готово: $OUT"
ls -lh "$OUT"
