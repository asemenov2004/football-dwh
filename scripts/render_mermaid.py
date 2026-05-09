"""Рендерит Mermaid-блоки из docs/diagrams/*.md в PNG через Selenium.

Альтернатива mermaid-cli (mmdc), который требует Puppeteer/Chromium download
из Google CDN (заблокирован из РФ). Здесь используем уже установленный
Selenium ChromeDriver и mermaid.js с jsDelivr CDN.

Выход: docs/diagrams/png/<filename>_<idx>.png

Запуск: python scripts/render_mermaid.py
"""
from __future__ import annotations

import base64
import re
import sys
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


ROOT = Path(__file__).resolve().parent.parent
DIAGRAMS = ROOT / "docs" / "diagrams"
OUT_DIR = DIAGRAMS / "png"

HTML_TEMPLATE = """<!doctype html>
<html><head>
<meta charset="utf-8">
<style>
  body {{
    margin: 0;
    padding: 40px;
    background: white;
    font-family: 'Segoe UI', Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
  }}
  #diagram {{ display: inline-block; }}
  /* Mermaid выдаёт SVG — заставим его рендериться крупно */
  #diagram svg {{
    width: auto !important;
    height: auto !important;
    max-width: none !important;
    font-size: 16px;
  }}
  #diagram .nodeLabel, #diagram .edgeLabel {{ font-size: 16px !important; }}
  #diagram text {{ font-family: 'Segoe UI', Arial, sans-serif !important; }}
</style>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.min.js"></script>
</head><body>
<div id="diagram" class="mermaid">{code}</div>
<script>
  mermaid.initialize({{
    startOnLoad: false,
    theme: 'default',
    securityLevel: 'loose',
    flowchart: {{ useMaxWidth: false, htmlLabels: true, curve: 'basis' }},
    er: {{ useMaxWidth: false }},
    themeVariables: {{ fontSize: '16px' }},
  }});
  window.renderDone = false;
  mermaid.run({{ querySelector: '#diagram' }}).then(() => {{
    window.renderDone = true;
  }}).catch(e => {{
    document.body.innerHTML = '<pre style="color:red">ERROR: ' + e.message + '</pre>';
    window.renderDone = 'error';
  }});
</script>
</body></html>
"""


def make_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--window-size=3200,3200")
    opts.add_argument("--force-device-scale-factor=3")
    opts.add_argument("--high-dpi-support=1")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--hide-scrollbars")
    return webdriver.Chrome(options=opts)


def render_block(driver: webdriver.Chrome, code: str, out_path: Path) -> None:
    html = HTML_TEMPLATE.format(code=code.replace("`", r"\`"))
    data_url = "data:text/html;base64," + base64.b64encode(html.encode("utf-8")).decode("ascii")
    driver.get(data_url)
    WebDriverWait(driver, 30).until(
        lambda d: d.execute_script("return window.renderDone") is not False
    )
    if driver.execute_script("return window.renderDone") == "error":
        body = driver.find_element(By.TAG_NAME, "body").text
        raise RuntimeError(f"Mermaid render error: {body}")
    time.sleep(0.7)

    # Подгоняем размер окна под контент диаграммы
    elem = driver.find_element(By.ID, "diagram")
    size = elem.size
    width = max(int(size["width"]) + 100, 800)
    height = max(int(size["height"]) + 100, 400)
    driver.set_window_size(width, height)
    time.sleep(0.5)

    # Снимаем через CDP в высоком DPI (deviceScaleFactor=3 ⇒ ~3x размер)
    location = elem.location
    rect_size = elem.size
    clip = {
        "x": location["x"],
        "y": location["y"],
        "width": rect_size["width"],
        "height": rect_size["height"],
        "scale": 3,
    }
    result = driver.execute_cdp_cmd(
        "Page.captureScreenshot",
        {"format": "png", "clip": clip, "captureBeyondViewport": True},
    )
    out_path.write_bytes(base64.b64decode(result["data"]))


def extract_mermaid_blocks(md_path: Path) -> list[str]:
    text = md_path.read_text(encoding="utf-8")
    return re.findall(r"```mermaid\n(.*?)\n```", text, re.DOTALL)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    md_files = sorted(DIAGRAMS.glob("*.md"))
    if not md_files:
        print(f"no .md files in {DIAGRAMS}")
        return 1

    driver = make_driver()
    try:
        total = 0
        for md in md_files:
            blocks = extract_mermaid_blocks(md)
            for idx, code in enumerate(blocks, 1):
                suffix = f"_{idx}" if len(blocks) > 1 else ""
                out = OUT_DIR / f"{md.stem}{suffix}.png"
                print(f"  rendering {md.name} block {idx}/{len(blocks)} -> {out.name}")
                render_block(driver, code, out)
                total += 1
        print(f"\nDONE. {total} PNG в {OUT_DIR}")
        return 0
    finally:
        driver.quit()


if __name__ == "__main__":
    sys.exit(main())
