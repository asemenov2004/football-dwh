"""Снимает PNG-скриншоты дашбордов Superset для отчёта/презентации.

Через Selenium headless Chrome логинится под admin, проходит по дашбордам,
ждёт пока чарты прорендерятся (window.networkIdle proxy через time.sleep),
сохраняет full-page screenshots в docs/screenshots/.

Запуск: python scripts/capture_dashboards.py
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
PASS = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")

OUT_DIR = Path(__file__).resolve().parent.parent / "docs" / "screenshots"

DASHBOARDS = [
    (1, "01_football_dwh.png"),
    (2, "02_european_teams.png"),
]


def make_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,3000")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    return webdriver.Chrome(options=opts)


def login(driver: webdriver.Chrome) -> None:
    driver.get(f"{SUPERSET_URL}/login/")
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.NAME, "username"))
    ).send_keys(USER)
    driver.find_element(By.NAME, "password").send_keys(PASS)
    driver.find_element(By.CSS_SELECTOR, 'input[type="submit"]').click()
    WebDriverWait(driver, 15).until(EC.url_contains("/superset/welcome"))


def capture(driver: webdriver.Chrome, dashboard_id: int, fname: str) -> None:
    url = f"{SUPERSET_URL}/superset/dashboard/{dashboard_id}/?standalone=3"
    driver.get(url)
    # Ждём появление любого чарта на странице
    WebDriverWait(driver, 45).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".dashboard, .grid-container, .dashboard-content"))
    )
    # Даём чартам прогрузиться (нет нативного "all charts loaded" события в Superset)
    time.sleep(15)

    # Полная высота страницы
    height = driver.execute_script("return document.body.scrollHeight")
    driver.set_window_size(1920, max(height + 200, 3000))
    time.sleep(2)

    out = OUT_DIR / fname
    out.parent.mkdir(parents=True, exist_ok=True)
    driver.save_screenshot(str(out))
    size_kb = out.stat().st_size // 1024
    print(f"  saved {out.name} ({size_kb} KiB)")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    driver = make_driver()
    try:
        print(f"login {USER}@{SUPERSET_URL}")
        login(driver)
        for did, fname in DASHBOARDS:
            print(f"capturing dashboard {did} -> {fname}")
            capture(driver, did, fname)
        print(f"\nDONE. {len(DASHBOARDS)} screenshots in {OUT_DIR}")
        return 0
    finally:
        driver.quit()


if __name__ == "__main__":
    sys.exit(main())
