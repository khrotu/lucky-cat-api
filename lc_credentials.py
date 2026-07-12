from __future__ import annotations
import json
import shutil
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
GUEST_URL = "https://lumo.proton.me/guest/"
COOKIE_DOMAIN_SUFFIX = "proton.me"
AUTH_COOKIE_PREFIX = "AUTH-"
DEFAULT_OUTPUT = Path(__file__).parent / "credentials.json"
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36 Edg/150.0.0.0"
DRIVER_NAMES = {"edge": ("msedgedriver.exe", "msedgedriver"), "chrome": ("chromedriver.exe", "chromedriver")}
MAX_CREDENTIALS = 8
def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
@dataclass
class Credential:
    id: str
    cookie_header: str
    session_id: Optional[str]
    x_pm_uid: str = ""
    user_agent: str = DEFAULT_USER_AGENT
    created_at: str = field(default_factory=_utc_now)
    source: str = "guest"
    @property
    def auth_cookie(self) -> Optional[str]:
        for part in self.cookie_header.split(";"):
            name = part.strip().split("=", 1)[0]
            if name.startswith(AUTH_COOKIE_PREFIX):
                return name
        return None
    def is_valid(self) -> bool:
        return bool(self.cookie_header) and self.auth_cookie is not None
class CredentialFile:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
    def load(self) -> List[Credential]:
        if not self.path.exists():
            return []
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return []
        items = raw.get("credentials", []) if isinstance(raw, dict) else raw
        creds: List[Credential] = []
        for item in items:
            creds.append(Credential(id=item.get("id") or str(uuid.uuid4()), cookie_header=item.get("cookie_header", ""), session_id=item.get("session_id"), x_pm_uid=item.get("x_pm_uid", ""), user_agent=item.get("user_agent", DEFAULT_USER_AGENT), created_at=item.get("created_at", _utc_now()), source=item.get("source", "guest")))
        return creds
    def save(self, credentials: List[Credential]) -> None:
        payload = {"version": 1, "updated_at": _utc_now(), "count": len(credentials), "credentials": [asdict(c) for c in credentials]}
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
def _find_local_driver(browser: str) -> Optional[str]:
    for directory in (Path(__file__).parent, Path.cwd()):
        for name in DRIVER_NAMES.get(browser, ()):
            candidate = directory / name
            if candidate.is_file():
                return str(candidate.resolve())
    for name in DRIVER_NAMES.get(browser, ()):
        found = shutil.which(name)
        if found:
            return found
    return None
def _make_driver(browser: str):
    local = _find_local_driver(browser)
    if browser == "chrome":
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        options = Options()
        options.add_argument("--incognito")
        options.add_argument(f"--user-agent={DEFAULT_USER_AGENT}")
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        service = Service(executable_path=local) if local else None
        return webdriver.Chrome(options=options, service=service)
    from selenium import webdriver
    from selenium.webdriver.edge.options import Options
    from selenium.webdriver.edge.service import Service
    options = Options()
    options.add_argument("--inprivate")
    options.add_argument(f"--user-agent={DEFAULT_USER_AGENT}")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    service = Service(executable_path=local) if local else None
    return webdriver.Edge(options=options, service=service)
def _get_all_cookies(driver) -> List[Dict[str, Any]]:
    try:
        result = driver.execute_cdp_cmd("Network.getAllCookies", {})
        return result.get("cookies", [])
    except Exception:
        return driver.get_cookies()
def _build_cookie_header(cookies: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    seen: set[str] = set()
    for cookie in cookies:
        domain = (cookie.get("domain") or "").lstrip(".")
        if COOKIE_DOMAIN_SUFFIX not in domain:
            continue
        name = cookie.get("name")
        value = cookie.get("value")
        if not name or value is None or name in seen:
            continue
        if name.startswith("REFRESH-"):
            continue
        seen.add(name)
        parts.append(f"{name}={value}")
    return "; ".join(parts)
def _extract_session_id(cookies: List[Dict[str, Any]]) -> Optional[str]:
    for cookie in cookies:
        if cookie.get("name") == "Session-Id":
            return cookie.get("value")
    return None
def _has_auth_cookie(cookies: List[Dict[str, Any]]) -> bool:
    return any(str(c.get("name", "")).startswith(AUTH_COOKIE_PREFIX) for c in cookies)
def _select_browser() -> str:
    if _find_local_driver("edge"):
        return "edge"
    if _find_local_driver("chrome"):
        return "chrome"
    return "edge"
def harvest_one(browser: str, wait_timeout: float = 30.0, settle_seconds: float = 1.5) -> Credential:
    driver = _make_driver(browser)
    try:
        driver.set_page_load_timeout(wait_timeout)
        try:
            driver.get(GUEST_URL)
        except Exception:
            pass
        deadline = time.monotonic() + wait_timeout
        cookies: List[Dict[str, Any]] = []
        while time.monotonic() < deadline:
            cookies = _get_all_cookies(driver)
            if _has_auth_cookie(cookies):
                break
            time.sleep(0.5)
        time.sleep(settle_seconds)
        cookies = _get_all_cookies(driver)
        return Credential(id=str(uuid.uuid4()), cookie_header=_build_cookie_header(cookies), session_id=_extract_session_id(cookies), user_agent=DEFAULT_USER_AGENT)
    finally:
        driver.quit()
def harvest(count: int, output: Path = DEFAULT_OUTPUT, append: bool = True, on_event=None) -> List[Credential]:
    try:
        import selenium
    except ImportError as exc:
        raise SystemExit("Selenium is required. Install it with: pip install selenium") from exc
    browser = _select_browser()
    store = CredentialFile(output)
    existing = store.load() if append else []
    harvested: List[Credential] = []
    for index in range(1, count + 1):
        try:
            cred = harvest_one(browser)
        except Exception as exc:
            if on_event:
                on_event("error", index, count, str(exc))
            continue
        if not cred.is_valid():
            if on_event:
                on_event("skip", index, count, None)
            continue
        harvested.append(cred)
        store.save(existing + harvested)
        if on_event:
            on_event("ok", index, count, cred.auth_cookie)
    return harvested
def _print_event(kind: str, index: int, count: int, detail) -> None:
    if kind == "ok":
        print(f"[{index}/{count}] captured {detail}")
    elif kind == "skip":
        print(f"[{index}/{count}] no auth cookie captured, skipping")
    else:
        print(f"[{index}/{count}] failed: {detail}")
def main() -> int:
    harvest(MAX_CREDENTIALS, on_event=_print_event)
    return 0
if __name__ == "__main__":
    raise SystemExit(main())
