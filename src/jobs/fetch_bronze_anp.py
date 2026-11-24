from datetime import datetime, timezone
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import urllib.parse
import urllib.request


ANP_PAGE_URL = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
    "serie-historica-de-precos-de-combustiveis"
)
TARGET_START_YM = (2024, 1)
TARGET_END_YM = (2026, 10)

PT_MONTHS = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "março": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}


class _LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._current_href: str | None = None
        self._text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            href = dict(attrs).get("href")
            if href:
                self._current_href = href
                self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._current_href is not None:
            label = " ".join("".join(self._text_parts).split())
            self.links.append((self._current_href, label))
            self._current_href = None
            self._text_parts = []


def _fetch_text(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) DistribuidoraVeredasCase/1.0"
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read()
    return raw.decode("utf-8", errors="replace")


def _extract_year_month(full_url: str, label: str) -> tuple[int, int] | None:
    url_lower = full_url.lower()
    label_lower = label.lower()

    # Exclui diesel-gnv e glp
    if "diesel" in url_lower or "gnv" in url_lower or "/glp/" in url_lower:
        return None
    if "gasolina" not in url_lower and "etanol" not in url_lower:
        return None

    year_match = re.search(r"/dsan/(202[4-6])/", url_lower) or re.search(
        r"\b(202[4-6])\b", f"{label_lower} {url_lower}"
    )
    if not year_match:
        return None
    year = int(year_match.group(1))

    # Tenta pelo nome do mês no texto do link ("Janeiro de 2024", etc.)
    for month_name, month_num in PT_MONTHS.items():
        if re.search(rf"\b{month_name}\b", label_lower):
            return (year, month_num)

    # Fallback: número do mês no nome do arquivo (ex.: -01.csv ou 01-dados...)
    path_name = Path(urllib.parse.urlparse(full_url).path).name.lower()
    m_end = re.search(r"-(0[1-9]|1[0-2])(?:\.[a-z0-9]+)?$", path_name)
    if m_end:
        return (year, int(m_end.group(1)))

    m_start = re.search(r"^(0[1-9]|1[0-2])-", path_name)
    if m_start:
        return (year, int(m_start.group(1)))

    return None


def _in_target_period(ym: tuple[int, int] | None) -> bool:
    if ym is None:
        return False
    return TARGET_START_YM <= ym <= TARGET_END_YM


def _download_file(url: str, dest_path: Path) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) DistribuidoraVeredasCase/1.0"
        },
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()
        status_code = resp.status

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    dest_path.write_bytes(data)

    return {
        "url": url,
        "status_code": status_code,
        "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        "file_path": str(dest_path),
        "size_bytes": len(data),
    }


def run_fetch_anp() -> None:
    project_root = Path(__file__).resolve().parents[2]
    anp_raw_dir = project_root / "data" / "raw" / "anp"
    anp_raw_dir.mkdir(parents=True, exist_ok=True)

    html = _fetch_text(ANP_PAGE_URL)
    parser = _LinkExtractor()
    parser.feed(html)

    matched_by_ym: dict[tuple[int, int], tuple[str, str]] = {}
    for href, label in parser.links:
        full_url = urllib.parse.urljoin(ANP_PAGE_URL, href)
        ym = _extract_year_month(full_url, label)
        if _in_target_period(ym):
            matched_by_ym[ym] = (full_url, label)

    metadata = []
    for ym in sorted(matched_by_ym.keys()):
        full_url, label = matched_by_ym[ym]
        raw_name = Path(urllib.parse.urlparse(full_url).path).name or f"anp_{ym[0]}_{ym[1]:02d}"
        if not Path(raw_name).suffix:
            raw_name = f"{raw_name}.csv"
        filename = f"{ym[0]:04d}-{ym[1]:02d}_{raw_name}"
        dest = anp_raw_dir / filename
        info = _download_file(full_url, dest)
        info["label"] = label
        info["year_month"] = f"{ym[0]:04d}-{ym[1]:02d}"
        metadata.append(info)
        print(f"[ANP {info['year_month']}] HTTP {info['status_code']} -> {filename} ({info['size_bytes']} bytes)")

    meta_path = anp_raw_dir / "fetch_anp_metadata.json"
    meta_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Total baixado: {len(metadata)} arquivo(s) de 34 meses esperados. Metadados em: {meta_path}")


if __name__ == "__main__":
    run_fetch_anp()