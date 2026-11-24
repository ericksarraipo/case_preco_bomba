from html.parser import HTMLParser
import re
import urllib.parse
import urllib.request


ANP_PAGE_URL = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
    "serie-historica-de-precos-de-combustiveis"
)


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            href = dict(attrs).get("href")
            if href:
                self._href = href
                self._text = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._href is not None:
            label = " ".join("".join(self._text).split())
            self.links.append((self._href, label))
            self._href = None
            self._text = []


def main() -> None:
    req = urllib.request.Request(
        ANP_PAGE_URL,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        html = resp.read().decode("utf-8", errors="replace")

    parser = LinkParser()
    parser.feed(html)

    pattern = re.compile(r"(2024|2025|2026|gasolina|etanol|precos|csv|xlsx|xls|zip)", re.I)
    seen: set[str] = set()
    for href, label in parser.links:
        full_url = urllib.parse.urljoin(ANP_PAGE_URL, href)
        combined = f"{full_url} {label}"
        if full_url not in seen and pattern.search(combined):
            seen.add(full_url)
            print(f"{full_url}  -->  {label}")


if __name__ == "__main__":
    main()
