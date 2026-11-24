from datetime import datetime, timezone
import json
from pathlib import Path
import urllib.parse
import urllib.request


IBGE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
BCB_BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados"
DATA_INICIAL = "01/01/2024"
DATA_FINAL = "31/10/2026"


def _download_json(url: str, output_path: Path) -> dict:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (DistribuidoraVeredasCase/1.0)"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw_bytes = resp.read()
        status_code = resp.status

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(raw_bytes)

    parsed = json.loads(raw_bytes.decode("utf-8"))
    record_count = len(parsed) if isinstance(parsed, list) else 1

    return {
        "source": output_path.stem,
        "url": url,
        "status_code": status_code,
        "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        "file_path": str(output_path),
        "size_bytes": len(raw_bytes),
        "records": record_count,
    }


def run_fetch() -> None:
    project_root = Path(__file__).resolve().parents[2]
    raw_dir = project_root / "data" / "raw"

    bcb_params = urllib.parse.urlencode(
        {"formato": "json", "dataInicial": DATA_INICIAL, "dataFinal": DATA_FINAL}
    )
    bcb_url = f"{BCB_BASE_URL}?{bcb_params}"

    manifest_entries = [
        _download_json(IBGE_URL, raw_dir / "ibge_municipios.json"),
        _download_json(bcb_url, raw_dir / "bcb_ipca_433_202401_202610.json"),
    ]

    meta_path = raw_dir / "fetch_ibge_bcb_metadata.json"
    meta_path.write_text(
        json.dumps(manifest_entries, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    for item in manifest_entries:
        print(
            f"[{item['source']}] HTTP {item['status_code']} | "
            f"registros={item['records']} | bytes={item['size_bytes']} -> {item['file_path']}"
        )
    print(f"Metadados salvos em: {meta_path}")


if __name__ == "__main__":
    run_fetch()