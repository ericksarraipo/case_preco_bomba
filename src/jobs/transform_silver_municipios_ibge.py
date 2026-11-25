"""
JOB: transform_silver_municipios_ibge
CAMADA: Prata (Silver)
LE:
  - data/bronze ou data/raw (ibge_municipios.json)
GRAVA:
  - data/silver/silver_municipios_ibge (particionado por `uf`)
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
SILVER_IBGE_DIR = PROJECT_ROOT / "data" / "silver" / "silver_municipios_ibge"


def _find_ibge_files() -> list[Path]:
    roots = [d for d in [BRONZE_DIR, RAW_DIR] if d.exists()]
    candidates: list[Path] = []
    for root in roots:
        for p in sorted(root.rglob("*")):
            if not p.is_file() or p.suffix.lower() not in {".json", ".csv", ".parquet"}:
                continue
            name_l = p.name.lower()
            if "metadata" in name_l:
                continue
            if "ibge" in name_l or "municipio" in name_l:
                candidates.append(p)
    if not candidates:
        raise FileNotFoundError(
            f"Nenhum arquivo de municipios IBGE encontrado em {BRONZE_DIR} ou {RAW_DIR}."
        )
    return candidates


def _flatten_ibge_records(payload: Any) -> list[dict[str, Any]]:
    items = payload if isinstance(payload, list) else payload.get("dados", payload.get("municipios", []))
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        microrregiao = item.get("microrregiao") or {}
        mesorregiao = microrregiao.get("mesorregiao") or {}
        uf_obj = mesorregiao.get("UF") or item.get("UF") or item.get("uf") or {}
        regiao_obj = uf_obj.get("regiao") or item.get("regiao") or {}

        codigo = item.get("id") or item.get("codigo_ibge") or item.get("codigo_municipio")
        nome_mun = item.get("nome") or item.get("nome_municipio")
        sigla_uf = (
            uf_obj.get("sigla")
            if isinstance(uf_obj, dict)
            else (uf_obj or item.get("sigla_uf") or item.get("uf"))
        )
        nome_uf = uf_obj.get("nome") if isinstance(uf_obj, dict) else item.get("nome_uf")
        sigla_reg = (
            regiao_obj.get("sigla")
            if isinstance(regiao_obj, dict)
            else item.get("sigla_regiao")
        )
        nome_reg = (
            regiao_obj.get("nome")
            if isinstance(regiao_obj, dict)
            else item.get("nome_regiao")
        )

        rows.append(
            {
                "codigo_ibge": codigo,
                "nome_municipio": nome_mun,
                "sigla_uf": sigla_uf,
                "nome_uf": nome_uf,
                "sigla_regiao": sigla_reg,
                "nome_regiao": nome_reg,
            }
        )
    return rows


def _load_ibge_df(files: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in files:
        if path.suffix.lower() == ".json":
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            df = pd.DataFrame(_flatten_ibge_records(payload))
        elif path.suffix.lower() == ".csv":
            df = pd.read_csv(path, sep=None, engine="python")
        else:
            df = pd.read_parquet(path)
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def _normalize_ibge(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    df["codigo_ibge"] = pd.to_numeric(df["codigo_ibge"], errors="coerce").astype("Int64")
    df["nome_municipio"] = df["nome_municipio"].astype(str).str.strip()
    df["sigla_uf"] = df["sigla_uf"].astype(str).str.strip().str.upper()
    df["nome_uf"] = df["nome_uf"].astype(str).str.strip()
    df["sigla_regiao"] = df["sigla_regiao"].astype(str).str.strip().str.upper()
    df["nome_regiao"] = df["nome_regiao"].astype(str).str.strip()
    df["uf"] = df["sigla_uf"]

    df = df.dropna(subset=["codigo_ibge", "uf"]).copy()
    df = df[df["uf"].str.len() == 2]
    df = df.drop_duplicates(subset=["codigo_ibge"]).reset_index(drop=True)
    df["data_ingestao_utc"] = datetime.now(timezone.utc)

    cols = [
        "codigo_ibge",
        "nome_municipio",
        "sigla_uf",
        "nome_uf",
        "sigla_regiao",
        "nome_regiao",
        "uf",
        "data_ingestao_utc",
    ]
    return df[cols]


def _write_idempotent_by_uf(df: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    parts_written = 0
    total_rows = 0

    for uf_val, group_df in df.groupby("uf", sort=True):
        uf_clean = str(uf_val).strip().upper()
        part_path = output_dir / f"uf={uf_clean}"
        if part_path.exists():
            shutil.rmtree(part_path)
        part_path.mkdir(parents=True, exist_ok=True)

        out_df = group_df.drop(columns=["uf"]).reset_index(drop=True)
        out_df.to_parquet(part_path / "part-00000.parquet", index=False)
        parts_written += 1
        total_rows += len(out_df)

    print(f"  [SILVER IBGE] particoes(uf)={parts_written} | linhas={total_rows}")


def run_job() -> None:
    files = _find_ibge_files()
    raw_df = _load_ibge_df(files)
    silver_df = _normalize_ibge(raw_df)
    _write_idempotent_by_uf(silver_df, SILVER_IBGE_DIR)
    print("[OK] Silver IBGE concluido com sucesso (pandas + pyarrow).")


if __name__ == "__main__":
    run_job()