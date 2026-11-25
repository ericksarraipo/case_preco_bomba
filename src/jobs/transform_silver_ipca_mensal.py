"""
JOB: transform_silver_ipca_mensal
CAMADA: Prata (Silver)
LE:
  - data/bronze ou data/raw (arquivos JSON/CSV/Parquet referentes ao IPCA mensal - BCB SGS 433)
GRAVA:
  - data/silver/silver_ipca_mensal (particionado por `ano`)
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
SILVER_IPCA_DIR = PROJECT_ROOT / "data" / "silver" / "silver_ipca_mensal"


def _find_ipca_files(bronze_dir: Path) -> list[Path]:
    search_roots = [d for d in [bronze_dir, RAW_DIR] if d.exists()]
    if not search_roots:
        raise FileNotFoundError(
            f"Diretorios de origem nao encontrados: {bronze_dir} / {RAW_DIR}"
        )

    candidates: list[Path] = []
    for root in search_roots:
        for p in sorted(root.rglob("*")):
            if not p.is_file() or p.suffix.lower() not in {".json", ".csv", ".parquet"}:
                continue
            name_l = p.name.lower()
            if "metadata" in name_l:
                continue
            if "bcb_ipca" in name_l or "ipca" in name_l or "433" in name_l:
                candidates.append(p)

    if not candidates:
        raise FileNotFoundError(
            f"Nenhum arquivo de IPCA encontrado em {bronze_dir} ou {RAW_DIR}."
        )
    return candidates


def _load_raw_ipca(files: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in files:
        if path.suffix.lower() == ".json":
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            data = payload if isinstance(payload, list) else payload.get("dados", payload)
            df = pd.DataFrame(data)
        elif path.suffix.lower() == ".csv":
            df = pd.read_csv(path, sep=None, engine="python")
        else:
            df = pd.read_parquet(path)
        df["_arquivo_origem"] = path.name
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def _normalize_ipca(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    date_col = next(
        (c for c in ["data_referencia", "data", "dt_referencia", "ano_mes"] if c in df.columns),
        None,
    )
    val_col = next(
        (c for c in ["variacao_mensal_pct", "valor", "ipca", "variacao"] if c in df.columns),
        None,
    )
    if not date_col or not val_col:
        raise ValueError(
            f"Colunas esperadas de data e variacao nao encontradas no IPCA. "
            f"Colunas disponiveis: {df.columns.tolist()}"
        )

    raw_date = df[date_col].astype(str).str.strip()
    dt = pd.to_datetime(raw_date, format="%d/%m/%Y", errors="coerce")
    mask_iso = dt.isna()
    if mask_iso.any():
        dt.loc[mask_iso] = pd.to_datetime(raw_date.loc[mask_iso], errors="coerce")

    val_str = (
        df[val_col]
        .astype(str)
        .str.strip()
        .str.replace(",", ".", regex=False)
    )
    var_pct = pd.to_numeric(val_str, errors="coerce")

    out = pd.DataFrame(
        {
            "data_referencia": dt.dt.strftime("%Y-%m-%d"),
            "ano_mes": dt.dt.strftime("%Y-%m"),
            "ano": dt.dt.strftime("%Y"),
            "variacao_mensal_pct": var_pct,
        }
    )
    out = out.dropna(subset=["data_referencia", "ano_mes", "ano", "variacao_mensal_pct"]).copy()
    out = (
        out.sort_values(["ano_mes", "data_referencia"])
        .drop_duplicates(subset=["ano_mes"], keep="last")
        .reset_index(drop=True)
    )

    out["fator_mensal"] = 1.0 + (out["variacao_mensal_pct"] / 100.0)
    out["indice_acumulado_base"] = np.cumprod(out["fator_mensal"].to_numpy(dtype=float))
    out["mes_base_ipca"] = out["ano_mes"].iloc[0] if not out.empty else None
    out["data_ingestao_utc"] = datetime.now(timezone.utc)
    return out


def _write_idempotent_by_ano(df: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    parts_written = 0
    total_rows = 0

    for ano_val, group_df in df.groupby("ano", sort=True):
        ano_clean = str(ano_val).strip()
        part_path = output_dir / f"ano={ano_clean}"
        if part_path.exists():
            shutil.rmtree(part_path)
        part_path.mkdir(parents=True, exist_ok=True)

        out_df = group_df.drop(columns=["ano"]).reset_index(drop=True)
        out_df.to_parquet(part_path / "part-00000.parquet", index=False)
        parts_written += 1
        total_rows += len(out_df)

    print(f"  [SILVER IPCA] particoes(ano)={parts_written} | linhas={total_rows}")


def run_job() -> None:
    files = _find_ipca_files(BRONZE_DIR)
    raw_df = _load_raw_ipca(files)
    silver_df = _normalize_ipca(raw_df)
    _write_idempotent_by_ano(silver_df, SILVER_IPCA_DIR)
    print("[OK] Silver IPCA concluido com sucesso (pandas + pyarrow).")


if __name__ == "__main__":
    run_job()