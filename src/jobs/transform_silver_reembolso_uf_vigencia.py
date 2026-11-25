"""
JOB: transform_silver_reembolso_uf_vigencia
CAMADA: Prata (Silver)
LE:
  - data/bronze ou data/raw (tabela_reembolso_uf)
GRAVA:
  - data/silver/silver_reembolso_uf_vigencia (particionado por `uf`)
"""

from datetime import datetime, timezone
from pathlib import Path
import shutil

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
SILVER_REEMBOLSO_DIR = PROJECT_ROOT / "data" / "silver" / "silver_reembolso_uf_vigencia"


def _find_reembolso_files() -> list[Path]:
    roots = [d for d in [BRONZE_DIR, RAW_DIR] if d.exists()]
    candidates: list[Path] = []
    for root in roots:
        for p in sorted(root.rglob("*")):
            if not p.is_file() or p.suffix.lower() not in {".parquet", ".csv", ".json"}:
                continue
            if "reembolso" in p.name.lower() or "reembolso" in str(p.parent).lower():
                candidates.append(p)
    if not candidates:
        raise FileNotFoundError(
            f"Nenhum arquivo de reembolso encontrado em {BRONZE_DIR} ou {RAW_DIR}."
        )
    return candidates


def _load_reembolso_df(files: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in files:
        if path.suffix.lower() == ".parquet":
            df = pd.read_parquet(path)
        elif path.suffix.lower() == ".csv":
            df = pd.read_csv(path, sep=None, engine="python")
        else:
            df = pd.read_json(path)
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def _normalize_reembolso(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    uf_col = next((c for c in ["uf", "sigla_uf", "estado"] if c in df.columns), None)
    teto_col = next(
        (c for c in ["teto_litro_reembolso", "valor_teto", "teto_reembolso", "teto", "valor"] if c in df.columns),
        None,
    )
    ini_col = next(
        (c for c in ["vigencia_inicio", "data_inicio", "inicio_vigencia", "dt_inicio"] if c in df.columns),
        None,
    )
    fim_col = next(
        (c for c in ["vigencia_fim", "data_fim", "fim_vigencia", "dt_fim"] if c in df.columns),
        None,
    )

    if not uf_col or not teto_col or not ini_col:
        raise ValueError(
            f"Colunas esperadas nao encontradas no reembolso. Disponiveis: {df.columns.tolist()}"
        )

    df["uf"] = df[uf_col].astype(str).str.strip().str.upper()
    df = df[df["uf"].str.len() == 2].copy()

    teto_str = (
        df[teto_col]
        .astype(str)
        .str.strip()
        .str.replace("R$", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df["teto_litro_reembolso"] = pd.to_numeric(teto_str, errors="coerce")

    ini_dt = pd.to_datetime(df[ini_col], errors="coerce")
    df["vigencia_inicio"] = ini_dt.dt.strftime("%Y-%m-%d")

    if fim_col:
        fim_raw = df[fim_col]
        fim_empty = fim_raw.isna() | fim_raw.astype(str).str.strip().str.lower().isin({"", "nan", "nat", "none", "null"})
        fim_dt = pd.to_datetime(fim_raw, errors="coerce")
        df["vigencia_fim_original_nula"] = (fim_empty | fim_dt.isna()).astype(bool)
        df["vigencia_fim"] = fim_dt.dt.strftime("%Y-%m-%d")
    else:
        df["vigencia_fim_original_nula"] = True
        df["vigencia_fim"] = pd.NA

    if "versao" in df.columns:
        df["versao"] = pd.to_numeric(df["versao"], errors="coerce").fillna(1).astype(int)
    else:
        df["versao"] = 1

    # Deteccao de anomalias sem imputacao de valores
    anom_parts: list[list[str]] = []
    for _, row in df.iterrows():
        motivos: list[str] = []
        if pd.isna(row["teto_litro_reembolso"]) or row["teto_litro_reembolso"] <= 0:
            motivos.append("teto_invalido_ou_nao_positivo")
        if bool(row["vigencia_fim_original_nula"]):
            motivos.append("vigencia_fim_nula")
        if pd.notna(row["vigencia_inicio"]) and pd.notna(row["vigencia_fim"]):
            if str(row["vigencia_fim"]) < str(row["vigencia_inicio"]):
                motivos.append("vigencia_fim_anterior_ao_inicio")
        anom_parts.append(motivos)

    df["detalhe_anomalia"] = ["; ".join(m) if m else None for m in anom_parts]
    df["flag_anomalia_vigencia"] = df["detalhe_anomalia"].notna().astype(bool)
    df["data_ingestao_utc"] = datetime.now(timezone.utc)

    cols = [
        "uf",
        "versao",
        "teto_litro_reembolso",
        "vigencia_inicio",
        "vigencia_fim",
        "vigencia_fim_original_nula",
        "flag_anomalia_vigencia",
        "detalhe_anomalia",
        "data_ingestao_utc",
    ]
    return df[cols].dropna(subset=["uf", "vigencia_inicio"]).reset_index(drop=True)


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

    print(f"  [SILVER REEMBOLSO] particoes(uf)={parts_written} | linhas={total_rows}")


def run_job() -> None:
    files = _find_reembolso_files()
    raw_df = _load_reembolso_df(files)
    silver_df = _normalize_reembolso(raw_df)
    _write_idempotent_by_uf(silver_df, SILVER_REEMBOLSO_DIR)
    print("[OK] Silver Reembolso concluido com sucesso (pandas + pyarrow).")


if __name__ == "__main__":
    run_job()