"""
JOB: transform_silver_precos_combustiveis
CAMADA: Prata (Silver)
LE:
  - data/raw/anp ou data/bronze (arquivos CSV/Parquet/JSON de precos ANP)
  - data/raw/ibge_municipios.json (opcional, para preencher codigo_ibge_municipio por UF+Municipio quando ausente)
GRAVA:
  - data/silver/silver_precos_combustiveis (particionado por `ano_mes`)
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import unicodedata

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_ANP_DIR = PROJECT_ROOT / "data" / "raw" / "anp"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
RAW_IBGE_FILE = PROJECT_ROOT / "data" / "raw" / "ibge_municipios.json"
SILVER_PRECOS_DIR = PROJECT_ROOT / "data" / "silver" / "silver_precos_combustiveis"


def _norm_str(val: object) -> str:
    if val is None or pd.isna(val):
        return ""
    text = str(val).strip().upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.split())


def _is_manifest_or_metadata(path: Path) -> bool:
    name_l = path.name.lower()
    if any(k in name_l for k in ["manifest", "metadata", "index", "status"]):
        return True
    if path.suffix.lower() == ".json":
        try:
            with path.open("r", encoding="utf-8") as f:
                head = json.load(f)
            sample = head[0] if isinstance(head, list) and head else (head if isinstance(head, dict) else {})
            if isinstance(sample, dict) and {"file_path", "url"}.issubset(set(sample.keys())):
                return True
        except Exception:
            pass
    return False


def _find_precos_files() -> list[Path]:
    search_roots = [d for d in [RAW_ANP_DIR, BRONZE_DIR] if d.exists()]
    files: list[Path] = []
    for root in search_roots:
        for p in sorted(root.rglob("*")):
            if not p.is_file() or p.suffix.lower() not in {".csv", ".parquet", ".json"}:
                continue
            if root == BRONZE_DIR and "reembolso" in str(p).lower():
                continue
            if _is_manifest_or_metadata(p):
                continue
            files.append(p)
    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo de precos ANP encontrado em {RAW_ANP_DIR} ou {BRONZE_DIR}."
        )
    return files


def _read_single_file(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".csv":
        for sep in [";", ",", "\t"]:
            try:
                df = pd.read_csv(path, sep=sep, dtype=str, low_memory=False)
                if len(df.columns) > 1:
                    return df
            except Exception:
                continue
        return pd.read_csv(path, sep=None, engine="python", dtype=str)
    if ext == ".parquet":
        return pd.read_parquet(path)
    return pd.read_json(path, dtype=str)


def _pick_col(cols: set[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in cols:
            return c
    return None


def _load_ibge_lookup() -> dict[tuple[str, str], int]:
    if not RAW_IBGE_FILE.exists():
        return {}
    with RAW_IBGE_FILE.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    items = payload if isinstance(payload, list) else payload.get("dados", [])
    lookup: dict[tuple[str, str], int] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        cod = item.get("id") or item.get("codigo_ibge")
        nome_mun = item.get("nome") or item.get("nome_municipio")
        micro = item.get("microrregiao") or {}
        meso = micro.get("mesorregiao") or {}
        uf_obj = meso.get("UF") or item.get("UF") or {}
        uf = uf_obj.get("sigla") if isinstance(uf_obj, dict) else item.get("sigla_uf")
        if cod is not None and nome_mun and uf:
            try:
                lookup[(_norm_str(uf), _norm_str(nome_mun))] = int(cod)
            except (TypeError, ValueError):
                continue
    return lookup


def _normalize_precos_chunk(
    raw_df: pd.DataFrame, ingest_ts: datetime, ibge_lookup: dict[tuple[str, str], int]
) -> pd.DataFrame:
    df = raw_df.copy()
    df.columns = [
        str(c).strip().lower().replace(" ", "_").replace("-", "_")
        for c in df.columns
    ]
    cols = set(df.columns)

    uf_col = _pick_col(cols, ["estado___sigla", "estado__sigla", "estado_sigla", "uf", "sigla_uf", "estado"])
    data_col = _pick_col(cols, ["data_da_coleta", "data_coleta", "dt_coleta", "data"])
    prod_col = _pick_col(cols, ["produto", "combustivel", "tipo_combustivel"])
    band_col = _pick_col(cols, ["bandeira", "nome_bandeira", "bandeira_posto"])
    cnpj_col = _pick_col(cols, ["cnpj_da_revenda", "cnpj_revenda", "cnpj", "revenda_cnpj"])
    mun_col = _pick_col(cols, ["municipio", "nome_municipio", "cidade"])
    ibge_col = _pick_col(
        cols,
        ["codigo_ibge_municipio", "codigo_ibge", "cod_ibge", "id_municipio", "municipio_ibge"],
    )
    venda_col = _pick_col(
        cols,
        ["valor_de_venda", "valor_venda", "preco_venda", "vl_venda", "preco_revenda"],
    )
    compra_col = _pick_col(
        cols,
        ["valor_de_compra", "valor_compra", "preco_compra", "vl_compra"],
    )

    if not uf_col or not data_col or not prod_col or not venda_col:
        raise ValueError(
            f"Colunas obrigatorias ausentes nos precos ANP. Disponiveis: {sorted(cols)}"
        )

    raw_dt = df[data_col].astype(str).str.strip()
    dt = pd.to_datetime(raw_dt, format="%d/%m/%Y", errors="coerce")
    mask_iso = dt.isna()
    if mask_iso.any():
        dt.loc[mask_iso] = pd.to_datetime(raw_dt.loc[mask_iso], errors="coerce")

    def _to_float(series: pd.Series) -> pd.Series:
        cleaned = (
            series.astype(str)
            .str.strip()
            .str.replace("R$", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        return pd.to_numeric(cleaned, errors="coerce")

    valor_venda = _to_float(df[venda_col])
    valor_compra = _to_float(df[compra_col]) if compra_col else pd.Series(pd.NA, index=df.index)

    uf_clean = df[uf_col].astype(str).str.strip().str.upper()

    if ibge_col:
        cod_ibge = pd.to_numeric(df[ibge_col], errors="coerce").astype("Int64")
    elif mun_col and ibge_lookup:
        cod_ibge = pd.Series(
            [
                ibge_lookup.get((_norm_str(u), _norm_str(m)), pd.NA)
                for u, m in zip(uf_clean, df[mun_col])
            ],
            index=df.index,
            dtype="Int64",
        )
    else:
        cod_ibge = pd.Series(pd.NA, index=df.index, dtype="Int64")

    out = pd.DataFrame(
        {
            "ano_mes": dt.dt.strftime("%Y-%m"),
            "uf": uf_clean,
            "data_coleta": dt.dt.strftime("%Y-%m-%d"),
            "produto": df[prod_col].astype(str).str.strip().str.upper(),
            "bandeira": (
                df[band_col].astype(str).str.strip().str.upper()
                if band_col
                else pd.Series("NAO INFORMADA", index=df.index)
            ),
            "cnpj_revenda": (
                df[cnpj_col].astype(str).str.strip()
                if cnpj_col
                else pd.Series(pd.NA, index=df.index)
            ),
            "codigo_ibge_municipio": cod_ibge,
            "valor_venda": valor_venda,
            "valor_compra": valor_compra,
        }
    )

    out = out[
        out["ano_mes"].notna()
        & (out["uf"].str.len() == 2)
        & out["produto"].notna()
        & (out["produto"] != "")
        & out["valor_venda"].notna()
        & (out["valor_venda"] > 0)
    ].copy()

    out["data_ingestao_utc"] = ingest_ts
    return out


def _write_idempotent_by_ano_mes(df: pd.DataFrame, output_dir: Path) -> tuple[int, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    parts_written = 0
    total_rows = 0

    for ym_val, group_df in df.groupby("ano_mes", sort=True):
        ym_clean = str(ym_val).strip()
        part_path = output_dir / f"ano_mes={ym_clean}"
        if part_path.exists():
            shutil.rmtree(part_path)
        part_path.mkdir(parents=True, exist_ok=True)

        out_df = group_df.drop(columns=["ano_mes"]).reset_index(drop=True)
        out_df.to_parquet(part_path / "part-00000.parquet", index=False)
        parts_written += 1
        total_rows += len(out_df)

    return parts_written, total_rows


def run_job() -> None:
    ingest_ts = datetime.now(timezone.utc)
    files = _find_precos_files()
    ibge_lookup = _load_ibge_lookup()

    frames: list[pd.DataFrame] = []
    for f in files:
        raw_df = _read_single_file(f)
        norm_df = _normalize_precos_chunk(raw_df, ingest_ts, ibge_lookup)
        if not norm_df.empty:
            frames.append(norm_df)

    if not frames:
        raise ValueError("Nenhuma linha valida de precos ANP apos normalizacao.")

    full_df = pd.concat(frames, ignore_index=True)
    parts_written, total_rows = _write_idempotent_by_ano_mes(full_df, SILVER_PRECOS_DIR)
    print(f"  [SILVER PRECOS] particoes(ano_mes)={parts_written} | linhas={total_rows}")
    print("[OK] Silver Precos concluido com sucesso (pandas + pyarrow).")


if __name__ == "__main__":
    run_job()