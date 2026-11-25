"""
JOB: transform_gold_precos_reembolso
CAMADA: Ouro (Gold)
LE:
  - data/silver/silver_precos_combustiveis
  - data/silver/silver_municipios_ibge
  - data/silver/silver_reembolso_uf_vigencia
  - data/silver/silver_ipca_mensal
GRAVA:
  - data/gold/gold_precos_reembolso (particionado por `ano_mes`)
  - data/gold/gold_resumo_uf_mensal (particionado por `ano_mes`)
REGRAS APLICADAS:
  1. Enriquece precos da ANP com atributos geograficos do IBGE (regiao, UF, municipio) quando houver `codigo_ibge`.
  2. Associa a vigencia de reembolso por `uf` e `data_coleta` (`vigencia_inicio` <= `data_coleta` <= `vigencia_fim`),
     preservando `flag_anomalia_vigencia`, `vigencia_fim_original_nula` e `detalhe_anomalia` sem imputacao.
  3. Associa o indice acumulado do IPCA (`silver_ipca_mensal`) por `ano_mes` e calcula preco real deflacionado.
  4. Grava particoes `ano_mes=YYYY-MM` de forma idempotente no Windows.
"""

from datetime import datetime, timezone
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_PRECOS_DIR = PROJECT_ROOT / "data" / "silver" / "silver_precos_combustiveis"
SILVER_IBGE_DIR = PROJECT_ROOT / "data" / "silver" / "silver_municipios_ibge"
SILVER_REEMBOLSO_DIR = PROJECT_ROOT / "data" / "silver" / "silver_reembolso_uf_vigencia"
SILVER_IPCA_DIR = PROJECT_ROOT / "data" / "silver" / "silver_ipca_mensal"

GOLD_DETALHE_DIR = PROJECT_ROOT / "data" / "gold" / "gold_precos_reembolso"
GOLD_RESUMO_DIR = PROJECT_ROOT / "data" / "gold" / "gold_resumo_uf_mensal"


def _read_partitioned_parquet(base_dir: Path, partition_col: str) -> pd.DataFrame:
    if not base_dir.exists():
        raise FileNotFoundError(f"Diretorio nao encontrado: {base_dir}")

    frames: list[pd.DataFrame] = []
    for part_dir in sorted(base_dir.glob(f"{partition_col}=*")):
        if not part_dir.is_dir():
            continue
        part_val = part_dir.name.split("=", 1)[1].strip()
        for pq_file in sorted(part_dir.glob("*.parquet")):
            df = pd.read_parquet(pq_file)
            df[partition_col] = part_val
            frames.append(df)

    if not frames:
        raise ValueError(f"Nenhum arquivo parquet encontrado em: {base_dir}")

    return pd.concat(frames, ignore_index=True)


def _write_idempotent_by_ano_mes(df: pd.DataFrame, output_dir: Path, label: str) -> tuple[int, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    if df.empty:
        return 0, 0

    partitions_written = 0
    total_rows = 0

    for ym_val, group_df in df.groupby("ano_mes", sort=True):
        ym_clean = str(ym_val).strip()
        part_path = output_dir / f"ano_mes={ym_clean}"
        if part_path.exists():
            shutil.rmtree(part_path)

        part_path.mkdir(parents=True, exist_ok=True)
        out_df = group_df.drop(columns=["ano_mes"]).reset_index(drop=True)
        out_df.to_parquet(part_path / "part-00000.parquet", index=False)
        partitions_written += 1
        total_rows += len(out_df)

    print(f"  [GOLD {label}] particoes={partitions_written} | linhas={total_rows}")
    return partitions_written, total_rows

def _match_reembolso_vigente(precos_df: pd.DataFrame, reemb_df: pd.DataFrame) -> pd.DataFrame:
    """
    Associa cada coleta de preco a vigencia de reembolso da UF vigente na `data_coleta`.
    Quando houver mais de uma vigencia compativel na mesma data, prioriza a de maior `vigencia_inicio`
    e `versao` sem mascarar anomalias (`flag_anomalia_vigencia` e propagada).
    Processa UF a UF para evitar explosao de memoria em volumes grandes.
    """
    reemb = reemb_df.loc[:, ~reemb_df.columns.duplicated()].copy()
    reemb["uf"] = reemb["uf"].astype(str).str.strip().str.upper()
    reemb["vigencia_inicio_dt"] = pd.to_datetime(reemb["vigencia_inicio"], errors="coerce")
    reemb["vigencia_fim_dt"] = pd.to_datetime(reemb["vigencia_fim"], errors="coerce")
    reemb["teto_litro_reembolso"] = pd.to_numeric(reemb["teto_litro_reembolso"], errors="coerce")

    precos = precos_df.loc[:, ~precos_df.columns.duplicated()].copy()
    precos["uf"] = precos["uf"].astype(str).str.strip().str.upper()
    precos["_data_coleta_dt"] = pd.to_datetime(precos["data_coleta"], errors="coerce")
    precos["_row_id"] = np.arange(len(precos))

    cols_reemb = list(
        dict.fromkeys(
            [
                "versao",
                "teto_litro_reembolso",
                "vigencia_fim_original_nula",
                "flag_anomalia_vigencia",
                "detalhe_anomalia",
            ]
        )
    )
    cols_r_merge = list(
        dict.fromkeys(["vigencia_inicio_dt", "vigencia_fim_dt"] + cols_reemb)
    )

    reemb_by_uf = {
        uf_val: grp.reset_index(drop=True)
        for uf_val, grp in reemb.groupby("uf", sort=False)
    }

    matched_parts: list[pd.DataFrame] = []
    for uf_val, p_grp in precos.groupby("uf", sort=False):
        r_grp = reemb_by_uf.get(uf_val)
        if r_grp is None or r_grp.empty:
            continue

        merged_uf = p_grp[["_row_id", "_data_coleta_dt"]].merge(
            r_grp[cols_r_merge],
            how="cross",
        )

        in_window = (
            merged_uf["_data_coleta_dt"].notna()
            & merged_uf["vigencia_inicio_dt"].notna()
            & (merged_uf["_data_coleta_dt"] >= merged_uf["vigencia_inicio_dt"])
            & (
                merged_uf["vigencia_fim_dt"].isna()
                | (merged_uf["_data_coleta_dt"] <= merged_uf["vigencia_fim_dt"])
            )
        )
        if not in_window.any():
            continue

        cols_select = list(
            dict.fromkeys(["_row_id", "vigencia_inicio_dt", "versao"] + cols_reemb)
        )
        best_uf = (
            merged_uf.loc[in_window, cols_select]
            .sort_values(
                by=["_row_id", "vigencia_inicio_dt", "versao"],
                ascending=[True, False, False],
                na_position="last",
            )
            .drop_duplicates(subset=["_row_id"], keep="first")
        )
        matched_parts.append(best_uf[["_row_id"] + cols_reemb])

    if matched_parts:
        matched_all = pd.concat(matched_parts, ignore_index=True)
        out = precos.merge(matched_all, on="_row_id", how="left")
    else:
        out = precos.copy()
        for col in cols_reemb:
            out[col] = np.nan

    out["flag_anomalia_vigencia"] = (
        out["flag_anomalia_vigencia"]
        .map(lambda v: bool(v) if pd.notna(v) else False)
        .astype(bool)
    )
    out["vigencia_fim_original_nula"] = (
        out["vigencia_fim_original_nula"]
        .map(lambda v: bool(v) if pd.notna(v) else False)
        .astype(bool)
    )
    return out.drop(columns=["_data_coleta_dt", "_row_id"])

def run_job() -> None:
    ingest_ts = datetime.now(timezone.utc)

    precos = _read_partitioned_parquet(SILVER_PRECOS_DIR, "ano_mes")
    ibge = _read_partitioned_parquet(SILVER_IBGE_DIR, "uf")
    reembolso = _read_partitioned_parquet(SILVER_REEMBOLSO_DIR, "uf")
    ipca = _read_partitioned_parquet(SILVER_IPCA_DIR, "ano")

    precos["valor_venda_num"] = pd.to_numeric(precos["valor_venda"], errors="coerce")
    precos["valor_compra_num"] = pd.to_numeric(precos["valor_compra"], errors="coerce")
    precos["codigo_ibge_num"] = pd.to_numeric(precos["codigo_ibge_municipio"], errors="coerce")

    ibge_dim = (
        ibge[
            [
                "codigo_ibge",
                "nome_municipio",
                "sigla_uf",
                "nome_uf",
                "sigla_regiao",
                "nome_regiao",
            ]
        ]
        .dropna(subset=["codigo_ibge"])
        .copy()
    )
    ibge_dim["codigo_ibge_num"] = pd.to_numeric(ibge_dim["codigo_ibge"], errors="coerce")
    ibge_dim = ibge_dim.drop_duplicates(subset=["codigo_ibge_num"]).drop(columns=["codigo_ibge"])

    precos_ibge = precos.merge(ibge_dim, on="codigo_ibge_num", how="left")
    precos_enriq = _match_reembolso_vigente(precos_ibge, reembolso)

    ipca_clean = ipca[["ano_mes", "variacao_mensal_pct", "indice_acumulado_base", "mes_base_ipca"]].copy()
    ipca_clean["ano_mes"] = ipca_clean["ano_mes"].astype(str).str.strip()
    ipca_clean["indice_acumulado_base"] = pd.to_numeric(
        ipca_clean["indice_acumulado_base"], errors="coerce"
    )
    ipca_clean["variacao_mensal_pct"] = pd.to_numeric(
        ipca_clean["variacao_mensal_pct"], errors="coerce"
    )
    ipca_clean = ipca_clean.drop_duplicates(subset=["ano_mes"])

    uf_regiao_map = (
        ibge_dim[["sigla_uf", "sigla_regiao", "nome_regiao"]]
        .dropna(subset=["sigla_uf"])
        .drop_duplicates(subset=["sigla_uf"])
        .rename(columns={"sigla_uf": "uf"})
    )

    precos_enriq["ano_mes"] = precos_enriq["ano_mes"].astype(str).str.strip()
    precos_enriq["bandeira"] = precos_enriq["bandeira"].fillna("NAO INFORMADA").astype(str).str.strip()
    precos_enriq = precos_enriq.drop(columns=["sigla_regiao", "nome_regiao"], errors="ignore").merge(
        uf_regiao_map, on="uf", how="left"
    )
    precos_enriq["sigla_regiao"] = precos_enriq["sigla_regiao"].fillna("NAO_MAPEADA")
    precos_enriq["nome_regiao"] = precos_enriq["nome_regiao"].fillna("Nao mapeada")

    # 1. Mart detalhado mensal por ano_mes, uf, produto e bandeira
    grp_cols = ["ano_mes", "uf", "produto", "bandeira"]
    detalhe = (
        precos_enriq.groupby(grp_cols, dropna=False, as_index=False)
        .agg(
            sigla_regiao=("sigla_regiao", "first"),
            nome_regiao=("nome_regiao", "first"),
            qtd_coletas=("valor_venda_num", "count"),
            qtd_revendas=("cnpj_revenda", "nunique"),
            qtd_com_ibge=("codigo_ibge_num", lambda s: int(s.notna().sum())),
            preco_medio_venda=("valor_venda_num", "mean"),
            preco_mediano_venda=("valor_venda_num", "median"),
            preco_min_venda=("valor_venda_num", "min"),
            preco_max_venda=("valor_venda_num", "max"),
            preco_medio_compra=("valor_compra_num", "mean"),
            teto_litro_reembolso=("teto_litro_reembolso", "first"),
            versao_reembolso=("versao", "first"),
            flag_anomalia_vigencia=("flag_anomalia_vigencia", "max"),
            vigencia_fim_original_nula=("vigencia_fim_original_nula", "max"),
            detalhe_anomalia=(
                "detalhe_anomalia",
                lambda s: " | ".join(sorted({str(x) for x in s.dropna() if str(x).strip()})) or None,
            ),
        )
    )

    detalhe = detalhe.merge(ipca_clean, on="ano_mes", how="left")
    detalhe["pct_cobertura_ibge"] = np.where(
        detalhe["qtd_coletas"] > 0,
        (detalhe["qtd_com_ibge"] / detalhe["qtd_coletas"]) * 100.0,
        np.nan,
    )
    detalhe["delta_vs_teto"] = detalhe["preco_medio_venda"] - detalhe["teto_litro_reembolso"]
    detalhe["pct_vs_teto"] = np.where(
        detalhe["teto_litro_reembolso"].notna() & (detalhe["teto_litro_reembolso"] > 0),
        ((detalhe["preco_medio_venda"] - detalhe["teto_litro_reembolso"]) / detalhe["teto_litro_reembolso"]) * 100.0,
        np.nan,
    )
    detalhe["flag_excede_teto"] = np.where(
        detalhe["preco_medio_venda"].notna() & detalhe["teto_litro_reembolso"].notna(),
        detalhe["preco_medio_venda"] > detalhe["teto_litro_reembolso"],
        False,
    )
    detalhe["preco_real_deflacionado"] = np.where(
        detalhe["indice_acumulado_base"].notna() & (detalhe["indice_acumulado_base"] > 0),
        detalhe["preco_medio_venda"] / detalhe["indice_acumulado_base"],
        np.nan,
    )
    detalhe["data_processamento_utc"] = ingest_ts

    # 2. Mart executivo por ano_mes, uf e produto
    resumo = (
        precos_enriq.groupby(["ano_mes", "uf", "produto"], dropna=False, as_index=False)
        .agg(
            sigla_regiao=("sigla_regiao", "first"),
            nome_regiao=("nome_regiao", "first"),
            qtd_coletas=("valor_venda_num", "count"),
            qtd_revendas=("cnpj_revenda", "nunique"),
            qtd_bandeiras=("bandeira", "nunique"),
            qtd_com_ibge=("codigo_ibge_num", lambda s: int(s.notna().sum())),
            preco_medio_venda=("valor_venda_num", "mean"),
            preco_mediano_venda=("valor_venda_num", "median"),
            teto_litro_reembolso=("teto_litro_reembolso", "first"),
            versao_reembolso=("versao", "first"),
            flag_anomalia_vigencia=("flag_anomalia_vigencia", "max"),
            vigencia_fim_original_nula=("vigencia_fim_original_nula", "max"),
            detalhe_anomalia=(
                "detalhe_anomalia",
                lambda s: " | ".join(sorted({str(x) for x in s.dropna() if str(x).strip()})) or None,
            ),
        )
    )
    resumo = resumo.merge(ipca_clean, on="ano_mes", how="left")
    resumo["pct_cobertura_ibge"] = np.where(
        resumo["qtd_coletas"] > 0,
        (resumo["qtd_com_ibge"] / resumo["qtd_coletas"]) * 100.0,
        np.nan,
    )
    resumo["delta_vs_teto"] = resumo["preco_medio_venda"] - resumo["teto_litro_reembolso"]
    resumo["pct_vs_teto"] = np.where(
        resumo["teto_litro_reembolso"].notna() & (resumo["teto_litro_reembolso"] > 0),
        ((resumo["preco_medio_venda"] - resumo["teto_litro_reembolso"]) / resumo["teto_litro_reembolso"]) * 100.0,
        np.nan,
    )
    resumo["flag_excede_teto"] = np.where(
        resumo["preco_medio_venda"].notna() & resumo["teto_litro_reembolso"].notna(),
        resumo["preco_medio_venda"] > resumo["teto_litro_reembolso"],
        False,
    )
    resumo["preco_real_deflacionado"] = np.where(
        resumo["indice_acumulado_base"].notna() & (resumo["indice_acumulado_base"] > 0),
        resumo["preco_medio_venda"] / resumo["indice_acumulado_base"],
        np.nan,
    )
    resumo["data_processamento_utc"] = ingest_ts

    _write_idempotent_by_ano_mes(detalhe, GOLD_DETALHE_DIR, "gold_precos_reembolso")
    _write_idempotent_by_ano_mes(resumo, GOLD_RESUMO_DIR, "gold_resumo_uf_mensal")
    print("[OK] Camada Ouro concluida com sucesso.")


if __name__ == "__main__":
    run_job()