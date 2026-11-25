"""
JOB: validate_gold
VALIDACAO DE QUALIDADE E REGRAS DE NEGOCIO - CAMADA OURO
"""

from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_PRECOS_DIR = PROJECT_ROOT / "data" / "silver" / "silver_precos_combustiveis"
GOLD_DETALHE_DIR = PROJECT_ROOT / "data" / "gold" / "gold_precos_reembolso"
GOLD_RESUMO_DIR = PROJECT_ROOT / "data" / "gold" / "gold_resumo_uf_mensal"


def _load_gold(base_dir: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for part_dir in sorted(base_dir.glob("ano_mes=*")):
        if not part_dir.is_dir():
            continue
        ym = part_dir.name.split("=", 1)[1].strip()
        for pq_file in sorted(part_dir.glob("*.parquet")):
            df = pd.read_parquet(pq_file)
            df["ano_mes"] = ym
            frames.append(df)
    if not frames:
        raise AssertionError(f"Nenhuma particao encontrada em {base_dir}")
    return pd.concat(frames, ignore_index=True)


def run_validation() -> None:
    silver_precos = _load_gold(SILVER_PRECOS_DIR)
    detalhe = _load_gold(GOLD_DETALHE_DIR)
    resumo = _load_gold(GOLD_RESUMO_DIR)

    # 1. Particoes esperadas (34 meses: 2024-01 a 2026-10)
    months_det = sorted(detalhe["ano_mes"].unique().tolist())
    months_res = sorted(resumo["ano_mes"].unique().tolist())
    assert len(months_det) == 34, f"Esperado 34 meses em gold_precos_reembolso, obtido {len(months_det)}"
    assert len(months_res) == 34, f"Esperado 34 meses em gold_resumo_uf_mensal, obtido {len(months_res)}"
    assert months_det == months_res, "Divergencia de meses entre marts Ouro"

    # 2. Conservacao de volume de coletas em relacao a Prata
    silver_count = len(silver_precos)
    gold_det_coletas = int(detalhe["qtd_coletas"].sum())
    gold_res_coletas = int(resumo["qtd_coletas"].sum())
    assert gold_det_coletas == silver_count, (
        f"Coletas divergentes (Prata={silver_count} vs gold_precos_reembolso={gold_det_coletas})"
    )
    assert gold_res_coletas == silver_count, (
        f"Coletas divergentes (Prata={silver_count} vs gold_resumo_uf_mensal={gold_res_coletas})"
    )

    # 3. Unicidade de grao
    dup_det = int(detalhe.duplicated(subset=["ano_mes", "uf", "produto", "bandeira"]).sum())
    dup_res = int(resumo.duplicated(subset=["ano_mes", "uf", "produto"]).sum())
    assert dup_det == 0, f"Duplicatas em gold_precos_reembolso: {dup_det}"
    assert dup_res == 0, f"Duplicatas em gold_resumo_uf_mensal: {dup_res}"

    # 4. Nulos criticos e precos validos
    for col in ["ano_mes", "uf", "produto", "preco_medio_venda", "qtd_coletas", "indice_acumulado_base"]:
        nulls = int(resumo[col].isna().sum())
        assert nulls == 0, f"Nulos inesperados em gold_resumo_uf_mensal.{col}: {nulls}"

    assert (resumo["preco_medio_venda"] > 0).all(), "Ha preco_medio_venda <= 0 em gold_resumo_uf_mensal"

    # 5. Auditoria de anomalias (BA e PR) e excesso de teto
    ufs_anomalia = sorted(resumo.loc[resumo["flag_anomalia_vigencia"] == True, "uf"].unique().tolist())
    qtd_anom_res = int(resumo["flag_anomalia_vigencia"].sum())
    qtd_excede_res = int(resumo["flag_excede_teto"].sum())
    cobertura_ibge_media = float(resumo["pct_cobertura_ibge"].mean())

    print("[OK] Todas as assercoes de qualidade da Camada Ouro passaram!")
    print(f"  - Periodo coberto: {months_res[0]} a {months_res[-1]} ({len(months_res)} meses)")
    print(f"  - Total de coletas auditadas: {silver_count:,}")
    print(f"  - Cobertura media codigo IBGE: {cobertura_ibge_media:.2f}%")
    print(f"  - Linhas com flag_anomalia_vigencia=True (resumo): {qtd_anom_res} | UFs: {ufs_anomalia}")
    print(f"  - Linhas com flag_excede_teto=True (resumo): {qtd_excede_res}")


if __name__ == "__main__":
    run_validation()