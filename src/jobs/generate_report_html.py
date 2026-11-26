"""
JOB: generate_report_html
CAMADA: Relatorio (HTML)
LE:
- data/gold/gold_precos_reembolso
- data/gold/gold_resumo_uf_mensal
- data/silver/silver_precos_combustiveis (fallback de contagem de coletas)
ATUALIZA:
- reports/relatorio_distribuidora_veredas.html (preservando estrutura e secoes)
"""

from datetime import datetime, timezone
from pathlib import Path
import re

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DETALHE_DIR = PROJECT_ROOT / "data" / "gold" / "gold_precos_reembolso"
GOLD_RESUMO_DIR = PROJECT_ROOT / "data" / "gold" / "gold_resumo_uf_mensal"
SILVER_PRECOS_DIR = PROJECT_ROOT / "data" / "silver" / "silver_precos_combustiveis"
REPORT_HTML_PATH = PROJECT_ROOT / "reports" / "relatorio_distribuidora_veredas.html"


def _fmt_int_pt(val: int) -> str:
    return f"{int(val):,}".replace(",", ".")


def _fmt_pct_pt(val: float) -> str:
    return f"{val:.2f}".replace(".", ",") + "%"


def _read_parquet_tree(root: Path) -> pd.DataFrame:
    if not root.exists():
        return pd.DataFrame()
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def _compute_metrics() -> dict[str, str]:
    detalhe_df = _read_parquet_tree(GOLD_DETALHE_DIR)
    resumo_df = _read_parquet_tree(GOLD_RESUMO_DIR)
    silver_df = _read_parquet_tree(SILVER_PRECOS_DIR)

    total_coletas = 0
    for col in ["qtd_coletas", "total_coletas", "n_coletas", "qtd_registros"]:
        if not resumo_df.empty and col in resumo_df.columns:
            total_coletas = int(pd.to_numeric(resumo_df[col], errors="coerce").fillna(0).sum())
            break
    if total_coletas == 0 and not silver_df.empty:
        total_coletas = len(silver_df)

    meses_cobertos = 0
    for df_cand in [resumo_df, detalhe_df, silver_df]:
        if not df_cand.empty:
            for col in ["ano_mes", "mes_ano", "referencia_mes"]:
                if col in df_cand.columns:
                    meses_cobertos = int(df_cand[col].dropna().nunique())
                    break
        if meses_cobertos > 0:
            break

    cobertura_ibge = 0.0
    if not resumo_df.empty:
        for col in ["cobertura_ibge_pct", "pct_cobertura_ibge", "cobertura_ibge"]:
            if col in resumo_df.columns:
                vals = pd.to_numeric(resumo_df[col], errors="coerce").dropna()
                if not vals.empty:
                    mean_val = float(vals.mean())
                    cobertura_ibge = mean_val * 100.0 if mean_val <= 1.0 else mean_val
                    break

    acima_teto = 0
    if not resumo_df.empty and "flag_excede_teto" in resumo_df.columns:
        acima_teto = int(resumo_df["flag_excede_teto"].fillna(False).astype(bool).sum())
    elif not detalhe_df.empty and "flag_excede_teto" in detalhe_df.columns:
        acima_teto = int(detalhe_df["flag_excede_teto"].fillna(False).astype(bool).sum())

    mart_detalhe = len(detalhe_df)
    gerado_em = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    return {
        "gerado_em": gerado_em,
        "total_coletas": _fmt_int_pt(total_coletas),
        "meses_cobertos": _fmt_int_pt(meses_cobertos),
        "cobertura_ibge": _fmt_pct_pt(cobertura_ibge),
        "acima_teto": _fmt_int_pt(acima_teto),
        "mart_detalhe": _fmt_int_pt(mart_detalhe),
    }


def _replace_kpi_value(html: str, label_pattern: str, new_value: str) -> str:
    pattern = re.compile(
        rf'(<div class="label">\s*{label_pattern}\s*</div>\s*<div class="value">)([^<]*)(</div>)',
        flags=re.IGNORECASE | re.DOTALL,
    )
    return pattern.sub(rf"\g<1>{new_value}\g<3>", html, count=1)


def update_report(html_path: Path = REPORT_HTML_PATH) -> Path:
    if not html_path.exists():
        raise FileNotFoundError(f"Relatorio HTML base nao encontrado em: {html_path}")

    metrics = _compute_metrics()
    html = html_path.read_text(encoding="utf-8")

    html = re.sub(
        r"(Gerado em:\s*)([^<]+)(</p>)",
        rf"\g<1>{metrics['gerado_em']}\g<3>",
        html,
        count=1,
    )
    html = _replace_kpi_value(html, "Total de Coletas", metrics["total_coletas"])
    html = _replace_kpi_value(html, "Meses Cobertos", metrics["meses_cobertos"])
    html = _replace_kpi_value(html, "Cobertura Media IBGE", metrics["cobertura_ibge"])
    html = _replace_kpi_value(html, "Linhas Acima do Teto", metrics["acima_teto"])
    html = _replace_kpi_value(html, "Linhas Mart Detalhe", metrics["mart_detalhe"])

    html_path.write_text(html, encoding="utf-8")
    print(
        f"[OK] Relatorio HTML atualizado em: {html_path} "
        f"(coletas={metrics['total_coletas']}, meses={metrics['meses_cobertos']}, "
        f"acima_teto={metrics['acima_teto']}, mart={metrics['mart_detalhe']})"
    )
    return html_path


def run_job() -> None:
    update_report(REPORT_HTML_PATH)


if __name__ == "__main__":
    run_job()
