"""
JOB: transform_silver_precos_combustiveis
CAMADA: Prata (Silver)
LE:
  - data/raw/anp/*.csv (34 arquivos mensais SHPC ANP, jan/2024 a out/2026)
  - data/silver/silver_municipios_ibge (dimensao de municipios para enriquecer codigo_ibge)
GRAVA:
  - data/silver/silver_precos_combustiveis (Parquet particionado por coluna `ano_mes`)
GRAO:
  - 1 linha por coleta de preco por revenda, produto e data (`cnpj_revenda`, `produto`, `data_coleta`)
REGRAS APLICADAS:
  1. Filtra o periodo (`PERIODO_INICIO` a `PERIODO_FIM`) e produtos configurados (`PRODUTOS_ALVO`).
  2. Normaliza colunas da ANP (separador `;`, encoding com fallback, datas `dd/MM/yyyy` e decimais com virgula).
  3. Enriquece `codigo_ibge_municipio` via cruzamento com `silver_municipios_ibge` (`uf` + nome normalizado), preservando `NULL` quando nao houver match exato.
  4. Deduplica pelo grao (`cnpj_revenda`, `produto`, `data_coleta`) para garantir idempotencia e grava por particao `ano_mes` (`YYYY-MM`).
"""

from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import sys
import unicodedata

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ==============================================================================
# PARAMETROS DO JOB (CENTRALIZADOS NO TOPO)
# ==============================================================================
PERIODO_INICIO = "2024-01"
PERIODO_FIM = "2026-10"
PRODUTOS_ALVO = ["GASOLINA COMUM", "ETANOL"]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_ANP_DIR = PROJECT_ROOT / "data" / "raw" / "anp"
INPUT_IBGE_SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "silver_municipios_ibge"
OUTPUT_SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "silver_precos_combustiveis"

SILVER_SCHEMA = StructType(
    [
        StructField("codigo_ibge_municipio", IntegerType(), nullable=True),
        StructField("uf", StringType(), nullable=False),
        StructField("municipio_anp", StringType(), nullable=False),
        StructField("bairro", StringType(), nullable=True),
        StructField("cep", StringType(), nullable=True),
        StructField("revenda", StringType(), nullable=True),
        StructField("cnpj_revenda", StringType(), nullable=False),
        StructField("bandeira", StringType(), nullable=True),
        StructField("produto", StringType(), nullable=False),
        StructField("data_coleta", DateType(), nullable=False),
        StructField("valor_venda", DecimalType(10, 4), nullable=False),
        StructField("valor_compra", DecimalType(10, 4), nullable=True),
        StructField("unidade_medida", StringType(), nullable=True),
        StructField("arquivo_origem", StringType(), nullable=False),
        StructField("data_ingestao_utc", TimestampType(), nullable=False),
        StructField("ano_mes", StringType(), nullable=False),
    ]
)

COLUMN_ALIASES = {
    "uf": ["estado - sigla", "sigla da uf", "uf", "estado"],
    "municipio_anp": ["municipio", "município", "nome do municipio"],
    "bairro": ["bairro"],
    "cep": ["cep"],
    "revenda": ["revenda", "razao social", "razão social"],
    "cnpj_revenda": ["cnpj da revenda", "cnpj"],
    "bandeira": ["bandeira"],
    "produto": ["produto"],
    "data_coleta": ["data da coleta", "data coleta"],
    "valor_venda": ["valor de venda", "preco venda", "preço venda"],
    "valor_compra": ["valor de compra", "preco compra", "preço compra"],
    "unidade_medida": ["unidade de medida", "unidade"],
}


def _norm_col_name(name: str) -> str:
    nfkd = unicodedata.normalize("NFKD", name or "")
    ascii_str = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    return " ".join(ascii_str.lower().strip().split())


def _build_spark_session() -> SparkSession:
    python_exe = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
    return (
        SparkSession.builder.appName("DistribuidoraVeredas_Silver_Precos_Combustiveis")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.python.worker.reuse", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.pyspark.python", python_exe)
        .config("spark.pyspark.driver.python", python_exe)
        .getOrCreate()
    )


def _detect_encoding(csv_path: Path) -> str:
    raw_head = csv_path.read_bytes()[:65536]
    for enc in ("utf-8", "cp1252", "latin1"):
        try:
            raw_head.decode(enc)
            return "UTF-8" if enc == "utf-8" else enc
        except UnicodeDecodeError:
            continue
    return "latin1"


def _read_single_anp_csv(spark: SparkSession, csv_path: Path, ingest_ts: datetime) -> DataFrame:
    enc = _detect_encoding(csv_path)
    df = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("encoding", enc)
        .csv(str(csv_path))
    )

    norm_map = {_norm_col_name(c): f"`{c}`" for c in df.columns}

    def pick_expr(target_key: str) -> F.Column:
        for alias in COLUMN_ALIASES[target_key]:
            norm_alias = _norm_col_name(alias)
            if norm_alias in norm_map:
                return F.col(norm_map[norm_alias])
        return F.lit(None).cast(StringType())

    return (
        df.select(
            F.upper(F.trim(pick_expr("uf"))).alias("uf"),
            F.upper(F.trim(pick_expr("municipio_anp"))).alias("municipio_anp"),
            F.trim(pick_expr("bairro")).alias("bairro"),
            F.regexp_replace(F.trim(pick_expr("cep")), r"[^0-9]", "").alias("cep"),
            F.trim(pick_expr("revenda")).alias("revenda"),
            F.regexp_replace(F.trim(pick_expr("cnpj_revenda")), r"[^0-9]", "").alias("cnpj_revenda"),
            F.upper(F.trim(pick_expr("bandeira"))).alias("bandeira"),
            F.upper(F.trim(pick_expr("produto"))).alias("produto_raw"),
            F.to_date(F.trim(pick_expr("data_coleta")), "dd/MM/yyyy").alias("data_coleta"),
            F.regexp_replace(F.trim(pick_expr("valor_venda")), ",", ".")
            .cast(DecimalType(10, 4))
            .alias("valor_venda"),
            F.regexp_replace(F.trim(pick_expr("valor_compra")), ",", ".")
            .cast(DecimalType(10, 4))
            .alias("valor_compra"),
            F.trim(pick_expr("unidade_medida")).alias("unidade_medida"),
        )
        .withColumn(
            "produto",
            F.when(F.col("produto_raw").contains("GASOLINA"), F.lit("GASOLINA COMUM"))
            .when(F.col("produto_raw").contains("ETANOL"), F.lit("ETANOL"))
            .otherwise(F.col("produto_raw")),
        )
        .filter(F.col("produto").isin(PRODUTOS_ALVO))
        .withColumn("ano_mes", F.date_format(F.col("data_coleta"), "yyyy-MM"))
        .filter(
            F.col("data_coleta").isNotNull()
            & F.col("valor_venda").isNotNull()
            & (F.col("valor_venda") > F.lit(0))
            & F.col("uf").isNotNull()
            & (F.length(F.col("uf")) == 2)
            & F.col("cnpj_revenda").isNotNull()
            & (F.length(F.col("cnpj_revenda")) > 0)
            & (F.col("ano_mes") >= F.lit(PERIODO_INICIO))
            & (F.col("ano_mes") <= F.lit(PERIODO_FIM))
        )
        .withColumn("arquivo_origem", F.lit(csv_path.name).cast(StringType()))
        .withColumn("data_ingestao_utc", F.lit(ingest_ts).cast(TimestampType()))
    )


def _load_ibge_lookup_df() -> pd.DataFrame | None:
    if not INPUT_IBGE_SILVER_DIR.exists():
        return None

    ibge_parts = []
    for uf_dir in sorted(INPUT_IBGE_SILVER_DIR.glob("uf=*")):
        uf_code = uf_dir.name.split("=", 1)[1].strip().upper()
        for pq_file in uf_dir.glob("*.parquet"):
            part_pdf = pd.read_parquet(pq_file)
            part_pdf["sigla_uf"] = uf_code
            ibge_parts.append(part_pdf)

    if not ibge_parts:
        return None

    ibge_pdf = pd.concat(ibge_parts, ignore_index=True)[
        ["sigla_uf", "nome_municipio", "codigo_ibge"]
    ].dropna(subset=["sigla_uf", "nome_municipio", "codigo_ibge"])

    ibge_pdf["_uf_ibge"] = ibge_pdf["sigla_uf"].astype(str).str.strip().str.upper()
    ibge_pdf["_mun_ibge"] = ibge_pdf["nome_municipio"].astype(str).str.strip().str.upper()
    ibge_pdf["codigo_ibge_municipio"] = ibge_pdf["codigo_ibge"].astype("int32")

    return (
        ibge_pdf[["_uf_ibge", "_mun_ibge", "codigo_ibge_municipio"]]
        .drop_duplicates(subset=["_uf_ibge", "_mun_ibge"])
        .reset_index(drop=True)
    )


def _write_months_idempotent(pdf: pd.DataFrame, output_dir: Path) -> tuple[int, int]:
    """
    Grava as particoes `ano_mes=YYYY-MM` de forma idempotente no Windows.
    Remove apenas as pastas de particao presentes no lote antes de gravar.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    if pdf.empty:
        return 0, 0

    total_rows = 0
    partitions_written = 0

    for ym_val, group_df in pdf.groupby("ano_mes", sort=True):
        ym_clean = str(ym_val).strip()
        part_path = output_dir / f"ano_mes={ym_clean}"
        if part_path.exists():
            shutil.rmtree(part_path)

        part_path.mkdir(parents=True, exist_ok=True)
        data_df = group_df.drop(columns=["ano_mes"]).reset_index(drop=True)
        data_df.to_parquet(part_path / "part-00000.parquet", index=False)
        rows_in_part = len(data_df)
        total_rows += rows_in_part
        partitions_written += 1
        print(f"  [PART {ym_clean}] linhas={rows_in_part}")

    return partitions_written, total_rows


def run_job() -> None:
    csv_files = sorted(INPUT_ANP_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"Nenhum CSV encontrado em: {INPUT_ANP_DIR}")

    spark = _build_spark_session()
    try:
        ingest_ts = datetime.now(timezone.utc)
        ibge_lookup = _load_ibge_lookup_df()

        all_pdfs: list[pd.DataFrame] = []

        for csv_path in csv_files:
            part_df = _read_single_anp_csv(spark, csv_path, ingest_ts)
            part_pdf = part_df.toPandas()

            if part_pdf.empty:
                continue

            if ibge_lookup is not None:
                part_pdf["_uf_join"] = part_pdf["uf"].astype(str).str.strip().str.upper()
                part_pdf["_mun_join"] = part_pdf["municipio_anp"].astype(str).str.strip().str.upper()
                part_pdf = part_pdf.merge(
                    ibge_lookup,
                    left_on=["_uf_join", "_mun_join"],
                    right_on=["_uf_ibge", "_mun_ibge"],
                    how="left",
                ).drop(columns=["_uf_join", "_mun_join", "_uf_ibge", "_mun_ibge"])
            else:
                part_pdf["codigo_ibge_municipio"] = pd.NA

            all_pdfs.append(part_pdf)

        if not all_pdfs:
            raise RuntimeError("Nenhum registro valido encontrado apos filtragem dos CSVs da ANP.")

        combined_pdf = pd.concat(all_pdfs, ignore_index=True)

        # Ordena de forma deterministica e deduplica pelo grao (`cnpj_revenda`, `produto`, `data_coleta`)
        combined_pdf = combined_pdf.sort_values(
            by=["valor_venda", "arquivo_origem"],
            ascending=[True, False],
            na_position="last",
        ).drop_duplicates(
            subset=["cnpj_revenda", "produto", "data_coleta"],
            keep="first",
        )

        expected_cols = [field.name for field in SILVER_SCHEMA.fields]
        for col_name in expected_cols:
            if col_name not in combined_pdf.columns:
                raise ValueError(f"Coluna ausente em relacao ao DDL: {col_name}")

        final_pdf = combined_pdf[expected_cols].copy()
        final_pdf["codigo_ibge_municipio"] = pd.to_numeric(
            final_pdf["codigo_ibge_municipio"], errors="coerce"
        ).astype("Int32")
        final_pdf["valor_venda"] = pd.to_numeric(final_pdf["valor_venda"], errors="coerce")
        final_pdf["valor_compra"] = pd.to_numeric(final_pdf["valor_compra"], errors="coerce")
        final_pdf["data_coleta"] = pd.to_datetime(final_pdf["data_coleta"], errors="coerce").dt.date
        final_pdf["data_ingestao_utc"] = pd.to_datetime(final_pdf["data_ingestao_utc"], utc=True)

        partitions_written, total_rows = _write_months_idempotent(final_pdf, OUTPUT_SILVER_DIR)

        print(
            f"[OK] silver_precos_combustiveis gravado em {OUTPUT_SILVER_DIR} | "
            f"particoes={partitions_written} | linhas={total_rows}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    run_job()