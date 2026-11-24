"""
JOB: transform_silver_ipca_mensal
CAMADA: Prata (Silver)
LE:
  - data/raw/bcb_ipca_433_202401_202610.json (serie historica mensal IPCA - BCB SGS 433)
GRAVA:
  - data/silver/silver_ipca_mensal (Parquet particionado por coluna `ano`)
GRAO:
  - 1 linha por mes de referencia (`ano_mes` no formato YYYY-MM)
REGRAS APLICADAS:
  1. Filtra o periodo configurado (`PERIODO_INICIO` a `PERIODO_FIM`).
  2. Padroniza `data_referencia` (DATE), `ano_mes` (STRING YYYY-MM), `variacao_mensal_pct` (DECIMAL(10,4)) e `ano` (INT).
  3. Deduplica pela chave de grao `ano_mes` mantendo o registro mais recente para garantir idempotencia.
  4. Calcula `fator_mensal = 1 + (variacao_mensal_pct / 100)` e `indice_acumulado_base` normalizado no `MES_BASE_IPCA`.
  5. Grava de forma idempotente por particao `ano` respeitando os tipos do DDL.
"""

from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

# ==============================================================================
# PARAMETROS DO JOB (CENTRALIZADOS NO TOPO)
# ==============================================================================
PERIODO_INICIO = "2024-01"
PERIODO_FIM = "2026-10"
MES_BASE_IPCA = "2024-01"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_JSON_PATH = PROJECT_ROOT / "data" / "raw" / "bcb_ipca_433_202401_202610.json"
OUTPUT_SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "silver_ipca_mensal"

SILVER_SCHEMA = StructType(
    [
        StructField("ano_mes", StringType(), nullable=False),
        StructField("data_referencia", DateType(), nullable=False),
        StructField("variacao_mensal_pct", DecimalType(10, 4), nullable=False),
        StructField("fator_mensal", DecimalType(18, 10), nullable=False),
        StructField("indice_acumulado_base", DecimalType(18, 10), nullable=False),
        StructField("mes_base_ipca", StringType(), nullable=False),
        StructField("data_ingestao_utc", TimestampType(), nullable=False),
        StructField("ano", IntegerType(), nullable=False),
    ]
)


def _build_spark_session() -> SparkSession:
    python_exe = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
    return (
        SparkSession.builder.appName("DistribuidoraVeredas_Silver_IPCA_Mensal")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.pyspark.python", python_exe)
        .config("spark.pyspark.driver.python", python_exe)
        .getOrCreate()
    )


def _write_partitioned_parquet_idempotent(df_spark, output_dir: Path) -> None:
    """
    Grava o DataFrame particionado por `ano` de forma idempotente no Windows.
    Remove apenas as pastas de particao (`ano=YYYY`) presentes no lote antes de gravar,
    evitando duplicacao em reexecucoes sem depender de winutils.exe.
    """
    pdf = df_spark.toPandas()
    output_dir.mkdir(parents=True, exist_ok=True)

    if pdf.empty:
        return

    partitions_in_batch = sorted(pdf["ano"].dropna().unique().tolist())
    for ano_val in partitions_in_batch:
        part_path = output_dir / f"ano={int(ano_val)}"
        if part_path.exists():
            shutil.rmtree(part_path)

    for ano_val, group_df in pdf.groupby("ano", sort=True):
        part_path = output_dir / f"ano={int(ano_val)}"
        part_path.mkdir(parents=True, exist_ok=True)
        data_df = group_df.drop(columns=["ano"]).reset_index(drop=True)
        data_df.to_parquet(part_path / "part-00000.parquet", index=False)


def run_job() -> None:
    if not INPUT_JSON_PATH.exists():
        raise FileNotFoundError(f"Arquivo de entrada nao encontrado: {INPUT_JSON_PATH}")

    spark = _build_spark_session()
    try:
        ingest_ts = datetime.now(timezone.utc)

        raw_df = spark.read.option("multiline", "true").json(str(INPUT_JSON_PATH))

        parsed_df = (
            raw_df.withColumn("data_referencia", F.to_date(F.col("data"), "dd/MM/yyyy"))
            .withColumn("ano_mes", F.date_format(F.col("data_referencia"), "yyyy-MM"))
            .withColumn("ano", F.year(F.col("data_referencia")).cast(IntegerType()))
            .withColumn(
                "variacao_mensal_pct",
                F.regexp_replace(F.trim(F.col("valor")), ",", ".").cast(DecimalType(10, 4)),
            )
            .filter(
                F.col("data_referencia").isNotNull()
                & F.col("variacao_mensal_pct").isNotNull()
                & (F.col("ano_mes") >= F.lit(PERIODO_INICIO))
                & (F.col("ano_mes") <= F.lit(PERIODO_FIM))
            )
        )

        dedup_window = Window.partitionBy("ano_mes").orderBy(F.col("data_referencia").desc())
        dedup_df = (
            parsed_df.withColumn("_rn", F.row_number().over(dedup_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        ordered_window = Window.orderBy("ano_mes").rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )

        with_factor_df = dedup_df.withColumn(
            "fator_dbl",
            F.lit(1.0) + (F.col("variacao_mensal_pct").cast(DoubleType()) / F.lit(100.0)),
        ).withColumn(
            "indice_bruto_dbl",
            F.exp(F.sum(F.log(F.col("fator_dbl"))).over(ordered_window)),
        )

        base_row = (
            with_factor_df.filter(F.col("ano_mes") == F.lit(MES_BASE_IPCA))
            .select("indice_bruto_dbl")
            .collect()
        )
        if not base_row:
            raise ValueError(
                f"MES_BASE_IPCA '{MES_BASE_IPCA}' nao encontrado no periodo filtrado "
                f"({PERIODO_INICIO} a {PERIODO_FIM})."
            )
        base_indice = float(base_row[0]["indice_bruto_dbl"])

        typed_df = (
            with_factor_df.withColumn(
                "indice_rebased_dbl",
                F.col("indice_bruto_dbl") / F.lit(base_indice),
            )
            .withColumn(
                "fator_mensal",
                F.round(F.col("fator_dbl"), 10).cast(DecimalType(18, 10)),
            )
            .withColumn(
                "indice_acumulado_base",
                F.round(F.col("indice_rebased_dbl"), 10).cast(DecimalType(18, 10)),
            )
            .withColumn("mes_base_ipca", F.lit(MES_BASE_IPCA).cast(StringType()))
            .withColumn("data_ingestao_utc", F.lit(ingest_ts).cast(TimestampType()))
            .select(
                F.col("ano_mes").cast(StringType()),
                F.col("data_referencia").cast(DateType()),
                F.col("variacao_mensal_pct").cast(DecimalType(10, 4)),
                F.col("fator_mensal"),
                F.col("indice_acumulado_base"),
                F.col("mes_base_ipca"),
                F.col("data_ingestao_utc"),
                F.col("ano").cast(IntegerType()),
            )
        )

        # Garante que todas as colunas do DDL existem no DataFrame final
        expected_cols = [field.name for field in SILVER_SCHEMA.fields]
        missing_cols = [c for c in expected_cols if c not in typed_df.columns]
        if missing_cols:
            raise ValueError(f"Colunas ausentes em relacao ao DDL: {missing_cols}")

        row_count = typed_df.count()
        _write_partitioned_parquet_idempotent(typed_df, OUTPUT_SILVER_DIR)

        print(
            f"[OK] silver_ipca_mensal gravado em {OUTPUT_SILVER_DIR} | "
            f"linhas={row_count} | periodo={PERIODO_INICIO}..{PERIODO_FIM} | base={MES_BASE_IPCA}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    run_job()
    