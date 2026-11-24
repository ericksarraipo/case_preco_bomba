"""
JOB: transform_silver_reembolso_uf_vigencia
CAMADA: Prata (Silver)
LE:
  - data/raw/tabela_reembolso_uf.csv (tabela de tetos de reembolso por UF e vigencia)
GRAVA:
  - data/silver/silver_reembolso_uf_vigencia (Parquet particionado por coluna `uf`)
GRAO:
  - 1 linha por versao/registro de vigencia de teto por UF (`uf`, `versao`, `vigencia_inicio`)
REGRAS APLICADAS:
  1. Le o CSV separado por `;` com datas `dd/MM/yyyy` e decimais com virgula (`teto_litro_reembolso`).
  2. Preserva `vigencia_fim_original_nula` e preenche `vigencia_fim` nula com `LIMITE_VIGENCIA_PADRAO` (`2026-10-31`) para analise de intervalos no recorte do case, sem imputar valores de teto.
  3. Ordena por `uf`, `vigencia_inicio`, `versao` e identifica `LACUNA_ANTERIOR` e `SOBREPOSICAO_ANTERIOR` (como nos casos de BA e PR), registrando `flag_anomalia_vigencia` e `detalhe_anomalia` sem estimativas manuais.
  4. Deduplica pela chave do grao (`uf`, `versao`, `vigencia_inicio`) e grava de forma idempotente por particao `uf`.
"""

from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
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
LIMITE_VIGENCIA_PADRAO = "2026-10-31"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_CSV_PATH = PROJECT_ROOT / "data" / "raw" / "tabela_reembolso_uf.csv"
OUTPUT_SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "silver_reembolso_uf_vigencia"

SILVER_SCHEMA = StructType(
    [
        StructField("uf", StringType(), nullable=False),
        StructField("versao", IntegerType(), nullable=False),
        StructField("teto_litro_reembolso", DecimalType(10, 4), nullable=False),
        StructField("vigencia_inicio", DateType(), nullable=False),
        StructField("vigencia_fim", DateType(), nullable=False),
        StructField("vigencia_fim_original_nula", BooleanType(), nullable=False),
        StructField("flag_anomalia_vigencia", StringType(), nullable=False),
        StructField("detalhe_anomalia", StringType(), nullable=True),
        StructField("data_ingestao_utc", TimestampType(), nullable=False),
    ]
)


def _build_spark_session() -> SparkSession:
    python_exe = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
    return (
        SparkSession.builder.appName("DistribuidoraVeredas_Silver_Reembolso_UF_Vigencia")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.pyspark.python", python_exe)
        .config("spark.pyspark.driver.python", python_exe)
        .getOrCreate()
    )


def _write_partitioned_parquet_idempotent(df_spark, output_dir: Path) -> None:
    """
    Grava o DataFrame particionado por `uf` de forma idempotente no Windows.
    Remove apenas as pastas de particao (`uf=XX`) presentes no lote antes de gravar.
    """
    pdf = df_spark.toPandas()
    output_dir.mkdir(parents=True, exist_ok=True)

    if pdf.empty:
        return

    partitions_in_batch = sorted(pdf["uf"].dropna().unique().tolist())
    for uf_val in partitions_in_batch:
        part_path = output_dir / f"uf={str(uf_val).strip()}"
        if part_path.exists():
            shutil.rmtree(part_path)

    for uf_val, group_df in pdf.groupby("uf", sort=True):
        uf_clean = str(uf_val).strip()
        part_path = output_dir / f"uf={uf_clean}"
        part_path.mkdir(parents=True, exist_ok=True)
        # Mantem `uf` dentro do arquivo parquet para facilitar leitura direta por particao/schema,
        # ou remova se preferir hive-only; aqui mantemos todas as colunas do DDL.
        data_df = group_df.drop(columns=["uf"]).reset_index(drop=True)
        data_df.to_parquet(part_path / "part-00000.parquet", index=False)


def run_job() -> None:
    if not INPUT_CSV_PATH.exists():
        raise FileNotFoundError(f"Arquivo de entrada nao encontrado: {INPUT_CSV_PATH}")

    spark = _build_spark_session()
    try:
        ingest_ts = datetime.now(timezone.utc)

        raw_df = (
            spark.read.option("header", "true")
            .option("sep", ";")
            .option("encoding", "UTF-8")
            .csv(str(INPUT_CSV_PATH))
        )

        limite_date = F.to_date(F.lit(LIMITE_VIGENCIA_PADRAO), "yyyy-MM-dd")

        parsed_df = (
            raw_df.withColumn("uf_limpa", F.upper(F.trim(F.col("uf"))))
            .withColumn("versao_int", F.trim(F.col("versao")).cast(IntegerType()))
            .withColumn(
                "teto_dec",
                F.regexp_replace(F.trim(F.col("teto_litro_reembolso")), ",", ".").cast(
                    DecimalType(10, 4)
                ),
            )
            .withColumn("vig_ini", F.to_date(F.trim(F.col("vigencia_inicio")), "dd/MM/yyyy"))
            .withColumn("vig_fim_raw", F.to_date(F.trim(F.col("vigencia_fim")), "dd/MM/yyyy"))
            .withColumn("vig_fim_nula", F.col("vig_fim_raw").isNull())
            .withColumn("vig_fim", F.coalesce(F.col("vig_fim_raw"), limite_date))
            .withColumn("obs_limpa", F.trim(F.col("observacao")))
            .filter(
                F.col("uf_limpa").isNotNull()
                & (F.length(F.col("uf_limpa")) == 2)
                & F.col("versao_int").isNotNull()
                & F.col("teto_dec").isNotNull()
                & F.col("vig_ini").isNotNull()
                & F.col("vig_fim").isNotNull()
            )
        )

        # Deduplicacao pela chave do grao (`uf_limpa`, `versao_int`, `vig_ini`)
        dedup_window = Window.partitionBy("uf_limpa", "versao_int", "vig_ini").orderBy(
            F.col("vig_fim").desc(), F.col("obs_limpa").desc_nulls_last()
        )
        dedup_df = (
            parsed_df.withColumn("_rn", F.row_number().over(dedup_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        # Janela temporal por UF para deteccao de lacunas e sobreposicoes
        seq_window = Window.partitionBy("uf_limpa").orderBy(
            F.col("vig_ini").asc(), F.col("versao_int").asc()
        )

        with_prev_df = (
            dedup_df.withColumn("prev_vig_fim", F.lag("vig_fim").over(seq_window))
            .withColumn("prev_versao", F.lag("versao_int").over(seq_window))
            .withColumn(
                "is_lacuna",
                F.col("prev_vig_fim").isNotNull()
                & (F.col("vig_ini") > F.date_add(F.col("prev_vig_fim"), 1)),
            )
            .withColumn(
                "is_sobreposicao",
                F.col("prev_vig_fim").isNotNull()
                & (F.col("vig_ini") <= F.col("prev_vig_fim")),
            )
        )

        flagged_df = (
            with_prev_df.withColumn(
                "flag_anomalia_vigencia",
                F.when(F.col("is_lacuna"), F.lit("LACUNA_ANTERIOR"))
                .when(F.col("is_sobreposicao"), F.lit("SOBREPOSICAO_ANTERIOR"))
                .otherwise(F.lit("SEM_ANOMALIA")),
            )
            .withColumn(
                "detalhe_anomalia",
                F.when(
                    F.col("is_lacuna"),
                    F.concat(
                        F.lit("Lacuna entre "),
                        F.date_format(F.date_add(F.col("prev_vig_fim"), 1), "yyyy-MM-dd"),
                        F.lit(" e "),
                        F.date_format(F.date_sub(F.col("vig_ini"), 1), "yyyy-MM-dd"),
                        F.lit(" (apos versao "),
                        F.col("prev_versao").cast(StringType()),
                        F.lit(")"),
                        F.when(
                            F.col("obs_limpa").isNotNull() & (F.length(F.col("obs_limpa")) > 0),
                            F.concat(F.lit(" | obs: "), F.col("obs_limpa")),
                        ).otherwise(F.lit("")),
                    ),
                )
                .when(
                    F.col("is_sobreposicao"),
                    F.concat(
                        F.lit("Sobreposicao a partir de "),
                        F.date_format(F.col("vig_ini"), "yyyy-MM-dd"),
                        F.lit(" com vigencia anterior ate "),
                        F.date_format(F.col("prev_vig_fim"), "yyyy-MM-dd"),
                        F.lit(" (versao anterior: "),
                        F.col("prev_versao").cast(StringType()),
                        F.lit(")"),
                        F.when(
                            F.col("obs_limpa").isNotNull() & (F.length(F.col("obs_limpa")) > 0),
                            F.concat(F.lit(" | obs: "), F.col("obs_limpa")),
                        ).otherwise(F.lit("")),
                    ),
                )
                .otherwise(
                    F.when(
                        F.col("obs_limpa").isNotNull() & (F.length(F.col("obs_limpa")) > 0),
                        F.concat(F.lit("obs: "), F.col("obs_limpa")),
                    ).otherwise(F.lit(None).cast(StringType()))
                ),
            )
        )

        typed_df = (
            flagged_df.withColumn("data_ingestao_utc", F.lit(ingest_ts).cast(TimestampType()))
            .select(
                F.col("uf_limpa").cast(StringType()).alias("uf"),
                F.col("versao_int").cast(IntegerType()).alias("versao"),
                F.col("teto_dec").cast(DecimalType(10, 4)).alias("teto_litro_reembolso"),
                F.col("vig_ini").cast(DateType()).alias("vigencia_inicio"),
                F.col("vig_fim").cast(DateType()).alias("vigencia_fim"),
                F.col("vig_fim_nula").cast(BooleanType()).alias("vigencia_fim_original_nula"),
                F.col("flag_anomalia_vigencia").cast(StringType()),
                F.col("detalhe_anomalia").cast(StringType()),
                F.col("data_ingestao_utc"),
            )
        )

        expected_cols = [field.name for field in SILVER_SCHEMA.fields]
        missing_cols = [c for c in expected_cols if c not in typed_df.columns]
        if missing_cols:
            raise ValueError(f"Colunas ausentes em relacao ao DDL: {missing_cols}")

        row_count = typed_df.count()
        _write_partitioned_parquet_idempotent(typed_df, OUTPUT_SILVER_DIR)

        anom_count = typed_df.filter(F.col("flag_anomalia_vigencia") != "SEM_ANOMALIA").count()
        print(
            f"[OK] silver_reembolso_uf_vigencia gravado em {OUTPUT_SILVER_DIR} | "
            f"linhas={row_count} | anomalias_vigencia={anom_count}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    run_job()