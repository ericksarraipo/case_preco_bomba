"""
JOB: transform_silver_municipios_ibge
CAMADA: Prata (Silver)
LE:
  - data/raw/ibge_municipios.json (cadastro oficial de municipios - API IBGE)
GRAVA:
  - data/silver/silver_municipios_ibge (Parquet particionado por coluna `uf`)
GRAO:
  - 1 linha por municipio (`codigo_ibge` unico)
REGRAS APLICADAS:
  1. Extrai campos de municipio, microrregiao, mesorregiao, UF e regiao a partir do JSON do IBGE.
  2. Normaliza tipos conforme o DDL (`codigo_ibge`, `codigo_uf`, `codigo_regiao` como INT; textos e `uf` como STRING).
  3. Filtra registros com `codigo_ibge` e `sigla_uf` validos e deduplica por `codigo_ibge` para garantir idempotencia.
  4. Grava de forma idempotente por particao `uf` (`uf=XX`) sem duplicar em reexecucoes.
"""

from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
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
PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_JSON_PATH = PROJECT_ROOT / "data" / "raw" / "ibge_municipios.json"
OUTPUT_SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "silver_municipios_ibge"

SILVER_SCHEMA = StructType(
    [
        StructField("codigo_ibge", IntegerType(), nullable=False),
        StructField("nome_municipio", StringType(), nullable=False),
        StructField("codigo_uf", IntegerType(), nullable=False),
        StructField("sigla_uf", StringType(), nullable=False),
        StructField("nome_uf", StringType(), nullable=False),
        StructField("codigo_regiao", IntegerType(), nullable=False),
        StructField("sigla_regiao", StringType(), nullable=False),
        StructField("nome_regiao", StringType(), nullable=False),
        StructField("mesorregiao", StringType(), nullable=True),
        StructField("microrregiao", StringType(), nullable=True),
        StructField("data_ingestao_utc", TimestampType(), nullable=False),
        StructField("uf", StringType(), nullable=False),
    ]
)


def _build_spark_session() -> SparkSession:
    python_exe = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
    return (
        SparkSession.builder.appName("DistribuidoraVeredas_Silver_Municipios_IBGE")
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
        data_df = group_df.drop(columns=["uf"]).reset_index(drop=True)
        data_df.to_parquet(part_path / "part-00000.parquet", index=False)


def run_job() -> None:
    if not INPUT_JSON_PATH.exists():
        raise FileNotFoundError(f"Arquivo de entrada nao encontrado: {INPUT_JSON_PATH}")

    spark = _build_spark_session()
    try:
        ingest_ts = datetime.now(timezone.utc)

        raw_df = spark.read.option("multiline", "true").json(str(INPUT_JSON_PATH))
        cols = set(raw_df.columns)

        # Resolve UF e Regiao tanto pela hierarquia classica (microrregiao -> mesorregiao -> UF -> regiao)
        # quanto por fallback em `regiao-imediata.regiao-intermediaria.UF` caso algum registro varie
        if "microrregiao" in cols:
            micro_nome = F.trim(F.col("microrregiao.nome"))
            meso_nome = F.trim(F.col("microrregiao.mesorregiao.nome"))
            uf_id = F.col("microrregiao.mesorregiao.UF.id")
            uf_sigla = F.upper(F.trim(F.col("microrregiao.mesorregiao.UF.sigla")))
            uf_nome = F.trim(F.col("microrregiao.mesorregiao.UF.nome"))
            reg_id = F.col("microrregiao.mesorregiao.UF.regiao.id")
            reg_sigla = F.upper(F.trim(F.col("microrregiao.mesorregiao.UF.regiao.sigla")))
            reg_nome = F.trim(F.col("microrregiao.mesorregiao.UF.regiao.nome"))
        else:
            micro_nome = F.lit(None).cast(StringType())
            meso_nome = F.lit(None).cast(StringType())
            uf_id = F.lit(None).cast(IntegerType())
            uf_sigla = F.lit(None).cast(StringType())
            uf_nome = F.lit(None).cast(StringType())
            reg_id = F.lit(None).cast(IntegerType())
            reg_sigla = F.lit(None).cast(StringType())
            reg_nome = F.lit(None).cast(StringType())

        if "`regiao-imediata`" in [f"`{c}`" for c in cols]:
            im_uf_id = F.col("`regiao-imediata`.`regiao-intermediaria`.UF.id")
            im_uf_sigla = F.upper(F.trim(F.col("`regiao-imediata`.`regiao-intermediaria`.UF.sigla")))
            im_uf_nome = F.trim(F.col("`regiao-imediata`.`regiao-intermediaria`.UF.nome"))
            im_reg_id = F.col("`regiao-imediata`.`regiao-intermediaria`.UF.regiao.id")
            im_reg_sigla = F.upper(
                F.trim(F.col("`regiao-imediata`.`regiao-intermediaria`.UF.regiao.sigla"))
            )
            im_reg_nome = F.trim(F.col("`regiao-imediata`.`regiao-intermediaria`.UF.regiao.nome"))

            uf_id = F.coalesce(uf_id, im_uf_id)
            uf_sigla = F.coalesce(uf_sigla, im_uf_sigla)
            uf_nome = F.coalesce(uf_nome, im_uf_nome)
            reg_id = F.coalesce(reg_id, im_reg_id)
            reg_sigla = F.coalesce(reg_sigla, im_reg_sigla)
            reg_nome = F.coalesce(reg_nome, im_reg_nome)

        extracted_df = (
            raw_df.select(
                F.col("id").cast(IntegerType()).alias("codigo_ibge"),
                F.trim(F.col("nome")).cast(StringType()).alias("nome_municipio"),
                uf_id.cast(IntegerType()).alias("codigo_uf"),
                uf_sigla.cast(StringType()).alias("sigla_uf"),
                uf_nome.cast(StringType()).alias("nome_uf"),
                reg_id.cast(IntegerType()).alias("codigo_regiao"),
                reg_sigla.cast(StringType()).alias("sigla_regiao"),
                reg_nome.cast(StringType()).alias("nome_regiao"),
                meso_nome.cast(StringType()).alias("mesorregiao"),
                micro_nome.cast(StringType()).alias("microrregiao"),
            )
            .filter(
                F.col("codigo_ibge").isNotNull()
                & F.col("nome_municipio").isNotNull()
                & F.col("sigla_uf").isNotNull()
                & (F.length(F.col("sigla_uf")) == 2)
            )
        )

        # Deduplicacao estrita pela chave `codigo_ibge` para idempotencia
        dedup_window = Window.partitionBy("codigo_ibge").orderBy(
            F.col("nome_municipio").asc(), F.col("sigla_uf").asc()
        )
        typed_df = (
            extracted_df.withColumn("_rn", F.row_number().over(dedup_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .withColumn("data_ingestao_utc", F.lit(ingest_ts).cast(TimestampType()))
            .withColumn("uf", F.col("sigla_uf").cast(StringType()))
            .select(
                "codigo_ibge",
                "nome_municipio",
                "codigo_uf",
                "sigla_uf",
                "nome_uf",
                "codigo_regiao",
                "sigla_regiao",
                "nome_regiao",
                "mesorregiao",
                "microrregiao",
                "data_ingestao_utc",
                "uf",
            )
        )

        expected_cols = [field.name for field in SILVER_SCHEMA.fields]
        missing_cols = [c for c in expected_cols if c not in typed_df.columns]
        if missing_cols:
            raise ValueError(f"Colunas ausentes em relacao ao DDL: {missing_cols}")

        row_count = typed_df.count()
        _write_partitioned_parquet_idempotent(typed_df, OUTPUT_SILVER_DIR)

        print(
            f"[OK] silver_municipios_ibge gravado em {OUTPUT_SILVER_DIR} | linhas={row_count}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    run_job()