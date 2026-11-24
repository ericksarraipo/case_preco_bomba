from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from pyspark.sql import functions as F
from src.utils.spark_session import get_spark_session


def run_bronze_ingestion(
    source_filename: str = "tabela_reembolso_uf.csv",
    app_name: str = "veredas_bronze_reembolso_uf",
) -> None:
    project_root = Path(__file__).resolve().parents[2]
    raw_path = project_root / "data" / "raw" / source_filename
    bronze_dir = project_root / "data" / "bronze" / "tabela_reembolso_uf"
    bronze_file = bronze_dir / "part-00000.parquet"

    if not raw_path.exists():
        raise FileNotFoundError(f"Arquivo bruto não encontrado em: {raw_path}")

    spark = get_spark_session(app_name=app_name)

    try:
        df_raw = (
            spark.read.option("header", True)
            .option("sep", ";")
            .option("encoding", "UTF-8")
            .csv(str(raw_path))
        )

        df_bronze = df_raw.withColumn(
            "_ingested_at", F.current_timestamp()
        ).withColumn("_source_file", F.lit(source_filename))

        bronze_dir.mkdir(parents=True, exist_ok=True)
        pdf_bronze = df_bronze.toPandas()
        pdf_bronze["_ingested_at"] = datetime.now(timezone.utc)
        pdf_bronze.to_parquet(bronze_file, index=False)

        df_check = spark.read.parquet(str(bronze_dir))
        print(f"Bronze gravada com sucesso em: {bronze_file}")
        print(f"Total de registros: {df_check.count()}")
        df_check.printSchema()
    finally:
        spark.stop()


if __name__ == "__main__":
    run_bronze_ingestion()