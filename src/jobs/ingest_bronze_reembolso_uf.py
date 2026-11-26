from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq


def run_bronze_ingestion(
    source_filename: str = "tabela_reembolso_uf.csv",
    app_name: str = "veredas_bronze_reembolso_uf",
) -> None:
    _ = app_name
    project_root = Path(__file__).resolve().parents[2]
    raw_path = project_root / "data" / "raw" / source_filename
    bronze_dir = project_root / "data" / "bronze" / "tabela_reembolso_uf"
    bronze_file = bronze_dir / "part-00000.parquet"

    if not raw_path.exists():
        raise FileNotFoundError(f"Arquivo bruto não encontrado em: {raw_path}")

    df_raw = pd.read_csv(raw_path, sep=";", encoding="utf-8", dtype=str)

    pdf_bronze = df_raw.copy()
    pdf_bronze["_ingested_at"] = datetime.now(timezone.utc)
    pdf_bronze["_source_file"] = source_filename

    bronze_dir.mkdir(parents=True, exist_ok=True)
    pdf_bronze.to_parquet(bronze_file, index=False)

    parquet_meta = pq.read_table(bronze_file)
    print(f"Bronze gravada com sucesso em: {bronze_file}")
    print(f"Total de registros: {parquet_meta.num_rows}")
    print(parquet_meta.schema)


if __name__ == "__main__":
    run_bronze_ingestion()
