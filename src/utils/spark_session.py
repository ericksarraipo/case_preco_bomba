import os
from pathlib import Path
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "distribuidora_veredas") -> SparkSession:
    project_root = Path(__file__).resolve().parents[2]
    warehouse_dir = project_root / "spark-warehouse"
    hadoop_home = os.environ.get("HADOOP_HOME", "C:\\hadoop")

    if Path(hadoop_home).exists():
        os.environ["HADOOP_HOME"] = hadoop_home

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.hadoop.home.dir", hadoop_home)
        .config("spark.ui.enabled", "false")
    )
    return builder.getOrCreate()