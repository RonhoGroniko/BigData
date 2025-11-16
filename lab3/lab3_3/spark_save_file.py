from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, split, explode, trim,
    countDistinct, desc
)
import sys
from pathlib import Path


def main(input_path: str, output_dir: str):
    spark = (
        SparkSession.builder
        .appName("AnimeGenres")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", "true")
        .option("escape", '"')
        .option("quote", '"')
        .csv(input_path)
        .select("anime_id", "Genres")
    )

    df_genres = (
        df
        .where(col("Genres").isNotNull())
        .withColumn("Genres", regexp_replace("Genres", ";", ","))  
        .withColumn("genre", explode(split(col("Genres"), ",")))
        .withColumn("genre", trim(col("genre")))
        .where(col("genre") != "")
    )

    result = (
        df_genres
        .groupBy("genre")
        .agg(countDistinct("anime_id").alias("anime_count"))
        .orderBy(desc("anime_count"))
    )

    out_dir = Path(output_dir)
    out_dir.parent.mkdir(parents=True, exist_ok=True)

    (
        result
        .coalesce(1)              
        .write
        .option("header", "true")
        .mode("overwrite")
        .csv(str(out_dir))
    )

    result.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: anime_genres_pyspark.py <input_csv> <output_dir>")
        sys.exit(1)

    input_csv = sys.argv[1]
    output_dir = sys.argv[2]
    main(input_csv, output_dir)
