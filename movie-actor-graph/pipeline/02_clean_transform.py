r"""02 Spark 清洗转换：IMDb TSV → Parquet（FR-D2）。

清洗规则（设计规约 2.2）：
- title.basics：只留 titleType=='movie'，startYear 为 4 位数字；genres 拆数组
- title.principals：只留 category in ('actor','actress')
- name.basics：primaryName 非空；按 nconst 去重
- 数值列 `\N` → null 后 cast int；runtimeMinutes 先做数字校验
每步打印输入/输出行数与剔除率（FR-D4 统计报告）。
"""
import sys

from pyspark.sql import SparkSession, functions as F

import common


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName(common.SPARK_APP_NAME)
        .master(common.SPARK_MASTER)
        .config("spark.sql.warehouse.dir", common.SPARK_WAREHOUSE)
        .config("spark.driver.memory", "4g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def report(name: str, before: int, after: int) -> None:
    drop = before - after
    rate = drop / before * 100 if before else 0.0
    print(f"[clean] {name}: {before:,} -> {after:,} （剔除 {drop:,}，{rate:.1f}%）")


def read_tsv(spark, path):
    return spark.read.option("sep", "\t").option("header", True).option("compression", "gzip").csv(path)


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # ---- 电影表 ----
    titles = read_tsv(spark, common.RAW_FILES["title.basics.tsv.gz"])
    n0 = titles.count()
    movies = (
        titles.filter(F.col("titleType") == "movie")
        .filter(F.col("startYear").rlike(r"^\d{4}$"))
        .filter(F.col("primaryTitle").isNotNull() & (F.trim(F.col("primaryTitle")) != ""))
        .withColumn("year", F.col("startYear").cast("int"))
        .withColumn(
            "runtimeMinutes",
            F.when(F.col("runtimeMinutes").rlike(r"^\d+$"), F.col("runtimeMinutes").cast("int")).otherwise(None),
        )
        .withColumn("genres", F.split(F.col("genres"), ","))
        .select(
            F.col("tconst").alias("movieId"),
            F.col("primaryTitle").alias("title"),
            "year",
            "runtimeMinutes",
            "genres",
        )
        .dropDuplicates(["movieId"])
    )
    n1 = movies.count()
    report("movies", n0, n1)
    movies.write.mode("overwrite").parquet(f"{common.PROCESSED_DIR}/movies_clean")

    # ---- 出演表 ----
    principals = read_tsv(spark, common.RAW_FILES["title.principals.tsv.gz"])
    n0 = principals.count()
    roles = (
        principals.filter(F.col("category").isin("actor", "actress"))
        .filter(F.col("ordering").rlike(r"^\d+$"))
        .select(
            F.col("tconst").alias("movieId"),
            F.col("nconst").alias("actorId"),
            "category",
            F.col("ordering").cast("int").alias("ordering"),
        )
        .dropDuplicates(["movieId", "actorId"])
    )
    n1 = roles.count()
    report("roles(actor/actress)", n0, n1)
    roles.write.mode("overwrite").parquet(f"{common.PROCESSED_DIR}/roles_clean")

    # ---- 人员表 ----
    names = read_tsv(spark, common.RAW_FILES["name.basics.tsv.gz"])
    n0 = names.count()
    actors = (
        names.filter(F.col("primaryName").isNotNull() & (F.trim(F.col("primaryName")) != ""))
        .withColumn(
            "birthYear",
            F.when(F.col("birthYear").rlike(r"^\d{4}$"), F.col("birthYear").cast("int")).otherwise(None),
        )
        .select(F.col("nconst").alias("actorId"), F.col("primaryName").alias("name"), "birthYear")
        .dropDuplicates(["actorId"])
    )
    n1 = actors.count()
    report("persons", n0, n1)
    actors.write.mode("overwrite").parquet(f"{common.PROCESSED_DIR}/actors_clean")

    print("[ok] 清洗完成，Parquet 输出至 data/processed/")
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
