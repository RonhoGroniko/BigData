import luigi
from pathlib import Path
import subprocess
import sys
import glob
import pandas as pd
import datetime
import os
import shutil


PROJECT_ROOT = Path(r"C:\Users\Rodion\Documents\BigDataPy\lab3")

RAW_PATH = PROJECT_ROOT / "data" / "anime_100mb_sample.csv"

TMP_SPARK_DIR = PROJECT_ROOT / "data" / "tmp" / "spark"
TMP_HADOOP_DIR = PROJECT_ROOT / "data" / "tmp" / "hadoop"

MART_SPARK_PATH = PROJECT_ROOT / "data" / "marts" / "genre_stats_spark.csv"
MART_HADOOP_PATH = PROJECT_ROOT / "data" / "marts" / "genre_stats_hadoop.csv"

PYSPARK_SCRIPT = PROJECT_ROOT / "lab3_3" / "spark_save_file.py"

PYTHON_EXE = sys.executable  

HADOOP_ROOT = PROJECT_ROOT / "lab3_1"
HADOOP_INPUT_CSV = RAW_PATH
HADOOP_OUT_DIR = TMP_HADOOP_DIR 
MAPPER_PATH = HADOOP_ROOT / "mapper.py"
REDUCER_PATH = HADOOP_ROOT / "reducer.py"

HADOOP_HOME = os.environ.get("HADOOP_HOME")
if HADOOP_HOME is None:
    raise RuntimeError("Переменная окружения HADOOP_HOME не установлена")

HADOOP_STREAMING_JAR = (
    Path(HADOOP_HOME)
    / "share" / "hadoop" / "tools" / "lib"
    / "hadoop-streaming-3.2.1.jar"
)
HADOOP_CMD = Path(HADOOP_HOME) / "bin" / "hadoop.cmd"


class RunPysparkJob(luigi.Task):

    run_date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        TMP_SPARK_DIR.mkdir(parents=True, exist_ok=True)
        return luigi.LocalTarget(TMP_SPARK_DIR / "_SUCCESS")

    def run(self):
        if TMP_SPARK_DIR.exists():
            shutil.rmtree(TMP_SPARK_DIR)
        TMP_SPARK_DIR.mkdir(parents=True, exist_ok=True)

        cmd = [
            PYTHON_EXE,
            str(PYSPARK_SCRIPT),
            str(RAW_PATH),
            str(TMP_SPARK_DIR),
        ]

        print("[RunPysparkJob] Выполняю команду:", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print("[RunPysparkJob] ОШИБКА")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            raise RuntimeError("PySpark job failed")

        print("[RunPysparkJob] Успешно.")

        with self.output().open("w") as f:
            f.write("ok\n")


class RunHadoopJob(luigi.Task):

    run_date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        return luigi.LocalTarget(HADOOP_OUT_DIR / "_SUCCESS")

    def run(self):
        print("[RunHadoopJob] Запускаем Hadoop Streaming...")

        if HADOOP_OUT_DIR.exists():
            print(f"[RunHadoopJob] Удаляю старый каталог: {HADOOP_OUT_DIR}")
            shutil.rmtree(HADOOP_OUT_DIR)

        cmd = [
            str(HADOOP_CMD),
            "jar",
            str(HADOOP_STREAMING_JAR),
            "-D", "mapreduce.framework.name=local",
            "-D", "fs.defaultFS=file:///",
            "-D", "mapreduce.job.reduces=1",
            "-input", str(HADOOP_INPUT_CSV),
            "-output", str(HADOOP_OUT_DIR),
            "-mapper", f"python {MAPPER_PATH}",
            "-reducer", f"python {REDUCER_PATH}",
        ]

        print("[RunHadoopJob] Команда:", " ".join(str(c) for c in cmd))
        print("[RunHadoopJob] HADOOP_CMD:", HADOOP_CMD)
        print("[RunHadoopJob] HADOOP_STREAMING_JAR:", HADOOP_STREAMING_JAR)

        result = subprocess.run(
            cmd,
            cwd=str(HADOOP_ROOT),
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            print("[RunHadoopJob] ОШИБКА")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            raise RuntimeError("Hadoop job failed")

        print("[RunHadoopJob] Успешно.")
        print("STDOUT:", result.stdout)

        with self.output().open("w") as f:
            f.write("ok\n")


class BuildSparkGenreMart(luigi.Task):

    run_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return RunPysparkJob(run_date=self.run_date)

    def output(self):
        MART_SPARK_PATH.parent.mkdir(parents=True, exist_ok=True)
        return luigi.LocalTarget(MART_SPARK_PATH)

    def run(self):
        print("[BuildSparkGenreMart] Строим витрину из PySpark...")

        pattern = str(TMP_SPARK_DIR / "part-*.csv")
        files = glob.glob(pattern)

        if not files:
            raise FileNotFoundError(f"Не найдено файлов по шаблону: {pattern}")

        part_file = files[0]
        print("[BuildSparkGenreMart] Использую файл:", part_file)

        df = pd.read_csv(part_file)

        if "genre" not in df.columns or "anime_count" not in df.columns:
            raise ValueError(
                f"Ожидались колонки 'genre' и 'anime_count', а есть: {df.columns.tolist()}"
            )

        df = df.sort_values("anime_count", ascending=False)
        df["calc_date"] = self.run_date

        df.to_csv(self.output().path, index=False, encoding="utf-8")

        print(f"[BuildSparkGenreMart] Витрина сохранена в: {MART_SPARK_PATH}")
        print(df.head())


class BuildHadoopGenreMart(luigi.Task):

    run_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return RunHadoopJob(run_date=self.run_date)

    def output(self):
        MART_HADOOP_PATH.parent.mkdir(parents=True, exist_ok=True)
        return luigi.LocalTarget(MART_HADOOP_PATH)

    def run(self):
        print("[BuildHadoopGenreMart] Строим витрину из Hadoop...")

        pattern = str(TMP_HADOOP_DIR / "part-*")
        files = glob.glob(pattern)

        if not files:
            raise FileNotFoundError(f"Не найдено файлов по шаблону: {pattern}")

        part_file = files[0]
        print("[BuildHadoopGenreMart] Использую файл:", part_file)

        df = pd.read_csv(
            part_file,
            sep="\t",
            header=None,
            names=["genre", "anime_count"],
            encoding="utf-8"
        )

        df = df.sort_values("anime_count", ascending=False)
        df["calc_date"] = self.run_date

        df.to_csv(self.output().path, index=False, encoding="utf-8")

        print(f"[BuildHadoopGenreMart] Витрина сохранена в: {MART_HADOOP_PATH}")
        print(df.head())


class BuildAllMarts(luigi.WrapperTask):
   
    run_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return [
            BuildSparkGenreMart(run_date=self.run_date),
            BuildHadoopGenreMart(run_date=self.run_date),
        ]


if __name__ == "__main__":
    luigi.run()
